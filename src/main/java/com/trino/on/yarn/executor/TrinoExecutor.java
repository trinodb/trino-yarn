/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trino.on.yarn.executor;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.server.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.List;

import static com.trino.on.yarn.constant.Constants.*;

public class TrinoExecutor {
    protected static final Log LOG = LogFactory.getLog(TrinoExecutor.class);

    private JobInfo jobInfo;
    private SimpleServer server;
    private int amMemory;
    private String clientLogApi;
    private Process process;
    private boolean endStart = false;
    private String ip = Server.ip();
    private int trinoPort = NetUtil.getUsableLocalPort();
    private static final List<String> trinoEnv = CollUtil.newArrayList();
    private static final List<String> trinoEnvExport = CollUtil.newArrayList();

    public TrinoExecutor(JobInfo jobInfo, SimpleServer server, int amMemory) {
        this.jobInfo = jobInfo;
        this.server = server;
        this.amMemory = amMemory;
        clientLogApi = Server.formatUrl(Server.CLIENT_LOG, jobInfo.getIp(), jobInfo.getPort());
        CronUtil.schedule("*/5 * * * * *", (Task) () -> {
            try {
                HttpUtil.post(clientLogApi, "the heartbeat detection......", 3000);
            } catch (Exception e) {
                Server.setMasterFinish(2);
                process.destroy();
                throw new RuntimeException("client is stop", e);
            }
        });
        CronUtil.setMatchSecond(true);
        CronUtil.start();
    }

    public Process run() {
        process = start();
        return process;
    }

    /**
     * trino 启动逻辑
     *
     * @return
     */
    public Process start() {
        String conf = createConf();
        Process exec;
        String cmds = "ls -la ./ && ls -la ./conf";

        if (jobInfo.isTest()) {
            exec = RuntimeUtil.exec(cmds);
        } else {
            cmds = StrUtil.join(StrPool.LF, trinoEnvExport) + StrPool.LF + StrUtil.join(" ", trinoEnv);
            LOG.warn(cmds);
            String path = conf + "/" + StrUtil.uuid() + ".sh";
            File file = FileUtil.writeUtf8String(cmds, path);
            exec = RuntimeUtil.exec("sh " + file.getAbsolutePath());
        }

        ThreadUtil.execAsync(() -> {
            try {
                log(exec);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        return exec;
    }

    private void log(Process exec) throws InterruptedException {
        IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) line -> {
            if (StrUtil.contains(line, "======== SERVER STARTED ========") ||
                    StrUtil.contains(line, "==========")) {
                if (!endStart) {
                    endStart = true;
                    end();
                }
            }
            LOG.info(line);
            if (jobInfo.isDebug()) {
                HttpUtil.post(clientLogApi, line, 10000);
            }
        });
        int exitCode = exec.waitFor();
        assert (exitCode == 0);
    }

    /**
     * 构造启动脚本/环境变量
     *
     * @return
     */
    private String createConf() {
        String path = new File(".").getAbsolutePath();
        LOG.warn("trino conf path:" + path);
        LOG.warn("trino lib path:" + jobInfo.getPath());
        final String conf = path + "/conf/";
        final String data = path + "/data/";
        final String logPath = path + "/server.log";

        FileUtil.mkdir(conf);
        FileUtil.mkdir(data);

        String log = StrUtil.format(TRINO_LOG_CONTENT, "WARN");
        File file = FileUtil.writeUtf8String(log, conf + TRINO_LOG);
        int nodeMemory = amMemory / 3 * 2;
        String config = StrUtil.format(TRINO_CONFIG_CONTENT, ip, trinoPort, amMemory, nodeMemory, nodeMemory, trinoPort, path);
        File configEnv = FileUtil.writeUtf8String(config, conf + TRINO_CONFIG);

        //写入运行参数
        put(jobInfo.getJdk11Home() + "/bin/java");
        put("-cp");
        if (!StrUtil.endWith(jobInfo.getLibPath(), "*")) {
            put(".:" + jobInfo.getLibPath() + "/*");
        } else {
            put(".:" + jobInfo.getLibPath());
        }
        String jvms = StrUtil.format(TRINO_JVM_CONTENT, amMemory);
        for (String jvm : StrUtil.split(jvms, StrPool.LF)) {
            put(jvm);
        }
        putEnv(LOG_LEVELS_FILE, file.getAbsolutePath());
        putEnv(CONFIG, configEnv.getAbsolutePath());
        putEnv(LOG_OUTPUT_FILE, logPath);
        putEnv("log.enable-console=true");
        String nodes = StrUtil.format(TRINO_NODE_CONTENT, StrUtil.uuid(), path, jobInfo.getCatalog(), jobInfo.getPluginPath());
        for (String node : StrUtil.split(nodes, StrPool.LF)) {
            putEnv(node);
        }
        put("io.trino.server.TrinoServer");

        //写入环境变量
        String envs = StrUtil.format(TRINO_ENV_CONTENT, jobInfo.getJdk11Home(), ip, trinoPort);
        for (String env : StrUtil.split(envs, StrPool.LF)) {
            putEnvExport(env);
        }
        String arch = System.getProperty("os.arch", "x86_64");
        if (arch.equalsIgnoreCase("amd64")) {
            arch = "x86_64";
        }
        String osInfo = System.getProperty("os.name", "Linux") + "-" + arch;
        String procname = jobInfo.getProcname(osInfo);

        if (FileUtil.exist(procname)) {
            putEnvExport("LD_PRELOAD=:" + procname);
        }
        putEnvExport("PROCNAME=" + "trino-server");
        return path;
    }

    public void end() {
        String clientRun = Server.formatUrl(Server.CLIENT_RUN, jobInfo.getIp(), jobInfo.getPort());
        String body = JSONUtil.createObj()
                .putOpt("ip", Server.ip())
                .putOpt("port", server.getAddress().getPort())
                .putOpt("trinoPort", trinoPort)
                .putOpt("user", jobInfo.getUser())
                .putOpt("sql", jobInfo.getSql())
                .putOpt("start", true).toString();
        HttpUtil.post(clientRun, body, 10000);
    }

    public void putEnv(String k, String v) {
        trinoEnv.add("-D" + k + "=" + v);
    }

    public void putEnv(String kv) {
        trinoEnv.add("-D" + kv);
    }

    public void put(String kv) {
        trinoEnv.add(kv);
    }

    public void putEnvExport(String kv) {
        trinoEnvExport.add("export " + kv);
    }
}
