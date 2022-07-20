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
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
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
    private String ip = Server.ip();
    private int trinoPort = NetUtil.getUsableLocalPort();
    private static final List<String> trinoEnv = CollUtil.newArrayList();
    private static final List<String> trinoEnvExport = CollUtil.newArrayList();

    public TrinoExecutor(JobInfo jobInfo, SimpleServer server, int amMemory) {
        this.jobInfo = jobInfo;
        this.server = server;
        this.amMemory = amMemory;
    }

    public Process run() throws InterruptedException {
        Process exec = start();
        end();
        return exec;
    }

    /**
     * trino 启动逻辑
     *
     * @return
     */
    public Process start() throws InterruptedException {
        createConf();
        //Process exec = RuntimeUtil.exec(ArrayUtil.toArray(trinoEnvExport, String.class), ArrayUtil.toArray(trinoEnv, String.class));
        Process exec = RuntimeUtil.exec(ArrayUtil.toArray(trinoEnvExport, String.class), "ls -la ./ && ls -la ./conf");
        LOG.warn(JSONUtil.toJsonStr(trinoEnv));
        LOG.warn(JSONUtil.toJsonStr(trinoEnvExport));
        IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) LOG::info);
        int exitCode = exec.waitFor();
        assert (exitCode == 0);
        return exec;
    }

    /**
     * 构造启动脚本/环境变量
     *
     * @return
     */
    private String createConf() {
        String path = new File("./").getAbsolutePath();
        LOG.warn("trino conf path:" + path);
        LOG.warn("trino lib path:" + jobInfo.getPath());
        final String conf = path + "/conf/";
        final String data = path + "/data/";
        final String logPath = path + "/server.log";

        FileUtil.mkdir(conf);
        FileUtil.mkdir(data);

        String log = StrUtil.format(TRINO_LOG_CONTENT, "WARN");
        File file = FileUtil.writeUtf8String(log, conf + TRINO_LOG);
        String config = StrUtil.format(TRINO_CONFIG_CONTENT, ip, trinoPort, amMemory, amMemory, amMemory, trinoPort, path);
        File configEnv = FileUtil.writeUtf8String(config, conf + TRINO_CONFIG);

        //写入运行参数
        put(jobInfo.getJdk11Home() + "/bin/java");
        put("-cp");
        if (!StrUtil.endWith(jobInfo.getLibPath(), "*")) {
            put(jobInfo.getLibPath() + "/*");
        }
        put(jobInfo.getLibPath());
        String jvms = StrUtil.format(TRINO_JVM_CONTENT, amMemory);
        for (String jvm : StrUtil.split(jvms, StrPool.LF)) {
            put(jvm);
        }
        putEnv(LOG_LEVELS_FILE, file.getAbsolutePath());
        putEnv(CONFIG, configEnv.getAbsolutePath());
        putEnv(LOG_OUTPUT_FILE, logPath);
        putEnv("log.enable-console=false");
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
        String osInfo = System.getProperty("os.name", "Linux") + "-" + System.getProperty("os.arch", "x86_64");
        putEnvExport(jobInfo.getProcname(osInfo));
        return path;
    }

    public void end() {
        String clientRun = Server.formatUrl(Server.CLIENT_RUN, jobInfo.getIp(), jobInfo.getPort());
        String body = JSONUtil.createObj()
                .putOpt("ip", Server.ip())
                .putOpt("port", server.getAddress().getPort())
                .putOpt("trinoPort", trinoPort)
                .putOpt("email", jobInfo.getEmail())
                .putOpt("sql", jobInfo.getSql())
                .putOpt("start", true).toString();
        HttpUtil.post(clientRun, body);
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
        trinoEnvExport.add(kv);
    }
}
