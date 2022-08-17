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
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.trino.on.yarn.constant.RunType;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.server.Server;
import com.trino.on.yarn.util.PrestoSQLHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.trino.on.yarn.constant.Constants.*;

@Slf4j
public abstract class TrinoExecutor {
    protected static final Log LOG = LogFactory.getLog(TrinoExecutor.class);

    protected static final List<String> trinoEnv = CollUtil.newArrayList();
    protected static final List<String> trinoEnvExport = CollUtil.newArrayList();
    protected JobInfo jobInfo;
    protected int amMemory;
    protected String clientLogApi;
    protected boolean endStart = false;
    protected String path;
    protected boolean nodeSchedulerIncludeCoordinator = false;
    private String catalogNew;

    public TrinoExecutor(JobInfo jobInfo, int amMemory) {
        this.jobInfo = jobInfo;
        this.amMemory = amMemory;
        clientLogApi = Server.formatUrl(Server.CLIENT_LOG, jobInfo.getIp(), jobInfo.getPort());
        path = new File(".").getAbsolutePath();
    }

    protected abstract void log(Process exec) throws InterruptedException;

    protected abstract String trinoConfig();

    public Process run() {
        return start();
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

        try {
            log(exec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return exec;
    }

    /**
     * 构造启动脚本/环境变量
     *
     * @return
     */
    protected String createConf() {
        LOG.warn("trino conf path:" + path);
        LOG.warn("trino lib path:" + jobInfo.getPath());
        final String conf = path + "/conf/";
        final String data = path + "/data/";
        final String logPath = path + "/server.log";
        catalogNew = path + "/" + JAVA_TRINO_CATALOG_PATH + "new/";

        FileUtil.mkdir(conf);
        FileUtil.mkdir(data);
        FileUtil.mkdir(catalogNew);

        String log = StrUtil.format(TRINO_LOG_CONTENT, "WARN");
        File logFile = FileUtil.writeUtf8String(log, conf + TRINO_LOG);
        String config = trinoConfig();
        File configEnv = FileUtil.writeUtf8String(config, conf + TRINO_CONFIG);

        //获取JAVA11_HOME
        String java11Home = System.getenv("JAVA11_HOME");
        if (StrUtil.isBlank(java11Home)) {
            java11Home = jobInfo.getJdk11Home();
        }
        //写入运行参数
        put(java11Home + "/bin/java");
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
        putEnv(LOG_LEVELS_FILE, logFile.getAbsolutePath());
        putEnv(CONFIG, configEnv.getAbsolutePath());
        putEnv(LOG_OUTPUT_FILE, logPath);
        putEnv("log.enable-console=true");
        String catalog = jobInfo.getCatalog();
        //压缩包可能存在多一层嵌套问题
        if (jobInfo.isHdfsOrS3()) {
            catalog = path + "/" + JAVA_TRINO_CATALOG_PATH + "/" + JAVA_TRINO_CATALOG_PATH;
            if (!FileUtil.exist(catalog)) {
                catalog = path + "/" + JAVA_TRINO_CATALOG_PATH;
            }
            if (!FileUtil.exist(catalog)) {
                throw new RuntimeException("catalog not found");
            }
        }
        if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType())) {
            try {
                catalog = catalogFilter(catalog);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String nodes = StrUtil.format(TRINO_NODE_CONTENT, StrUtil.uuid(), path, catalog, jobInfo.getPluginPath());
        for (String node : StrUtil.split(nodes, StrPool.LF)) {
            putEnv(node);
        }
        put("io.trino.server.TrinoServer");

        //写入环境变量
        String envs = StrUtil.format(TRINO_ENV_CONTENT, jobInfo.getJdk11Home(), jobInfo.getIpMaster(), jobInfo.getPortTrino());
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

    private static final String clientLogMeta = "Trino Master IP:{} Port:{}";

    protected void end() {
        if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType())) {
            String clientRun = Server.formatUrl(Server.CLIENT_RUN, jobInfo.getIp(), jobInfo.getPort());
            jobInfo.setStart(true);
            HttpUtil.post(clientRun, jobInfo.toString(), 10000);
        }
        HttpUtil.post(clientLogApi, StrUtil.format(clientLogMeta, jobInfo.getIpMaster(), jobInfo.getPortTrino()), 10000);
    }

    protected void putEnv(String k, String v) {
        trinoEnv.add("-D" + k + "=" + v);
    }

    protected void putEnv(String kv) {
        trinoEnv.add("-D" + kv);
    }

    protected void put(String kv) {
        trinoEnv.add(kv);
    }

    protected void putEnvExport(String kv) {
        trinoEnvExport.add("export " + kv);
    }

    protected String catalogFilter(String catalogsPath) throws IOException {
        Configuration conf = new Configuration();
        LocalFileSystem fs = FileSystem.getLocal(conf);
        FileStatus[] status = fs.listStatus(new Path(catalogsPath));
        List<String> fileNames = CollUtil.newArrayList();
        for (FileStatus fileStatus : status) {
            fileNames.add(fileStatus.getPath().getName());
        }
        List<String> catalogs = PrestoSQLHelper.getStatementData(jobInfo.getSql());
        List<String> catalogsNew = new ArrayList<>();
        for (String catalog : catalogs) {
            String properties = catalog + ".properties";
            if (fileNames.contains(properties)) {
                catalogsNew.add(properties);
            } else {
                throw new RuntimeException("catalog :" + catalog + " is exist");
            }
        }

        for (String catalog : catalogsNew) {
            String catalogPath = catalogsPath + "/" + catalog;
            fs.copyToLocalFile(new Path(catalogPath), new Path(catalogNew));
        }

        return catalogNew;
    }
}
