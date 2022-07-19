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

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.net.NetUtil;
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

import static com.trino.on.yarn.constant.Constants.*;

public class TrinoExecutor {
    protected static final Log LOG = LogFactory.getLog(TrinoExecutor.class);

    private JobInfo jobInfo;
    private SimpleServer server;
    private int amMemory;
    private String ip = Server.ip();
    private int trinoPort = NetUtil.getUsableLocalPort();

    public TrinoExecutor(JobInfo jobInfo, SimpleServer server, int amMemory) {
        this.jobInfo = jobInfo;
        this.server = server;
        this.amMemory = amMemory;
    }

    public Process run() {
        Process exec = start();
        end();
        return exec;
    }

    public Process start() {
        // TODO:DUHANMIN 2022/7/18 trino 启动逻辑
        String conf = createConf();

        Process exec = RuntimeUtil.exec("ls -la");
        IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) System.out::println);
        return exec;
    }

    private String createConf() {
        String path = new File("./").getAbsolutePath();
        LOG.warn("trino path: " + path);
        final String conf = path + "/conf/";

        String log = StrUtil.format(TRINO_LOG_CONTENT, "WARN");
        FileUtil.writeUtf8String(log, conf + TRINO_LOG);

        String jvm = StrUtil.format(TRINO_JVM_CONTENT, amMemory);
        FileUtil.writeUtf8String(jvm, conf + TRINO_JVM);

        String env = StrUtil.format(TRINO_ENV_CONTENT, jobInfo.getJdk11Home(), ip, trinoPort);
        FileUtil.writeUtf8String(env, conf + TRINO_ENV);

        String node = StrUtil.format(TRINO_NODE_CONTENT, StrUtil.uuid(), path, path, jobInfo.getPluginPath());
        FileUtil.writeUtf8String(node, conf + TRINO_NODE);

        String config = StrUtil.format(TRINO_CONFIG_CONTENT, ip, trinoPort, amMemory, amMemory, amMemory, trinoPort, path);
        FileUtil.writeUtf8String(config, conf + TRINO_CONFIG);

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
}
