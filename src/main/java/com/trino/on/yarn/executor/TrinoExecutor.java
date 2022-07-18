package com.trino.on.yarn.executor;

import cn.hutool.core.net.NetUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.Server;
import com.trino.on.yarn.entity.JobInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TrinoExecutor {

    private static final Log LOG = LogFactory.getLog(TrinoExecutor.class);

    public static void run(JobInfo jobInfo, SimpleServer server) throws Throwable {
        String clientRun = Server.formatUrl(Server.CLIENT_RUN, jobInfo.getIp(), jobInfo.getPort());
        int trinoPort = NetUtil.getUsableLocalPort();
        String body = JSONUtil.createObj()
                .putOpt("ip", Server.ip())
                .putOpt("port", server.getAddress().getPort())
                .putOpt("trinoPort", trinoPort)
                .putOpt("start", true).toString();
        HttpUtil.post(clientRun, body);

    }
}
