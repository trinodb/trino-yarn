package com.trino.on.yarn.server;

import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.executor.TrinoJdbc;

import java.io.IOException;

public class ClientServer extends Server {

    //0,等待;1,正常结束;其他,异常结束
    private static final JSONObject start = JSONUtil.createObj().putOpt("start", 0);

    /**
     * 初始化Client接口
     *
     * @return
     */
    public static SimpleServer initClient() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction(CLIENT_RUN, (request, response) -> {
            JSONObject clientRun = JSONUtil.parseObj(request.getBody());
            if (!JSONUtil.isNull(clientRun) && clientRun.containsKey("start")
                    && clientRun.getBool("start", false)) {
                String ip = clientRun.getStr("ip");
                Integer trinoPort = clientRun.getInt("trinoPort");
                String user = clientRun.getStr("user");
                String sql = clientRun.getStr("sql");
                try {
                    start.putOpt("start", 1);
                    TrinoJdbc.run(ip, trinoPort, user, sql);
                } catch (Exception e) {
                    start.putOpt("start", 2);
                    LOG.error("TrinoJdbc.run error,sql:" + sql, e);
                }

                Integer port = clientRun.getInt("port");
                String masterEnd = formatUrl(MASTER_END, ip, port);
                HttpUtil.post(masterEnd, start.toString());
            } else {
                throw new IOException("master run false");
            }
            responseWriteSuccess(response);
        }).addAction(CLIENT_LOG, (request, response) -> {
            LOG.info(request.getBody());
            responseWriteSuccess(response);
        }).start();
        return server;
    }
}
