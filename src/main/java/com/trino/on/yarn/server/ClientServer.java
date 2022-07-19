package com.trino.on.yarn.server;

import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;

import java.io.IOException;

public class ClientServer extends Server {

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
                // TODO:DUHANMIN 2022/7/18 查询trino jdbc逻辑

                Integer port = clientRun.getInt("port");
                String masterEnd = formatUrl(MASTER_END, ip, port);
                HttpUtil.get(masterEnd);
            } else {
                throw new IOException("master run false");
            }
            responseWriteSuccess(response);
        }).addAction(CLIENT_LOG, (request, response) -> {
            System.out.println(request.getBody());
            responseWriteSuccess(response);
        }).start();
        return server;
    }
}
