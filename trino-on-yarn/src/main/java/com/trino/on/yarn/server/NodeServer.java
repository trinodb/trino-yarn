package com.trino.on.yarn.server;

import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;

public class NodeServer extends Server {

    /**
     * 初始化master接口
     *
     * @return
     */
    public static SimpleServer initNode() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction(NODE_END, (request, response) -> {
            LOG.warn("node end:" + request.getBody());
            NODE_FINISH = JSONUtil.parseObj(request.getBody()).getInt("start", 0);
            responseWriteSuccess(response);
        }).start();
        return server;
    }
}
