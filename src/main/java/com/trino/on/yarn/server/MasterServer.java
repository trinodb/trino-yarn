package com.trino.on.yarn.server;

import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;

public class MasterServer extends Server {

    /**
     * 初始化master接口
     *
     * @return
     */
    public static SimpleServer initMaster() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction(MASTER_END, (request, response) -> {
            MASTER_FINISH = JSONUtil.parseObj(request.getBody()).getInt("start", 0);
            responseWriteSuccess(response);
        }).start();
        return server;
    }
}
