package com.trino.on.yarn;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import org.junit.Test;

public class AdminTest {

    @Test
    public void admin() {
        String data = "{\"id\": 1, \"msg\": \"OK\"}";
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction("/restTest", (request, response) ->
                response.write("{\"id\": 1, \"msg\": \"OK\"}", ContentType.JSON.toString())
        ).start();

        String url = StrUtil.format("http://localhost:{}/restTest", server.getAddress().getPort());
        String dataResponse = HttpUtil.get(url);
        assert dataResponse.equals(data);
    }

}
