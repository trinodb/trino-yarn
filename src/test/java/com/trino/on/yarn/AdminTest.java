package com.trino.on.yarn;

import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import org.junit.Test;

import java.net.InetSocketAddress;

public class AdminTest {

    @Test
    public void admin() {
        try {
            SimpleServer server = HttpUtil.createServer(0);
            server.start();
            InetSocketAddress address = server.getAddress();
            System.out.println(JSONUtil.toJsonStr(address.getAddress().getHostAddress()));
        } catch (Throwable t) {
        }
    }

}
