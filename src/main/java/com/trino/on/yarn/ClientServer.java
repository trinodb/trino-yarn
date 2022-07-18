package com.trino.on.yarn;

import cn.hutool.core.net.NetUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.HttpServerResponse;
import cn.hutool.http.server.SimpleServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.LinkedHashSet;

/**
 *
 * 用于master/node和Client之间通信
 * @author duhanmin
 * @date 2020/7/6
 */
public class ClientServer {

    private static final Log LOG = LogFactory.getLog(ClientServer.class);

    public static SimpleServer init() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction("/master", (request, response) -> {
                            try {
                                LOG.info("Error running Client");
                                responseWriteSuccess(response);
                            }catch (Exception e) {
                                responseWriteFail(response);
                            }
                        }
                )
                .addAction("/node", (request, response) -> {
                            try {
                                LOG.info("Error running Client");
                                responseWriteSuccess(response);
                            }catch (Exception e) {
                                responseWriteFail(response);
                            }
                        }
                )
                .start();
        return server;
    }

    private static void responseWriteSuccess(HttpServerResponse response) {
        responseWrite(response,"{\"status\": 200}");
    }

    private static void responseWriteFail(HttpServerResponse response) {
        responseWrite(response,"{\"status\": 0}");
    }

    private static void responseWrite(HttpServerResponse response, String data) {
        response.write(data, ContentType.JSON.toString());
    }

    public static String ip(){
        for (String localIp : NetUtil.localIps()) {
            if (localIp.startsWith("10.")) {
                return localIp;
            }
        }
        return "";
    }


}
