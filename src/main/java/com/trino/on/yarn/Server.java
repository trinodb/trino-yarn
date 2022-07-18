package com.trino.on.yarn;

import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.HttpServerResponse;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 *
 * 用于master/node和Client之间通信
 * @author duhanmin
 * @date 2020/7/6
 */
public class Server {

    private static final Log LOG = LogFactory.getLog(Server.class);
    public static Boolean masterFinish = Boolean.FALSE;

    /**
     * 初始化Client接口
     * @return
     */
    public static SimpleServer initClient() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction("/client/run", (request, response) -> {
                            JSONObject clientRun = JSONUtil.parseObj(request.getBody());
                            if (!JSONUtil.isNull(clientRun) && clientRun.containsKey("start")
                                    && clientRun.getBool("start",false)) {
                                String ip = clientRun.getStr("ip");
                                String trinoPort = clientRun.getStr("trinoPort");
                                // TODO:DUHANMIN 2022/7/18 查询trino jdbc逻辑

                                String port = clientRun.getStr("port");
                                String masterEnd = StrUtil.format("http://{}:{}/master/end", ip, port);
                                HttpUtil.get(masterEnd);
                            }else {
                                throw new IOException("master run false");
                            }
                            responseWriteSuccess(response);
                        }
                )
                .start();
        return server;
    }

    /**
     * 初始化master接口
     * @return
     */
    public static SimpleServer initMaster() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction("/master/end", (request, response) -> {
                            masterFinish = Boolean.TRUE;
                            responseWriteSuccess(response);
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
