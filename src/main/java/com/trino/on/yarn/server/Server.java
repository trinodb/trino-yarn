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
package com.trino.on.yarn.server;

import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.server.HttpServerResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 用于master/node和Client之间通信
 *
 * @author duhanmin
 * @date 2022/7/18
 */
public class Server {
    protected static final Log LOG = LogFactory.getLog(Server.class);
    public static final String CLIENT_RUN = "/client/run";
    public static final String CLIENT_LOG = "/client/log";
    public static final String MASTER_END = "/master/end";
    public static final String NODE_END = "/node/end";
    public static Integer MASTER_FINISH = 0;//0,等待;1,正常结束;其他,异常结束
    public static Integer NODE_FINISH = 0;//0,等待;1,正常结束;其他,异常结束
    private static final String HTTP = "http://{}:{}";

    public static void responseWriteSuccess(HttpServerResponse response) {
        responseWrite(response, "{\"status\": 200}");
    }

    private static void responseWriteFail(HttpServerResponse response) {
        responseWrite(response, "{\"status\": 0}");
    }

    private static void responseWrite(HttpServerResponse response, String data) {
        response.write(data, ContentType.JSON.toString());
    }

    public static String ip() {
        for (String localIp : NetUtil.localIps()) {
            if (localIp.startsWith("10.")) {
                return localIp;
            }
        }
        return "";
    }

    public static String formatUrl(String urlSuffix, String ip, int port) {
        return StrUtil.format(HTTP + urlSuffix, ip, port);
    }

    public static void setMasterFinish(Integer masterFinish) {
        MASTER_FINISH = masterFinish;
    }
}
