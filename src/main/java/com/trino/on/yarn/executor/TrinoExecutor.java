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
package com.trino.on.yarn.executor;

import cn.hutool.core.net.NetUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.server.Server;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TrinoExecutor {

    private static final Log LOG = LogFactory.getLog(TrinoExecutor.class);

    public static void run(JobInfo jobInfo, SimpleServer server, int amMemory) throws Throwable {
        // TODODUHANMIN: 2022/7/18 trino 启动逻辑
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
