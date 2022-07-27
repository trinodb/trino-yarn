package com.trino.on.yarn.server;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.constant.RunType;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.executor.TrinoJdbc;

import java.io.IOException;
import java.util.List;

public class ClientServer extends Server {

    //0,等待;1,正常结束;其他,异常结束
    private static final JSONObject start = JSONUtil.createObj().putOpt("start", 0);
    private static final List<JobInfo> nodes = CollUtil.newArrayList();

    /**
     * 初始化Client接口
     *
     * @return
     */
    public static SimpleServer initClient() {
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction(CLIENT_RUN, (request, response) -> {
            JobInfo jobInfo = JSONUtil.toBean(request.getBody(), JobInfo.class);

            if (null != jobInfo && jobInfo.isStart()) {
                nodes.add(jobInfo);
                if (isaBoolean(jobInfo)) {
                    //等待,等待所有节点初始化完毕
                    ThreadUtil.sleep(2000);
                    try {
                        TrinoJdbc.run(jobInfo.getIpMaster(), jobInfo.getPortTrino(), jobInfo.getUser(), jobInfo.getSql());
                        start.putOpt("start", 1);
                    } catch (Exception e) {
                        start.putOpt("start", 2);
                        LOG.error("TrinoJdbc.run error,sql:" + jobInfo.getSql(), e);
                    }

                    //先关闭node
                    for (JobInfo node : nodes) {
                        if (node.isNode()) {
                            String masterEnd = formatUrl(NODE_END, node.getIpNode(), node.getPortNode());
                            HttpUtil.post(masterEnd, start.toString());
                            ThreadUtil.sleep(2000);
                        }
                    }
                    // TODO: 2022/7/26 后面再优化(一定要等待node关闭,再关闭Master)
                    ThreadUtil.sleep(2000);
                    String masterEnd = formatUrl(MASTER_END, jobInfo.getIpMaster(), jobInfo.getPortMaster());
                    HttpUtil.post(masterEnd, start.toString());

                    FINISH = start.getInt("start");
                }
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

    private static boolean isaBoolean(JobInfo jobInfo) {
        if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType()) && jobInfo.getNumTotalContainers() == 1) {
            return nodes.size() == 1;
        } else {
            return nodes.size() == jobInfo.getNumTotalContainers() + 1;
        }
    }
}
