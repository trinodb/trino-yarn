package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.trino.on.yarn.entity.JobInfo;

import static com.trino.on.yarn.constant.Constants.TRINO_CONFIG_CONTENT;

public class TrinoExecutorMaster extends TrinoExecutor {

    public TrinoExecutorMaster(JobInfo jobInfo, int amMemory) {
        super(jobInfo, amMemory);
    }

    @Override
    protected void log(Process exec) throws InterruptedException {
        IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) line -> {
            if (StrUtil.contains(line, "======== SERVER STARTED ========") ||
                    StrUtil.contains(line, "==========")) {
                if (!endStart) {
                    endStart = true;
                    end();
                }
            }
            LOG.info(line);
            if (jobInfo.isDebug()) {
                HttpUtil.post(clientLogApi, line, 10000);
            }
        });
    }

    @Override
    protected String trinoConfig() {
        int nodeMemory = amMemory / 3 * 2;
        return StrUtil.format(TRINO_CONFIG_CONTENT, jobInfo.getIpMaster(), jobInfo.getPortTrino(),
                amMemory, nodeMemory, nodeMemory, jobInfo.getPortTrino(), path);
    }
}
