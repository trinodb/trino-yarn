package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import com.trino.on.yarn.entity.JobInfo;

import static com.trino.on.yarn.constant.Constants.TRINO_CONFIG_CONTENT;

public class TrinoExecutorNode extends TrinoExecutor {

    public TrinoExecutorNode(JobInfo jobInfo, int amMemory) {
        super(jobInfo, amMemory);
    }

    @Override
    protected void log(Process exec) throws InterruptedException {
        ThreadUtil.execAsync(() -> IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) line -> {
            if (StrUtil.contains(line, "======== SERVER STARTED ========") ||
                    StrUtil.contains(line, "==========")) {
                if (!endStart) {
                    endStart = true;
                    end();
                }
            }
            LOG.info(line);
        }));
    }

    @Override
    protected String trinoConfig() {
        int nodeMemory = amMemory / 3 * 2;
        return StrUtil.format(TRINO_CONFIG_CONTENT, false, jobInfo.getIpMaster(), jobInfo.getPortTrino(),
                amMemory, nodeMemory, nodeMemory, NetUtil.getUsableLocalPort(), path);
    }
}