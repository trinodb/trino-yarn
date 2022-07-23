package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.StrUtil;
import com.trino.on.yarn.entity.JobInfo;

import static com.trino.on.yarn.constant.Constants.TRINO_CONFIG_CONTENT;

public class TrinoExecutorNode extends TrinoExecutor {

    public TrinoExecutorNode(JobInfo jobInfo, int amMemory) {
        super(jobInfo, amMemory);
    }

    @Override
    protected void log(Process exec) throws InterruptedException {
        IoUtil.readUtf8Lines(exec.getInputStream(), (LineHandler) LOG::info);
    }

    @Override
    protected String trinoConfig() {
        super.amMemory = amMemory / 2;
        int nodeMemory = amMemory / 3 * 2;
        // TODO: 2022/7/22 config不通点在这里修改
        return StrUtil.format(TRINO_CONFIG_CONTENT, jobInfo.getIpMaster(), jobInfo.getPortTrino(),
                amMemory, nodeMemory, nodeMemory, NetUtil.getUsableLocalPort(), path);
    }
}