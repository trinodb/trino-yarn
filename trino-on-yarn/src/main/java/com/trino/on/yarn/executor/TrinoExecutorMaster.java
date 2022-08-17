package com.trino.on.yarn.executor;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import com.trino.on.yarn.constant.RunType;
import com.trino.on.yarn.entity.JobInfo;

import java.io.InputStream;

import static com.trino.on.yarn.constant.Constants.TRINO_CONFIG_CONTENT;

public class TrinoExecutorMaster extends TrinoExecutor {

    public TrinoExecutorMaster(JobInfo jobInfo, int amMemory) {
        super(jobInfo, amMemory);
    }

    @Override
    protected void log(Process exec) throws InterruptedException {
        long start = System.currentTimeMillis();

        ThreadUtil.execAsync(() -> {
            InputStream inputStream = exec.getInputStream();
            IoUtil.readUtf8Lines(inputStream, (LineHandler) line -> {
                if (StrUtil.contains(line, "======== SERVER STARTED ========") ||
                        StrUtil.contains(line, "==========")) {
                    if (!endStart) {
                        endStart = true;
                        end();
                    }
                }
                if (!endStart && (System.currentTimeMillis() - start) > 1000 * 60) {
                    endStart = true;
                    end();
                }
                LOG.info(line);
                if (jobInfo.isDebug()) {
                    HttpUtil.post(clientLogApi, line, 10000);
                }
            });

            try {
                if (exec.waitFor() != 0) {
                    LOG.info("job error");
                } else {
                    LOG.info("job successfully");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("job error", e);
            } finally {
                IoUtil.close(inputStream);
                RuntimeUtil.destroy(exec);
            }
        });
    }

    @Override
    protected String trinoConfig() {
        if (RunType.YARN_PER.getName().equalsIgnoreCase(jobInfo.getRunType())) {
            if (jobInfo.getNumTotalContainers() == 1) {
                super.nodeSchedulerIncludeCoordinator = true;
            }
        } else {
            amMemory = amMemory / 2;
        }
        int nodeMemory = amMemory / 3 * 2;
        return StrUtil.format(TRINO_CONFIG_CONTENT, true, jobInfo.getIpMaster(), jobInfo.getPortTrino(),
                amMemory, nodeMemory, nodeMemory, jobInfo.getPortTrino(), path);
    }
}
