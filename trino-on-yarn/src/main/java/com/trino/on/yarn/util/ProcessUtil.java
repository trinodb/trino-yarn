package com.trino.on.yarn.util;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import com.trino.on.yarn.entity.JobInfo;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.UUID;

@Slf4j
public class ProcessUtil {
    private static final String PORT = "ps -ef | grep {} | grep io.trino.server.TrinoServer | grep -v \"grep\" | awk '{print $2}' | xargs kill -9";
    private static final String PORT_2 = "ps -ef | grep {} | grep io.trino.server.TrinoServer | awk '{print $2}' | xargs kill -9";

    public static boolean killPid(Process process, JobInfo info) {
        if (StrUtil.isNotBlank(info.getAppId())) {
            killAll(String.valueOf(info.getAppId()));
        }
        RuntimeUtil.destroy(process);
        return true;
    }

    private static void killAll(String port) {
        String command = StrUtil.format(PORT, port) + StrPool.LF + StrUtil.format(PORT_2, port);
        log.info("kill process command: {}", command);
        String tmpPath = "/tmp/trino/";
        if (!FileUtil.exist(tmpPath)) {
            FileUtil.mkdir(tmpPath);
        }
        tmpPath = tmpPath + UUID.randomUUID().toString() + ".sh";
        String absolutePath = FileUtil.writeUtf8String(command, tmpPath).getAbsolutePath();
        exec("sh " + absolutePath);
        FileUtil.del(tmpPath);
    }

    public static boolean exec(String command) {
        boolean result;
        Process process = null;
        InputStream inputStream = null;
        try {
            //杀掉进程
            process = RuntimeUtil.exec(command);
            inputStream = process.getInputStream();
            IoUtil.readUtf8Lines(inputStream, (LineHandler) log::info);
            assert process.waitFor() == 0;
            result = true;
        } catch (Exception e) {
            log.error("process error" + command, e);
            result = false;
        } finally {
            RuntimeUtil.destroy(process);
            IoUtil.close(inputStream);
        }
        return result;
    }
}
