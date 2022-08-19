package com.trino.on.yarn.util;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.text.StrPool;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import com.trino.on.yarn.entity.JobInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Field;

@Slf4j
public class ProcessUtil {
    private static final String PORT = "ps -ef | grep {} | grep io.trino.server.TrinoServer | grep -v \"grep\" | awk '{print $2}' | xargs kill -9";
    private static final String PORT_2 = "ps -ef | grep {} | grep io.trino.server.TrinoServer | awk '{print $2}' | xargs kill -9";

    private static String getProcessId(Process process) {
        long pid = -1;
        Field field;
        try {
            Class<?> clazz = Class.forName("java.lang.UNIXProcess");
            field = clazz.getDeclaredField("pid");
            field.setAccessible(true);
            pid = (Integer) field.get(process);
        } catch (Throwable e) {
            log.error("get process id for unix error {0}", e);
        }
        return String.valueOf(pid);
    }

    public static boolean killPid(Process process, JobInfo info) {
        if (StrUtil.isNotBlank(info.getAppId())) {
            killAll(String.valueOf(info.getAppId()));
        }
        String processId = getProcessId(process);
        boolean bool = killProcessByPid(processId);
        RuntimeUtil.destroy(process);
        return bool;
    }

    private static void killAll(String port) {
        String command = StrUtil.format(PORT, port) + StrPool.LF + StrUtil.format(PORT_2, port);
        log.info("kill process command: {}", command);
        String absolutePath = FileUtil.writeUtf8String(command, FileUtil.file(".")).getAbsolutePath();
        exec("sh " + absolutePath);
        FileUtil.del(absolutePath);
    }

    /**
     * 关闭Linux进程
     *
     * @param pid 进程的PID
     */
    private static boolean killProcessByPid(String pid) {
        if (StringUtils.isEmpty(pid) || "-1".equals(pid)) {
            throw new RuntimeException("Pid ==" + pid);
        }
        String command = "kill -9 " + pid;
        return exec(command);
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
