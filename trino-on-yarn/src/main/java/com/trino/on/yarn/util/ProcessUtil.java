package com.trino.on.yarn.util;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Field;

@Slf4j
public class ProcessUtil {
    private static final String PORT = "netstat -nlp | grep :{} | awk '{print $7}' | awk -F\"/\" '{ print $1 }' | xargs kill -9";

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

    public static boolean killPid(Process process, int portTrino) {
        killByPort(String.valueOf(portTrino));
        String processId = getProcessId(process);
        boolean bool = killProcessByPid(processId);
        RuntimeUtil.destroy(process);
        return bool;
    }

    private static void killByPort(String port) {
        String command = StrUtil.format(PORT, port);
        RuntimeUtil.exec(command);
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
            result = true;
        } catch (Exception e) {
            log.error("process error", e);
            result = false;
        } finally {
            RuntimeUtil.destroy(process);
            IoUtil.close(inputStream);
        }
        return result;
    }
}
