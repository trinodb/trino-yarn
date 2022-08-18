package com.trino.on.yarn.util;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.io.LineHandler;
import cn.hutool.core.util.RuntimeUtil;
import com.sun.jna.Platform;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.lang.reflect.Field;

@Slf4j
public class ProcessUtil {
    public static String getProcessId(Process process) {
        long pid = -1;
        Field field;
        if (Platform.isWindows()) {
            try {
                field = process.getClass().getDeclaredField("handle");
                field.setAccessible(true);
                pid = Kernel32.INSTANCE.GetProcessId((Long) field.get(process));
            } catch (Exception ex) {
                log.error("get process id for windows error {0}", ex);
            }
        } else if (Platform.isLinux() || Platform.isAIX() || Platform.isMac()) {
            try {
                Class<?> clazz = Class.forName("java.lang.UNIXProcess");
                field = clazz.getDeclaredField("pid");
                field.setAccessible(true);
                pid = (Integer) field.get(process);
            } catch (Throwable e) {
                log.error("get process id for unix error {0}", e);
            }
        }
        return String.valueOf(pid);
    }

    public static boolean killPid(Process process) {
        String processId = getProcessId(process);
        boolean bool = killProcessByPid(processId);
        RuntimeUtil.destroy(process);
        return bool;
    }

    /**
     * 关闭Linux进程
     *
     * @param pid 进程的PID
     */
    public static boolean killProcessByPid(String pid) {
        if (StringUtils.isEmpty(pid) || "-1".equals(pid)) {
            throw new RuntimeException("Pid ==" + pid);
        }
        Process process = null;
        InputStream inputStream = null;
        String command = "";
        boolean result;
        if (Platform.isWindows()) {
            command = "cmd.exe /c taskkill /PID " + pid + " /F /T ";
        } else if (Platform.isLinux() || Platform.isAIX()) {
            command = "kill -9 " + pid;
        }
        try {
            //杀掉进程
            process = RuntimeUtil.exec(command);
            inputStream = process.getInputStream();
            IoUtil.readUtf8Lines(inputStream, (LineHandler) System.out::println);
            result = true;
        } catch (Exception e) {
            log.error("kill pid error {0}", e);
            result = false;
        } finally {
            RuntimeUtil.destroy(process);
            IoUtil.close(inputStream);
        }
        return result;
    }
}
