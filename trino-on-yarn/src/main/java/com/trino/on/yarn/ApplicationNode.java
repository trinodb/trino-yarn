package com.trino.on.yarn;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.thread.GlobalThreadPool;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.server.SimpleServer;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.executor.TrinoExecutorNode;
import com.trino.on.yarn.server.NodeServer;
import com.trino.on.yarn.server.Server;
import com.trino.on.yarn.util.ProcessUtil;
import lombok.Data;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ExitUtil;
import org.apache.log4j.LogManager;

@Data
public class ApplicationNode {

    protected static final Log LOG = LogFactory.getLog(ApplicationNode.class);
    private static JobInfo jobInfo;
    private static Process exec = null;

    static {
        RuntimeUtil.addShutdownHook(new Thread(() -> {
            ProcessUtil.killPid(exec, jobInfo);
            GlobalThreadPool.shutdown(true);
        }));
    }

    public static void main(String[] args) {
        boolean result = false;
        try {
            ApplicationNode applicationNode = new ApplicationNode();
            applicationNode.init(args);
            ThreadUtil.sleep(3000);
            exec = new TrinoExecutorNode(jobInfo, jobInfo.getAmMemory()).run();
            while (Server.NODE_FINISH.equals(0)) {
                ThreadUtil.sleep(500);
            }
            if (Server.NODE_FINISH.equals(1)) {
                result = true;
            }
            LOG.info("ApplicationNode finish");
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationNode", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
            System.exit(2);
        } finally {
            ProcessUtil.killPid(exec, jobInfo);
        }

        if (result) {
            LOG.info("Application Node completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Node failed. exiting");
            System.exit(2);
        }
    }

    public void init(String[] args) throws ParseException {
        Options opts = new Options();
        opts.addOption("job_info", true, "******json*******");

        CommandLine cliParser = new GnuParser().parse(opts, args);
        String jobInfoStr = Base64.decodeStr(cliParser.getOptionValue("job_info"));
        jobInfo = JSONUtil.toBean(jobInfoStr, JobInfo.class);
        LOG.warn("jobInfo:" + jobInfo);
        if (StrUtil.isNotBlank(jobInfo.getUser())) {
            System.setProperty("HADOOP_USER_NAME", jobInfo.getUser());
        }
        SimpleServer simpleServer = NodeServer.initNode();
        jobInfo.setIpNode(Server.ip());
        jobInfo.setPortNode(simpleServer.getAddress().getPort());
        jobInfo.setNode(true);
    }
}
