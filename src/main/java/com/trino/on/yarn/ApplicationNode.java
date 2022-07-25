package com.trino.on.yarn;

import cn.hutool.core.codec.Base64;
import cn.hutool.core.util.RuntimeUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.trino.on.yarn.entity.JobInfo;
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
    private JobInfo jobInfo;
    private static Process exec = null;
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> RuntimeUtil.destroy(exec)));
    }

    public static void main(String[] args) {
        try {
            ApplicationNode applicationNode = new ApplicationNode();
            applicationNode.init(args);

            LOG.info("ApplicationNode finish");
            System.exit(0);
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationNode", t);
            LogManager.shutdown();
            ExitUtil.terminate(1, t);
            System.exit(2);
        } finally {
            RuntimeUtil.destroy(exec);
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
    }
}
