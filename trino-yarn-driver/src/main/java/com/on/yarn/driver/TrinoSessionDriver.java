package com.on.yarn.driver;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.RuntimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public class TrinoSessionDriver implements TrinoSessionBase {

    private static YarnClient yarnClient = null;

    static {
        RuntimeUtil.addShutdownHook(new Thread(() -> {
            if (yarnClient != null) {
                yarnClient.stop();
            }
        }));
    }

    public static void main(String[] args) {
        System.out.println(RandomUtil.randomInt(0, 1));
    }

    public static String getDriverInfo(String hadoopConfDir) throws IOException, YarnException, URISyntaxException {
        Configuration conf = TrinoSessionBase.getEntries(hadoopConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        List<ApplicationReport> applications = yarnClient.getApplications();
        return TrinoSessionBase.getMeta(conf, applications);
    }
}
