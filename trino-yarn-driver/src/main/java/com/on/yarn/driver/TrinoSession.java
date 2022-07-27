package com.on.yarn.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class TrinoSession {
    public static Map<String, Object> getDriverInfo(String hadoopConfDir) throws IOException, YarnException {
        Configuration conf = new Configuration();
        conf.addResource(new Path(Paths.get(hadoopConfDir, "core-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "hdfs-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "yarn-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
        for (ApplicationReport application : yarnClient.getApplications()) {
            String applicationType = application.getApplicationType();
            if (application.getYarnApplicationState().equals(YarnApplicationState.RUNNING) && applicationType.equals("trino-session")) {
                System.out.println("applicationType:" + applicationType);
                System.out.println("applicationTags:" + application.getApplicationTags());
                System.out.println("applicationState:" + application.getYarnApplicationState().name());
                System.out.println("applicationType:" + application);
            }

        }
        yarnClient.stop();
        return null;
    }
}
