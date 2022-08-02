package com.on.yarn.driver;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FastByteArrayOutputStream;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.CharsetUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

public interface TrinoSessionBase {
    public static final String TRINO_SESSION_S3 = "trino-session-s3";
    public static final String TRINO_SESSION_HDFS = "trino-session-hdfs";

    static Configuration getEntries(String hadoopConfDir) {
        Configuration conf = new Configuration();
        conf.addResource(new Path(Paths.get(hadoopConfDir, "core-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "hdfs-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        conf.addResource(new Path(Paths.get(hadoopConfDir, "yarn-site.xml").toAbsolutePath().toFile().getAbsolutePath()));
        return conf;
    }


    static String getMeta(Configuration conf, List<ApplicationReport> applications) throws IOException, URISyntaxException {
        List<ApplicationReport> apps = CollUtil.newArrayList();
        for (ApplicationReport application : applications) {
            String applicationType = application.getApplicationType();
            if (application.getYarnApplicationState().equals(YarnApplicationState.RUNNING) &&
                    (applicationType.equals(TRINO_SESSION_S3) || applicationType.equals(TRINO_SESSION_HDFS))) {
                apps.add(application);
            }
        }

        if (CollUtil.isEmpty(apps)) {
            System.out.println("No trino-session application found");
            System.exit(1);
        }

        int num = RandomUtil.randomInt(0, apps.size());
        ApplicationReport applicationReport = apps.get(num);
        String name = applicationReport.getApplicationType();
        String appId = applicationReport.getApplicationId().toString();

        boolean isS3 = false;
        String bucket = null;
        if (StrUtil.endWith(name, "-s3")) {
            isS3 = true;
            bucket = CollUtil.newArrayList(applicationReport.getApplicationTags()).get(0);
        }

        JSONObject app = JSONUtil.createObj().putOpt("name", name).putOpt("appId", appId).putOpt("s3", isS3).putOpt("bucket", bucket);

        String path = "{}/tmp/trino/{}/";
        if (isS3) {
            path = StrUtil.format(path, "s3a://" + bucket, appId);
        } else {
            path = StrUtil.format(path, conf.get("fs.defaultFS", "hdfs://"), appId);
        }

        FileSystem fileSystem = getFileSystem(conf, path);
        FileStatus[] fst = fileSystem.listStatus(new Path(path));
        Path[] paths = FileUtil.stat2Paths(fst);
        if (ArrayUtil.isEmpty(paths)) {
            System.out.println("No trino-session path found");
            System.exit(1);
        }

        try (FastByteArrayOutputStream fastByteArrayOutputStream = new FastByteArrayOutputStream()) {
            try (FSDataInputStream fsDataInputStream = fileSystem.open(paths[0])) {
                IOUtils.copyBytes(fsDataInputStream, fastByteArrayOutputStream, 4096, false);
            }
            return fastByteArrayOutputStream.toString(CharsetUtil.CHARSET_UTF_8);
        }
    }

    static FileSystem getFileSystem(Configuration conf, String path) throws IOException, URISyntaxException {
        FileSystem fs;
        if (StrUtil.startWith(path, "s3")) {
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            fs = FileSystem.get(new URI(path), conf);
        } else {
            fs = FileSystem.get(conf);
        }
        return fs;
    }
}
