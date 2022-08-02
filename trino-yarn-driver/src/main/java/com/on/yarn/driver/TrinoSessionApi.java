package com.on.yarn.driver;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class TrinoSessionApi {

    public static String getApiInfo(String hadoopConfDir) throws IOException, URISyntaxException {
        Configuration conf = TrinoSessionBase.getEntries(hadoopConfDir);
        String address = conf.get("yarn.resourcemanager.webapp.address");
        if (StrUtil.isBlank(address)) {
            System.out.println("No trino-session yarn address found");
            System.exit(1);
        }
        String url = "http://" + address + "/ws/v1/cluster/apps?state=RUNNING";
        String data = HttpUtil.get(url);
        JSONArray apps = JSONUtil.parseObj(data).getJSONObject("apps").getJSONArray("app");
        List<ApplicationReport> applicationReports = new ArrayList<>();
        for (int i = 0; i < apps.size(); i++) {
            JSONObject json = apps.getJSONObject(i);
            ApplicationReport report = Records.newRecord(ApplicationReport.class);
            report.setApplicationId(ApplicationId.fromString(json.getStr("id")));
            report.setApplicationType(json.getStr("applicationType"));
            if (json.containsKey("applicationTags")) {
                List<String> applicationType = json.getJSONArray("applicationTags").toList(String.class);
                report.setApplicationTags(CollUtil.newHashSet(applicationType));
            }
            applicationReports.add(report);
        }
        return TrinoSessionBase.getMeta(conf, applicationReports);
    }
}
