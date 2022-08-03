package com.on.yarn.driver;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class TrinoSessionApi {

    public static String getApiInfo(String hadoopConfDir, String address) throws IOException, URISyntaxException {
        Configuration conf = TrinoSessionBase.getEntries(hadoopConfDir);
        String url = "http://" + address + "/ws/v1/cluster/apps?state=RUNNING";
        String data = HttpUtil.get(url);
        JSONArray apps = JSONUtil.parseObj(data).getJSONObject("apps").getJSONArray("app");
        List<ApplicationReport> applicationReports = new ArrayList<>();
        for (int i = 0; i < apps.size(); i++) {
            JSONObject json = apps.getJSONObject(i);
            ApplicationReport report = Records.newRecord(ApplicationReport.class);
            report.setApplicationId(ApplicationId.fromString(json.getStr("id")));
            report.setApplicationType(json.getStr("applicationType"));
            report.setYarnApplicationState(json.getEnum(YarnApplicationState.class, "state"));
            if (json.containsKey("applicationTags")) {
                report.setApplicationTags(CollUtil.newHashSet(json.getStr("applicationTags")));
            }
            applicationReports.add(report);
        }
        return TrinoSessionBase.getMeta(conf, applicationReports);
    }
}
