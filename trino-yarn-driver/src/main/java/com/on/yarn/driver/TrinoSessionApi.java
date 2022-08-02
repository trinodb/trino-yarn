package com.on.yarn.driver;

import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import java.io.IOException;
import java.net.URISyntaxException;
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
        String json = HttpUtil.get(url);
        JSONArray apps = JSONUtil.parseObj(json).getJSONObject("apps").getJSONArray("app");
        List<ApplicationReport> applicationReports = JSONUtil.toList(apps, ApplicationReport.class);
        return TrinoSessionBase.getMeta(conf, applicationReports);
    }
}
