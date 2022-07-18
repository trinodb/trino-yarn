package com.trino.on.yarn.entity;

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

@Data
public class JobInfo {
    private String sql;
    private String libPath;
    private String configPath;
    private String catalogInfo;
    private String url;

    public JSONObject toJson() {
        return JSONUtil.parseObj(this);
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
