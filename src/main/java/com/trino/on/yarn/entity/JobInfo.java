/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trino.on.yarn.entity;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

@Data
public class JobInfo {

    private String jdk11Home;
    private String sql;
    private String path;
    private String catalog;
    private String ip;
    private int port;
    private String ipMaster;
    private int portMaster;
    private boolean node;
    private String ipNode;
    private int portNode;
    private int portTrino;
    private boolean test;
    private boolean debug;
    private String user;
    private String runType;
    private int numTotalContainers;
    private int amMemory;
    private boolean start;

    public String getPluginPath() {
        return path + "/plugin";
    }

    public String getLibPath() {
        return path + "/lib";
    }

    public String getProcname(String os) {
        return path + "/bin/procname/" + os + "/libprocname.so";
    }

    public String getUser() {
        if (StrUtil.isNotBlank(user)) {
            return user;
        }
        return "hadoop";
    }

    public boolean isHdfsOrS3() {
        if (StrUtil.startWith(catalog, "hdfs://")) {
            return true;
        } else if (StrUtil.startWith(catalog, "s3a://")) {
            return true;
        } else if (StrUtil.startWith(catalog, "s3://")) {
            return true;
        } else if (StrUtil.startWith(catalog, "s3n://")) {
            return true;
        }
        return false;
    }

    public String getCatalogHdfs() {
        String catalog = this.catalog;
        if (StrUtil.startWith(catalog, "hdfs://")) {
            catalog = StrUtil.replace(this.catalog, "hdfs://", "");
            if (!StrUtil.startWith(catalog, "/")) {
                catalog = "/" + catalog;
            }
        } else if (StrUtil.startWith(catalog, "s3a://")) {
        } else if (StrUtil.startWith(catalog, "s3n://")) {
            catalog = StrUtil.replace(this.catalog, "s3n://", "s3a://");
        } else if (StrUtil.startWith(catalog, "s3://")) {
            catalog = StrUtil.replace(this.catalog, "s3://", "s3a://");
        }
        return catalog;
    }

    public JSONObject toJson() {
        return JSONUtil.parseObj(this);
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
