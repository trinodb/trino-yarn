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

import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.Data;

@Data
public class JobInfo {

    private String jdk11Home;
    private String sql;
    private String path;
    private String ip;
    private int port;
    private boolean test;
    private boolean debug;
    private String user;

    public String getPluginPath() {
        return path + "/plugin";
    }

    public String getLibPath() {
        return path + "/lib";
    }

    public String getCatalog() {
        return path + "/catalog";
    }

    public String getProcname(String os) {
        return path + "/bin/procname/" + os + "/libprocname.so";
    }


    public JSONObject toJson() {
        return JSONUtil.parseObj(this);
    }

    @Override
    public String toString() {
        return JSONUtil.toJsonStr(this);
    }
}
