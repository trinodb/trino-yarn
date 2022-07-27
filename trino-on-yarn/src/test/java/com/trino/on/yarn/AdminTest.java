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
package com.trino.on.yarn;

import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.core.util.SystemPropsUtil;
import cn.hutool.http.ContentType;
import cn.hutool.http.HttpUtil;
import cn.hutool.http.server.SimpleServer;
import com.trino.on.yarn.entity.JobInfo;
import com.trino.on.yarn.server.Server;
import org.apache.commons.cli.*;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class AdminTest {

    public static void main(String[] args) throws ParseException {
        System.out.println("args:" + ArrayUtil.toString(args));
        new AdminTest().testOptions(args);
    }

    @Test
    public void admin() {
        String data1 = "{\"id\": 1, \"msg\": \"OK\"}";
        String data2 = "{\"id\": 2, \"msg\": \"OK\"}";
        String data3 = "dsgfadsfdsafdasfds";
        SimpleServer server = HttpUtil.createServer(0);
        server.addAction("/restTest", (request, response) -> {
                    assert request.getBody().equals(data1);
                    response.write("{\"id\": 1, \"msg\": \"OK\"}", ContentType.JSON.toString());
                }

        ).addAction("/restTest2", (request, response) -> {
                    assert request.getBody().equals(data2);
                    response.write("{\"id\": 2, \"msg\": \"OK\"}", ContentType.JSON.toString());
                }

        ).addAction("/restTest3", (request, response) -> {
                    assert request.getBody().equals(data3);
                    response.write(data3, ContentType.JSON.toString());
                }

        ).start();

        String localhostStr = Server.ip();
        System.out.println(localhostStr);
        String url = StrUtil.format("http://{}:{}/restTest", localhostStr, server.getAddress().getPort());
        String dataResponse = HttpUtil.post(url, data1);
        assert dataResponse.equals(data1);

        url = StrUtil.format("http://{}:{}/restTest2", localhostStr, server.getAddress().getPort());
        dataResponse = HttpUtil.post(url, data2);
        assert dataResponse.equals(data2);

        url = StrUtil.format("http://{}:{}/restTest3", localhostStr, server.getAddress().getPort());
        dataResponse = HttpUtil.post(url, data3);
        assert dataResponse.equals(data3);
    }

    @Test
    public void jobInfo() {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setSql("select * from table");
        jobInfo.setPath("/home/trino/lib");
        jobInfo.setIp("localhost");
        jobInfo.setPort(8080);
        System.out.println(jobInfo);
    }

    @Test
    public void linux_x86_64() {
        Map<String, String> getenv = System.getenv();
        System.out.println(getenv);
        String system = System.getProperty("os.name", "Linux") + "-" + System.getProperty("os.arch", "x86_64");
        System.out.println(system);
        System.out.println("-----------------------");
        Properties props = SystemPropsUtil.getProps();
        for (Map.Entry<Object, Object> objectObjectEntry : props.entrySet()) {
            System.out.println(objectObjectEntry.getKey() + " = " + objectObjectEntry.getValue());
        }

    }

    @Test
    public void cliTest() throws ParseException {
        String[] arg = {"-p", "localhost", "-g", "-n", "{\"sql\":\"insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')\",\"jdk11Home\":\"/usr/lib/jvm/java-11-amazon-corretto.x86_64\",\"path\":\"/mnt/dss/trino\",\"catalog\":\"/mnt/dss/trino/catalog\",\"debug\":true}"};
        testOptions(arg);
    }

    public void testOptions(String[] args) throws ParseException {
        //Create GNU like options
        Options gnuOptions = new Options();
        gnuOptions.addOption("p", "print", false, "Print")
                .addOption("g", "gui", false, "GUI")
                .addOption("n", true, "Scale");
        CommandLineParser gnuParser = new GnuParser();
        CommandLine cmd = gnuParser.parse(gnuOptions, args);
        if (cmd.hasOption("p")) {
            System.out.println("p option was used.");
        }
        if (cmd.hasOption("g")) {
            System.out.println("g option was used.");
        }
        if (cmd.hasOption("n")) {
            System.out.println("Value passed: " + cmd.getOptionValue("n"));
        }
    }

}
