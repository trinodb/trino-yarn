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
package com.trino.on.yarn.constant;

/**
 * Constants
 */
public class Constants {

    public static final String SHELL_ARGS_PATH = "shellArgs";

    public static final String JAVA_OPTS_PATH = "javaOpts";

    public static final String JAVA_TRINO_CATALOG_PATH = "catalog";

    public static final String JAR_FILE_LINKEDNAME = "jar";

    public static final String APP_MASTER_JAR_PATH = "AppMaster.jar";

    public static final String JAR_FILE_PATH = "JAR_FILE_PATH";

    public static final String LOG_4_J_PATH = "log4j.properties";

    public static final String TRINO_LOG = "log.properties";

    /**
     * 最低级别INFO，还有DEBUG，INFO，WARN，ERROR
     */
    public static final String TRINO_LOG_CONTENT = "io.trino={}";

    public static final String TRINO_JVM = "jvm.config";

    public static final String TRINO_JVM_CONTENT =
            "-Xmx{}m\n" +
                    "-XX:+UseG1GC\n" +
                    "-XX:G1HeapRegionSize=32M\n" +
                    "-XX:+ExplicitGCInvokesConcurrent\n" +
                    "-XX:+HeapDumpOnOutOfMemoryError\n" +
                    "-XX:ReservedCodeCacheSize=150M\n" +
                    "-Dlog4j2.formatMsgNoLookups=true\n" +
                    "-Djava.library.path=/usr/lib/hadoop/lib/native/:/usr/lib/hadoop-lzo/lib/native/:/usr/lib/\n" +
                    "-Djdk.attach.allowAttachSelf=true\n" +
                    "--add-exports=java.base/jdk.internal.util=ALL-UNNAMED \n" +
            "--add-opens=java.base/jdk.internal.util=ALL-UNNAMED\n" +
            "--add-exports=java.base/jdk.internal.loader=ALL-UNNAMED\n" +
            "--add-opens=java.base/jdk.internal.loader=ALL-UNNAMED\n" +
            "--add-exports=java.base/jdk.internal.module=ALL-UNNAMED\n" +
            "--add-opens=java.base/jdk.internal.module=ALL-UNNAMED\n" +
            "--add-exports=java.base/java.lang.module=ALL-UNNAMED\n" +
            "--add-opens=java.base/java.lang.module=ALL-UNNAMED\n" +
            "--add-exports=java.base/jdk.internal.util.jar=ALL-UNNAMED\n" +
            "--add-opens=java.base/jdk.internal.util.jar=ALL-UNNAMED\n" +
            "--add-exports=java.base/jdk.internal.reflect=ALL-UNNAMED\n" +
            "--add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED\n" +
            "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED\n" +
            "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED\n" +
            "-Djava.rmi.server.hostname=localhost";

    public static final String TRINO_ENV = "trino-env.sh";

    /**
     * JAVA11_HOME=$(ls -d /usr/lib/jvm/java-11-amazon-corretto.*)
     * export JAVA_HOME=$JAVA11_HOME
     * export PATH=$JAVA_HOME/bin:$PATH
     * export LC_ALL=zh_CN.UTF-8
     * export EXTRA_ARGS="--server http://ip-*-*-*-*.vpc.internal:8889"
     */
    public static final String TRINO_ENV_CONTENT = "JAVA_HOME={}\n" +
            "PATH=$JAVA_HOME/bin:$PATH\n" +
            "LC_ALL=zh_CN.UTF-8\n" +
            "LANG=zh_CN.UTF-8\n" +
            "EXTRA_ARGS=\"--server http://{}:{}\"";

    public static final String TRINO_NODE = "node.properties";

    /**
     * node.environment=production
     * node.id=i-dsadsadasda
     * node.data-dir=/var/lib/trino/data
     * catalog.config-dir=/etc/trino/conf/catalog
     * plugin.dir=/usr/lib/trino/plugin
     */
    public static final String TRINO_NODE_CONTENT = "node.environment=production\n" +
            "node.id={}\n" +
            "node.data-dir={}/data\n" +
            "catalog.config-dir={}\n" +
            "plugin.dir={}";


    public static final String TRINO_CONFIG = "config.properties";

    /**
     * coordinator=true
     * node-scheduler.include-coordinator=true
     * discovery.uri=http://ip-*-*-*-*.vpc.internal:8889
     * http-server.threads.max=500
     * sink.max-buffer-size=1GB
     * query.max-memory=22938MB \\单个查询在集群的最大内存
     * query.max-memory-per-node=26401163969B \\单个查询在单个节点上的最大用户内存
     * query.max-total-memory-per-node=31681396763B \\单个查询在单个节点上的最大用户和系统内存
     * query.max-history=40
     * query.min-expire-age=30m
     * query.client.timeout=30m
     * query.stage-count-warning-threshold=100
     * query.max-stage-count=150
     * http-server.http.port=8889
     * http-server.log.path=/var/log/trino/http-request.log
     * http-server.log.max-size=67108864B
     * http-server.log.max-history=5
     * log.max-size=268435456B
     * log.max-history=5
     * jmx.rmiregistry.port = 9080
     * jmx.rmiserver.port = 9081
     * scheduler.http-client.max-requests-queued-per-destination=5120
     * scheduler.http-client.max-connections-per-server=5120
     * exchange.http-client.request-timeout=50s
     * exchange.http-client.max-requests-queued-per-destination=5120
     * exchange.http-client.max-connections-per-server=5120
     */
    public static final String TRINO_CONFIG_CONTENT = "coordinator=true\n" +
            "node-scheduler.include-coordinator={}\n" +
            "discovery.uri=http://{}:{}\n" +
            "http-server.threads.max=500\n" +
            "sink.max-buffer-size=1GB\n" +
            "query.max-memory={}MB\n" +
            "query.max-memory-per-node={}MB\n" +
            "query.max-total-memory-per-node={}MB\n" +
            "query.max-history=40\n" +
            "query.min-expire-age=30m\n" +
            "query.client.timeout=30m\n" +
            "query.stage-count-warning-threshold=100\n" +
            "query.max-stage-count=150\n" +
            "http-server.http.port={}\n" +
            "http-server.log.path={}/http-request.log\n" +
            "http-server.log.max-size=67108864B\n" +
            "http-server.log.max-history=5\n" +
            "log.max-size=268435456B\n" +
            "log.max-history=5\n";

    public static final String LOG_OUTPUT_FILE = "log.output-file";

    public static final String LOG_LEVELS_FILE = "log.levels-file";

    public static final String CONFIG = "config";

}
