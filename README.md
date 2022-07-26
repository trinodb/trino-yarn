# trino-on-yarn

[English](README.md) | [中文](README_CN.md)

Trino-yarn Enables Trino to run on YARN

* Support yarn single execution (yarn-per) and yarn-session
* According to experience, the built-in memory ratio of master:node is 1:2, only need to set master_memory
* Since Trino reserves 0.3 times the memory for caching, each node may be able to use master_memory less than it really
  is
* master_memory Memory unit Currently supports MB
* Yarn Master /node Built-in memory 128 MB (do not change this parameter)
* The submission user can be displayed on YARN
* Supports sending master logs to Client to facilitate debugging
* Trino data source supports remote directories, such as HDFS/S3
* jdk11Home uses the environment variable JAVA11_HOME preference. if not, configure the jdk11Home parameter

### Yarn-per Submits task

```shell
/usr/bin/yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -run_type yarn-per \
  -appname DemoApp \
  -master_memory 1024 \
  -num_containers 2 \
  -queue default \
  -job_info  /mnt/dss/trino/testJob.json
```

* Job_info parameters

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog"
}
```

## parameter description

* run_type

Yarn Single-execution (yarn-per) and yarn Resident process (yarn-session)

* master_memory

As required, the memory ratio of master:node is 1:2, so only master_memory needs to be set. In addition, trino reserves
0.3 times of memory for cache, so each node can use master_memory may be smaller than the actual

* job_info

Example:

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "user": "hanmin.du",
  "debug": true
}
```

Parameter Description:

parameters | instructions
--- |---
SQL | needs to execute SQL
Jdk11Home | jdk11Home installation path
path | trino installation path
catalog | trino directory
user | submitted to the user
debug | is set to true to master log to the Client

catalog:

parameters | instructions
--- |---
local | /mnt/dss/trino/catalog
S3 | s3://bucket_name/tmp/catalog.zip
HDFS | hdfs://tmp/linkis/hadoop/catalog.zip

* Run the example
  ![image](https://user-images.githubusercontent.com/28647031/180349087-5138c867-58ef-4747-8bf5-802b5fec1167.png)

### logs

```shell
/usr/bin/yarn logs -applicationId application_1642747413846_0462
```

### stop

```shell
/usr/bin/yarn application -kill application_1642747413846_0462
```

### appendix

* [jdk11](https://jdk.java.net/java-se-ri/11)
* [trino](https://repo1.maven.org/maven2/io/trino/trino-server/363/)
