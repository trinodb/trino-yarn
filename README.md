# trino-on-yarn

[English](README.md) | [中文](README_CN.md)

Trino-yarn Enables Trino to run on YARN

* Master_memory is the actual memory used by trino (master+node)
* Since Trino reserves 0.3 times the memory for caching, master_memory may be smaller than actual memory
* Yarn Master Built-in memory 128M (generally do not need to change)
* Run sets yarn-per(single master running)/yarn-session(to be implemented),yarn-per one-time process,yarn-session
  permanent process

### Start

```shell
sudo yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -run_type yarn-per \
  -appname DemoApp \
  -master_memory 1024 \
  -queue default \
  -job_info  /mnt/dss/trino/testJob.json
```

* job_info: parameter

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog"
}
```

* job_info: Custom submission

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "user": "hanmin.du"
}
```

* job_info:The master logs are sent to the Client

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "debug": true
}
```

### logs

```shell
sudo yarn logs -applicationId application_1642747413846_0462
```

### stop

```shell
sudo yarn application -kill application_1642747413846_0462
```

### appendix

* [jdk11](https://jdk.java.net/java-se-ri/11)
* [trino](https://repo1.maven.org/maven2/io/trino/trino-server/363/)