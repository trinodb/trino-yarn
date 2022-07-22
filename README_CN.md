# trino-on-yarn

[English](README.md) | [中文](README_CN.md)

trino-yarn可以让trino在yarn上运行

* master_memory内存为trino实际使用内存(master+node)
* 由于trino保留0.3倍内存用于缓存,所以能使用master_memory可能比实际内存小
* yarn master内存内置128m(一般不需要修改)
* run参数设置yarn-per(已实现单master运行)/yarn-session(待实现),yarn-per一次性进程,yarn-session常驻进程

### 启动

```shell
sudo yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -run_type yarn-per \
  -appname DemoApp \
  -master_memory 1024 \
  -queue default \
  -job_info  /mnt/dss/trino/testJob.json
```

* job_info参数

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog"
}
```

* job_info自定义用户提交

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "user": "hanmin.du"
}
```

* job_info将master日志打到Client

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "debug": true
}
```
* 运行示例
![image](https://user-images.githubusercontent.com/28647031/180349087-5138c867-58ef-4747-8bf5-802b5fec1167.png)

### 日志

```shell
sudo yarn logs -applicationId application_1642747413846_0462
```

### 停止

```shell
sudo yarn application -kill application_1642747413846_0462
```

### 附录

* [jdk11下载](https://jdk.java.net/java-se-ri/11)
* [trino下载](https://repo1.maven.org/maven2/io/trino/trino-server/363/)
