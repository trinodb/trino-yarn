# trino-on-yarn

[English](README.md) | [中文](README_CN.md)

trino-yarn可以让trino在yarn上运行

* 支持yarn单次执行(yarn-per)和yarn常驻进程(yarn-session)
* 根据经验内置master:node内存比为1:2,只需要设置master_memory即可
* 由于trino保留0.3倍内存用于缓存,所以每个节点能使用master_memory可能比实际小
* master_memory内存单位目前支持MB
* yarn master/node内存内置128m(一般不需要修改)
* 支持yarn上显示提交用户
* 支持将master日志打到Client,方便debug
* trino数据源支持远程目录,例如hdfs/s3等
* jdk11Home优先使用环境变量JAVA11_HOME,如果没有则使用配置jdk11Home参数

### yarn-per提交任务

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

* job_info参数

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog"
}
```

### 参数说明

* run_type

yarn单次执行(yarn-per)和yarn常驻进程(yarn-session)

* master_memory

根据需要设置,其中master:node内存比为1:2,只需要设置master_memory即可,另外trino保留0.3倍内存用于缓存,所以每个节点能使用master_memory可能比实际小

* job_info

例子:

```json
{
  "sql": "insert into tmp.pe_ttm_35(stock_code, pe_ttm,date,pt) values('qw', rand()/random(),'1','2')",
  "jdk11Home": "/usr/lib/jvm/java-11-amazon-corretto.x86_64",
  "path": "/mnt/dss/trino",
  "catalog": "/mnt/dss/trino/catalog",
  "user": "hanmin.du",
  "debug": false
}
```

参数说明:

参数 |说明
--- |---
sql | 需要执行的sql
jdk11Home | jdk11Home安装路径
path |trino安装路径
catalog | trino目录
user |提交用户
debug |设置为true可将master日志打到Client

catalog:

参数 |说明
--- |---
local | /mnt/dss/trino/catalog
S3 | s3://bucket_name/tmp/catalog.zip
HDFS | hdfs://tmp/linkis/hadoop/catalog.zip

注意:只有local模式提供目录,其他需要zip格式

* 运行示例
  ![image](https://user-images.githubusercontent.com/28647031/180349087-5138c867-58ef-4747-8bf5-802b5fec1167.png)

### yarn-session提交任务

```shell
/usr/bin/yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -run_type yarn-session \
  -appname DemoApp \
  -master_memory 1024 \
  -num_containers 2 \
  -queue default \
  -job_info  /mnt/dss/trino/testJob.json
```

可以从日志中找到trino Master的ip和端口

![image](https://user-images.githubusercontent.com/28647031/181228150-fa9cd89d-d022-4b12-b217-49827dd5a1e7.png)


### 日志

```shell
/usr/bin/yarn logs -applicationId application_1642747413846_0462
```

### 停止

```shell
/usr/bin/yarn application -kill application_1642747413846_0462
```

### 附录

* [jdk11](https://jdk.java.net/java-se-ri/11)
* [trino](https://repo1.maven.org/maven2/io/trino/trino-server/363/)
