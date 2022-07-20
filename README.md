# trino-on-yarn

trino-yarn可以让trino在yarn上运行

* master_memory内存为trino实际使用内存(master+node)
* yarn master内存内置128m(一般不需要修改)
* run参数设置yarn-per/yarn-session,yarn-per一次性进程,yarn-session常驻进程

### 启动

```shell
sudo yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -run_type yarn-per \
  -appname DemoApp \
  -master_memory 1024 \
  -queue default \
  -job_info '{"sql":"select * from table","jdk11Home":"/mnt/dss","path":"/mnt/dss"}'
```

### 停止

```shell
sudo yarn logs -applicationId  application_1642747413846_0462
```