# trino-on-yarn

trino-yarn可以让trino在yarn上运行

* master_memory内存为trino实际使用内存(master+node)
* yarn master内存内置128m(一般不需要修改)

```shell

yarn jar /mnt/dss/trino-on-yarn-1.0.0.jar com.trino.on.yarn.Client \
  -jar_path /mnt/dss/trino-on-yarn-1.0.0.jar \
  -appname DemoApp \
  -master_memory 1024 \
  -queue default \
  -job_info {"sql":"select * from table"}
```
