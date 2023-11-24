# ops_scripts
运维工作中写的脚本
## 1. auto_restart_downstream_dag.py
通过airflow api，用户给定dag_id和重跑时间，自动调起其下游依赖。
```sh
~# python3 auto_restart_downstream_dag.py 
2023-10-13 14:54:36.764 | INFO     | __main__:fetch_deps_data:51 - truncate table dag_dps.dag_dps
2023-10-13 14:54:36.772 | INFO     | __main__:fetch_deps_data:55 - 175 dag dependencies records upserted successfully
usage: auto_restart_downstream_dag.py [-h] [-d DAG_ID] [-date DATE] [-t TYPE] [-i INTERACTION]

需要提供需要重跑的（失败的）dag_id，没有失败的检测不到，脚本将检测其是否存在下游dag，如果存在则`递归`调起下游dag。

optional arguments:
  -h, --help            show this help message and exit
  -d DAG_ID, --dag_id DAG_ID
                        指定dag_id
  -date DATE, --date DATE
                        指定需要重跑的日期，默认为今天，格式yyyy-MM-dd
  -t TYPE, --type TYPE  重跑类型 1.restart: 默认值。只重跑自己和自己下游失败的dag 2.rerun: 重跑自己和下游所有的dag, 不管是否失败
  -i INTERACTION, --interaction INTERACTION
                        默认非交互式。数值0或者1，是否交互式运行；交互式运行可以选择需要运行的dag

```
## 2. monitor_ck_qps.py
监控clickhouse每一个用户的QPS是否达到所啥配置配额的80%；如果是则告警； 使用企业微信告警机器人告警；并将监控信息推送一份至prometheus，以作图用。

`restart.sh`
```sh
#!/bin/bash
ps -ef | grep "monitor_ck_qps.py" | grep -v grep | awk '{print $2}' | xargs kill -9
nohup /monitor/diy_script/diy_monitor/bin/python monitor_ck_qps.py > /dev/null 2>&1 &
```
