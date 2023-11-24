from clickhouse_driver import Client
import datetime
import requests
import re
from bs4 import BeautifulSoup
from prometheus_api_client import PrometheusConnect
import logging
import time
import pandas as pd
import json
from loguru import logger
logger.add('./ck-mo-qps.log', rotation="10 MB", format="{time} {level} {message}", level="INFO", retention="7 days")
"""
  监控clickhouse每一个用户的QPS是否达到所啥配置配额的80%；如果是则告警
  使用企业微信告警机器人告警
  并将监控信息推送一份至prometheus，以作图用
"""

qps_limit = {
    'user':['trade_user1','default'],
    'qps_limit': [100,200]
}
df_qps_limit = pd.DataFrame(qps_limit)

    
def query_qps(host):
    logger.info(f"query qps of {host}")
    global df_qps_limit
    with  Client(host=host,
                      port=9000,
                      user='ck_monitor',
                      password='xxxxx',
                      settings={'use_numpy': True}) as ck:
        
        select_sql = 'select user,count(*) as per_user_qps from system.processes group by user'

        df_qps = ck.query_dataframe(select_sql)
        # 使用merge将查询到的数据合并到df_qps_limit中，使用outer表示保留所有用户
        df_qps_compare = pd.merge(df_qps_limit, df_qps, on='user', how='outer')

        # 将NaN值替换为0
        df_qps_compare['per_user_qps'].fillna(0, inplace=True)
        logger.info(df_qps_compare)
       # 判断第三列是否大于第二列的80%
        alert_users = df_qps_compare[df_qps_compare['per_user_qps'] > 0.8 * df_qps_compare['qps_limit']]

        user_qps_alert = []
        for index, row in alert_users.iterrows():
            user = row['user']
            per_user_qps = row['per_user_qps']
            qps_limit = row['qps_limit']
            user_qps_alert.append({"host":host,"user":user,"per_user_qps":per_user_qps,"qps_limit":qps_limit})
        return df_qps_compare,user_qps_alert

def push_to_prometheus(df_qps_compare):
    for index, row in df_qps_compare.iterrows():
        user = row['user']
        per_user_qps = row['per_user_qps']
        qps_limit = row['qps_limit']        
        ck_user_qps_limit.labels(host=host,user=user).set(qps_limit) #value自己定义,但是一定要为 整数或者浮点数 
        ck_user_qps.labels(host=host,user=user).set(per_user_qps)

def send_alerts(user_qps_alert):
    for user_qps in user_qps_alert:
        user = user_qps["user"]
        host = user_qps["host"]
        qps = user_qps["per_user_qps"]
        qps_limit = user_qps["qps_limit"]
        url = "https://qyapi.weixin.qq.com/xxx"
        headers = {'Content-Type': 'application/json;charset=utf-8'}
        data_text = {
            "msgtype": "markdown",
            "markdown": {
                "content": f"<font color=\"warning\">【Clickhouse QPS 80%告警】</font>Ck节点：<font color=\"info\"> **{host}** </font> User:<font color=\"info\"> **{user}** </font> exceeded 80% of QPS limit. Now QPS is: <font color=\"info\">**{qps}**</font> ,  limit: **{qps_limit} ** ."
            }
        }
        r = requests.post(url,data=json.dumps(data_text),headers=headers)
        logger.info(f"Send QPS Alert {user}@{host} : QPS: {qps},QPS_LIMIT: {qps_limit}")
        
if __name__ == "__main__":
    from prometheus_client import Gauge,start_http_server 
    ck_user_qps_limit = Gauge('ck_user_qps_limit', 'Description of gauge: ck_user_qps_limit',labelnames=['host','user']) 
    ck_user_qps = Gauge('ck_user_qps', 'Description of gauge: ck_user_qps nowatimes',labelnames=['host','user']) 
    
    start_http_server(8000)
    while True:
        host_list = ['192.168.xx.xx','192.168.xx.xx','192.168.xx.xx']
        for host in host_list:
            df_qps_compare,user_qps_alert = query_qps(host)
            send_alerts(user_qps_alert)
            push_to_prometheus(df_qps_compare)
        time.sleep(30)
