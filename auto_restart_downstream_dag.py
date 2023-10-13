import mysql.connector
import json
import logging
import time
import requests
import sys
import argparse 
import time
import requests
from datetime import datetime,timedelta,date
import logging
import json
from pprint import pprint
from loguru import logger
logger.add('./auto_restart_dag.log', rotation='10mb', format="{time} {level} {message}", level="INFO")

def fetch_deps_data():
    """
    获取dag依赖数据。从 serialized_dag 查询 dag_dependencies 并将结果写入 dag_dps.dag_deps
    """
    logging.info("获取dag依赖数据")
    connection = mysql.connector.connect(
        host="192.168.xx.xx",     # 数据库主机地址
        user="db_user", # 数据库用户名
        password="xxxxxx", # 数据库密码
        database="airflow_test_db"  # 数据库名称
    )
    cursor = connection.cursor()
    query = f"SELECT dag_id,data FROM serialized_dag " 
    cursor.execute(query)

    results = cursor.fetchall()
    records = []
    num = 1
    for row in results:
        dag_name = row[0]
        data = json.loads(row[1])
        dag_dps = data['dag']['dag_dependencies']
        for dps in dag_dps:
            source_dag = dps['source']
            target_dag = dps['target']
            dependency_id = dps['dependency_id']
            dependency_type = dps['dependency_type']
            dag_dps_info_tuple =(num,dag_name,source_dag,target_dag,dependency_id,dependency_type) 
            records.append(dag_dps_info_tuple) 
            num += 1       
    try:
        truncate_query = "truncate table dag_dps.dag_deps" 
        cursor.execute(truncate_query)
        logger.info("truncate table dag_dps.dag_dps")
        insert_query = "INSERT INTO dag_dps.dag_deps (deps_id,dag_name,source_dag,target_dag,dependency_id,dependency_type)  VALUES (%s,%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE dag_name=VALUES(dag_name), source_dag=VALUES(source_dag), target_dag=VALUES(target_dag)"
        cursor.executemany(insert_query, records)
        connection.commit()
        logger.info(f"{cursor.rowcount} dag dependencies records upserted successfully")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"Error upserting data: {str(e)}")
    cursor.close()
    connection.close()

  
# 查询dag依赖  
def trigger_downstream_dag(dag_id,rdate):
    connection = mysql.connector.connect(
        host="192.168.xx.xx",
        user="db_user",
        password="xxxxxx",
        database="dag_dps"
    )
    cursor = connection.cursor()
    query = f"select target_dag from dag_deps where source_dag='{dag_id}';" 
    cursor.execute(query)
    results = cursor.fetchall()
    if  len(results) == 0 :
        logger.warning(f"获取结果为空，【{dag_id}】无下游dag了")
    else:
        logger.info(f"结果不为空，存在下游dag {len(results)} 个")
        for downstream_dag in results:
            downstream_dag_id = downstream_dag[0]
            logger.info(f"dag依赖图： {dag_id} >> {downstream_dag_id}")
            logger.info(f"调起下游dag: {downstream_dag_id}")
            clear_dag(downstream_dag_id,rdate)
            trigger_downstream_dag(downstream_dag_id,rdate)
    cursor.close()
    connection.close() 


def get_downstream_dag(dag_id, path: list, result_list: list):
    connection = mysql.connector.connect(
        host="192.168.xx.xx",
        user="db_user",
        password="xxxxxx",
        database="dag_dps"
    )
    cursor = connection.cursor()
    query = f"select target_dag from dag_deps where source_dag='{dag_id}';"
    cursor.execute(query)
    results = cursor.fetchall()

    if len(results) == 0:
        result_list.append(path[:])  #copy
        return
    # pprint(results)
    for downstream_dag in results:
        downstream_dag_id = downstream_dag[0]
        path.append(downstream_dag_id)
        get_downstream_dag(downstream_dag_id, path, result_list)
        path.pop()


def get_dags_list(dag_id: str):
    path = [dag_id]
    result_list = []
    get_downstream_dag(dag_id, path, result_list)
    return result_list
    cursor.close()
    connection.close()


def select_dag(path_list):
    path_dict = {}
    count = 1
    for path in path_list:
        path_dict[count] = path
        count+= 1
    pprint(path_dict)
    selected_dag_list = []
    while True:
        user_input = input("选择需要重跑的路径，输入多个键（用空格分隔），或输入 'q' 退出: ")
        if user_input.lower() == 'q':
            print("退出")
            break
        if user_input.isdigit():
            user_choice = int(user_input)
            if user_choice in path_dict:
                selected_path = path_dict[user_choice]
                selected_dag_list = selected_path
                deps_path = ' >> ' .join(selected_dag_list)
                print('--------------------')
                print(f"Selected重跑依赖路径：{deps_path}")
                print('--------------------')
                logger.info(f"Selected重跑的dag：{deps_path}")
                break
            else:
                print(f"无效数字，请选择 1 到 {len(path_list)} 之间的键。")
        else:
            # 如果用户输入的不是数字，将其分割成一个列表
            user_input_list = user_input.split()  
            for i in user_input_list:
                if i.isdigit():
                    i= int(i)
                    selected_path = path_dict[i]
                    selected_dag_list = selected_dag_list + selected_path
                else:
                    selected_dag_list.append(i)
                    
            selected_dag_list = list(set(selected_dag_list))  # 去重
            print('______________________________________________')
            print(f"选择的dag：{selected_dag_list}")
            print('______________________________________________')    
            logger.warning(f"选择的dag：{selected_dag_list}")  
            break

    return selected_dag_list


def get_desire_dag_runid(dag_id,rdate,rerun_type):
    # 根据给出的时间（重跑时间 rdate）,和需要重跑的的dag_id
    rdate_date = datetime.strptime(rdate, '%Y-%m-%d')
    yesterday =  rdate_date - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')
    today_str = f"{rdate}T00:00:00.000z"         # 所给时间当天时间戳
    rdate_str = f"{yesterday_str}T00:00:00.000z" # 前一天0点时间戳
    logger.info(f"寻找在 {rdate} 0点之后的 {dag_id} 失败任务id")
    base_url = "http://192.168.xx.x:8080/api/v1"
    username = 'admin'
    password = 'Password'
    auth = (username,password)
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if rerun_type == 'restart':
        dags_url = f"http://192.168.xx.x:8080/api/v1/dags/{dag_id}/dagRuns?limit=1&execution_date_gte={rdate_str}&order_by=start_date&state=failed"
    elif rerun_type == 'rerun':
        dags_url = f"http://192.168.xx.x:8080/api/v1/dags/{dag_id}/dagRuns?limit=1&execution_date_gte={rdate_str}&execution_date_lte={today_str}&order_by=start_date"
    else:
        logger.error(f"rerun_type 参数异常,必须为 restart 或者 rerun")
    dag_runs_response = requests.get(f"{dags_url}", headers=headers,auth=auth)
    dag_runs = dag_runs_response.json()
    for runid in dag_runs['dag_runs']:
        dag_id = runid['dag_id']
        dag_run_id = runid['dag_run_id']
        state = runid['state']
        return dag_id,dag_run_id,state

# clear dag 状态函数
def clear_dag(dag_id, rdate):
    data = get_desire_dag_runid(dag_id, rdate, rerun_type)
    # 判断返回值是否为空
    if  data:      # 如果不为空
        dag_id,dag_run_id,state = data
        if rerun_type == 'restart':
            logger.warning(f"清理失败的 {dag_id} : {dag_run_id} 状态使其重跑")
        elif rerun_type == 'rerun':
            logger.warning(f"清理 {dag_id} : {dag_run_id} 状态使其重刷，无论此前成功与否")
        base_url = "http://192.168.xx.x:8080/api/v1"
        username = 'admin'
        password = 'Password'
        auth = (username,password)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        data={
        "dry_run": False
        }
        dags_url = f"{base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/clear"
        dag_runs_response = requests.post(f"{dags_url}", headers=headers,auth=auth,json=data)
        logger.info(f"HTTP POST: {dag_runs_response.status_code}" )
    else:
        if rerun_type == 'restart':
            logger.error(f"没找到 {dag_id} {rdate} 失败的dag_run_id ")
        elif rerun_type == 'rerun':
            logger.error(f"{dag_id} 在  {rdate} 未运行")
        


if __name__=="__main__":
    today_str = date.today().isoformat()
    parser = argparse.ArgumentParser(description="需要提供需要重跑的（失败的）dag_id，没有失败的检测不到，脚本将检测其是否存在下游dag，如果存在则`递归`调起下游dag。")
    parser.add_argument('-d', '--dag_id', type=str, help='指定dag_id')
    parser.add_argument('-date', '--date', type=str, default=today_str,help='指定需要重跑的日期，默认为今天，格式yyyy-MM-dd')
    parser.add_argument('-t', '--type', type=str, default="restart",help='重跑类型 \
                       1.restart: 默认值。只重跑自己和自己下游失败的dag  \
                       2.rerun: 重跑自己和下游所有的dag, 不管是否失败')
    parser.add_argument('-i', '--interaction', type=bool, default=0, help='默认非交互式。数值0或者1，是否交互式运行；交互式运行可以选择需要运行的dag')
    
    fetch_deps_data()
    # 解析命令行参数
    args = parser.parse_args()
    if  (args.dag_id):
        dag_id = args.dag_id
        rdate = args.date
        rerun_type = args.type
        is_interaction = args.interaction
        if not is_interaction:
            logger.info(f" 清理给定的dag_id： {dag_id} 及其下游 ，在日期 {rdate} 的状态")
            clear_dag(dag_id,rdate)
            logger.info(f" 判断{dag_id}是否有下游任务，如果有，则依次触发下游dag")
            trigger_downstream_dag(dag_id=dag_id,rdate=rdate)
        else: # 交互式
            logger.info(f" 清理给定的dag_id： {dag_id} 及所选择的下游 dag  列表，在日期 {rdate} 的状态")
            downstream_dag_deps_path = get_dags_list(dag_id=dag_id)
            selected_dags_list = select_dag(downstream_dag_deps_path)
            logger.warning(f'开始clear {selected_dags_list} 状态')
            for dag in selected_dags_list:
                clear_dag(dag_id=dag, rdate=rdate)
    else:
        #"参数为空，则打印帮助文档"
        parser.print_help()
