import os
import time
import pickle
import json
import logging

import pymysql
import pandas as pd
import requests

root_abs_path = os.path.dirname(os.path.abspath(__file__))

azkaban_status_codes = {10: 'Ready',
                        20: 'Preparing',
                        30: 'Running',
                        40: 'Paused',
                        50: 'Succeed',
                        55: 'Killing',
                        60: 'Killed',
                        70: 'Failed',
                        80: 'Failed Finishing',
                        90: 'Skipped',
                        100: 'Disabled',
                        110: 'Queued',
                        120: 'Failed, treated as success',
                        125: 'Cancelled'}


def transform_date(timestamp):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp * 0.001))


def get_current_time_history():
    # 需要修改
    conn = pymysql.connect(host="host", user="azkaban", password="azkaban", database="azkaban")  # host: Azkaban数据库所在IP

    df = pd.read_sql('select * from execution_flows', conn)
    current_day = time.strftime("%Y-%m-%d", time.localtime()) + ' 00:00:00'
    time_array = time.strptime(current_day, "%Y-%m-%d %H:%M:%S")
    timestamp = time.mktime(time_array)

    df = df[df['start_time'].apply(lambda x: x * 0.001) >= float(timestamp)]
    df = df.groupby('flow_id').apply(lambda sub_df: sub_df.sort_values('start_time')[-1:]).reset_index(drop=True)
    # df['start_time'] = df['start_time'].apply(transform_date)
    # df['end_time'] = df['end_time'].apply(transform_date)

    current_time_succeed = []
    running = []
    current_time_no_succeed = []
    for idx, row in df.iterrows():
        tmp_flow = {'flow_id': row['flow_id']}
        if row['status'] == 50:
            current_time_succeed.append(tmp_flow)
        elif row['status'] == 30:
            running.append(tmp_flow)
        else:
            current_time_no_succeed.append(tmp_flow)

    conn.close()

    return current_time_succeed, running, current_time_no_succeed


def send_dingding(current_time_succeed, running, current_time_no_succeed):
    current_day = time.strftime("%Y-%m-%d", time.localtime())

    pkl_path = os.path.join(root_abs_path, 'next_day_flows/{}.pkl'.format(current_day))
    if os.path.exists(pkl_path):
        with open(pkl_path, 'rb') as f:
            next_day_flows_dict = pickle.load(f)
        sub_send2 = '应加载：{}个'.format(len(next_day_flows_dict))
    else:
        sub_send2 = '未成功爬取调度页面'

    webhook = '钉钉报警群机器人webhook'  # 真实
    # webhook = '钉钉报警测试群机器人webhook'  # 测试

    headers = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }

    # 需要修改
    sub_send1 = '【标题】{}'.format(current_day)

    if len(running) == 0:
        sub_send3 = '成功{}个，非成功{}个\n'.format(len(current_time_succeed), len(current_time_no_succeed))
    else:
        sub_send3 = '成功{}个，执行中{}个，非成功{}个\n'.format(len(current_time_succeed), len(running),
                                                   len(current_time_no_succeed))
    if len(current_time_no_succeed) > 0:
        sub_send3 += '非成功作业名：\n' + '\n'.join([i['flow_id'] for i in current_time_no_succeed]) + '\n请及时处理~'

    send = '\n'.join([sub_send1, sub_send2, sub_send3])
    message = {
        "msgtype": "text",
        "text": {
            "content": send
        },
        "at": {
            "atMobiles": [],
            "isAtAll": False
        }
    }
    message_json = json.dumps(message)

    # 失败重试3次，有更优雅写法
    try:
        response = requests.post(url=webhook, headers=headers, data=message_json)
        logging.debug(response.text)
    except Exception as e:
        logging.critical('POST {}'.format(e))
        time.sleep(300)
        try:
            response = requests.post(url=webhook, headers=headers, data=message_json)
            logging.debug(response.text)
        except Exception as e:
            logging.critical('POST {}'.format(e))
            time.sleep(300)
            try:
                response = requests.post(url=webhook, headers=headers, data=message_json)
                logging.debug(response.text)
            except Exception as e:
                logging.critical('POST {}'.format(e))

    logging.info('\n')


def main():
    fa = open(os.path.join(root_abs_path, 'get_current_time_history_and_send.log'), mode='a+', encoding='utf8')
    logging.basicConfig(stream=fa,
                        format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                        level=logging.DEBUG, )

    current_time_succeed, running, current_time_no_succeed = get_current_time_history()

    if len(current_time_no_succeed) > 0:  # 有失败作业即告警
        send_dingding(current_time_succeed, running, current_time_no_succeed)
    else:
        logging.debug(
            '成功{}个，执行中{}个，非成功{}个\n\n'.format(len(current_time_succeed), len(running), len(current_time_no_succeed)))

    fa.close()


if __name__ == '__main__':
    main()
