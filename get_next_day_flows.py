import os
import logging
import time
import datetime
import pickle

import requests
from pyquery import PyQuery as pq

root_abs_path = os.path.dirname(os.path.abspath(__file__))

logging.basicConfig(level=logging.DEBUG,
                    filename=os.path.join(root_abs_path, 'get_next_day_flows.log'),
                    filemode='a+',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')

requests.packages.urllib3.disable_warnings()


def main():
    # 需要修改
    login_url = 'https://host:port/'  # host: Azkaban所在服务器
    schedule_url = 'https://host:port/schedule'  # Azkaban的调度页面，需爬取；host: Azkaban所在服务器

    session = requests.session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36'
    }

    try:
        # 需要修改
        post_data = {'action': 'login', 'username': 'azkaban', 'password': 'azkaban'}  # Azkaban登录界面账号密码

        response = session.post(login_url, headers=headers, data=post_data, verify=False)
        if response.status_code == 200:
            try:
                response = session.get(schedule_url, headers=headers, verify=False)
                if response.status_code == 200:
                    current_day = time.strftime("%Y-%m-%d", time.localtime())
                    next_day = \
                        str(datetime.datetime.strptime(current_day, '%Y-%m-%d') + datetime.timedelta(days=1)).split()[0]

                    next_day_flows_dict = {}
                    doc = pq(response.text)
                    trs = doc('tr').items()
                    for tr in trs:
                        flow = tr.find('td:nth-child(3)').text()
                        next_execution_time = tr.find('td:nth-child(7)').text()
                        if next_execution_time.startswith(next_day):
                            next_day_flows_dict[flow] = next_execution_time

                    if len(next_day_flows_dict) == 0:
                        logging.error('not crawl the scheduling page !')
                    else:
                        for k, v in next_day_flows_dict.items():
                            logging.info('flow: ' + k + ', execution time: ' + v)
                        with open(os.path.join(root_abs_path, 'next_day_flows/{}.pkl'.format(next_day)), 'wb') as f:
                            pickle.dump(next_day_flows_dict, f)
                        logging.info('Crawling Success')
                else:
                    logging.error('https GET status code {}'.format(response.status_code))
            except Exception as e:
                logging.critical('GET {}'.format(e))
        else:
            logging.error('https POST status code {}'.format(response.status_code))
    except Exception as e:
        logging.critical('POST {}'.format(e))

    session.close()

    logging.info('\n')


if __name__ == '__main__':
    main()
