### 监控Azkaban调度任务，有失败则钉钉告警

通过Azkaban数据库难以知道明天将要执行的调度任务，可用爬虫直接爬取调度页面获得

*.log：日志文件

next_day_flows：存放明天将要执行的任务（pickle格式）

可通过crontab定时任务来执行脚本：
```shell
55 23 * * * /usr/bin/python3 /home/admin/azkaban-monitor/get_next_day_flows.py

2 8 * * * /usr/bin/python3 /home/admin/azkaban-monitor/get_current_time_history_and_send.py
```