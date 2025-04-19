###  pymysql 实现版

#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-09-12

import datetime
import pymysql
import requests
import pytz


# Telegram Bot 配置
TELEGRAM_BOT_TOKEN = '' # 替换为实际的 Bot Token
CHAT_ID = '' # 替换为实际的 Chat ID


# 将消息分割成较小的部分
def split_message(message, max_length=4096):
    return [message[i:i + max_length] for i in range(0, len(message), max_length)]


# 发送 Telegram 消息
def send_telegram_message(message):
    if not message:
        return  # 不发送空消息

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': CHAT_ID,
        'text': message
    }

    messages = split_message(message)
    for msg in messages:
        payload['text'] = msg
        response = requests.post(url, data=payload)
        if response.status_code != 200:
            print(f"Failed to send message to Telegram: {response.status_code}, {response.text}")


# 执行 SQL 查询并返回结果
def run_query(connection, query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall(), cursor._last_executed


def manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic):
    """
    管理数据库分区的函数。

    参数:
    db_host (str): 数据库主机地址
    db_user (str): 数据库用户名
    db_pwd (str): 数据库密码
    db_list (list): 库列表
    table_list (list): 分区表列表
    """

    # 定义当前时间
    tz = pytz.timezone('Europe/London') # 设置时区为英国伦敦
    current_date = datetime.datetime.now(tz)

    # 告警消息队列
    error_messages = []
    add_messages = []
    delete_messages = []
    does_not_exist_messages = []
    already_messages = []

    # 打开数据库连接
    connection = pymysql.connect(host=db_host, user=db_user, password=db_pwd)

    def del_partitions(count_num, db_list, table_list):
        # 30天前的时间
        last_30_days = current_date - datetime.timedelta(days=self.del_day + count_num)
        # 设置时间为0点
        last_target_datetime = last_30_days.replace(hour=0, minute=0, second=0, microsecond=0)
        # 转成毫秒
        last_30_days_timestamp = int(last_target_datetime.timestamp() * 1000)
        # 分别提取 last_30_days 的年份、月份和日期，并将它们转换为字符串。如果月份或日期是单数字，则用 zfill(2) 填充前导零以确保两位数格式。
        year_str_last = str(last_30_days.year)
        month_str_last = str(last_30_days.month).zfill(2)
        day_str_last = str(last_30_days.day).zfill(2)

        # 拼接前缀“p”
        date_str_last = "p" + year_str_last + month_str_last + day_str_last

        for dbs in db_list:
            for tbs in table_list:
                # 检查是否存在要删除的分区
                check_drop_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_last}'"
                try:
                    result_drop_exists, _ = run_query(connection, check_drop_exists)
                    if result_drop_exists:
                        # 删除30天前的分区
                        sql_drop = f'ALTER TABLE {dbs}.{tbs} DROP PARTITION {date_str_last}'
                        try:
                            run_query(connection, sql_drop)
                            delete_messages.append(f"Deleted partition {date_str_last} for table {dbs}.{tbs}.")
                        except pymysql.MySQLError as e:
                            error_messages.append(
                                f"Error deleting partition {date_str_last} for table {dbs}.{tbs}: {e}")
                    else:
                        does_not_exist_messages.append(
                            f"Partition {date_str_last} does not exist for table {dbs}.{tbs}, skipping deletion")
                except pymysql.MySQLError as e:
                    error_messages.append(f"Error executing query: {e}")

    def add_partitions(count_num, db_list, table_list):
        # 下周时间
        next_week = current_date + datetime.timedelta(days=self.add_day + (count_num * self.interval_days))
        # 设置时间为0点
        next_target_datetime = next_week.replace(hour=0, minute=0, second=0, microsecond=0)
        # 转成毫秒
        next_week_timestamp = int(next_target_datetime.timestamp() * 1000)
        # 分别提取 next_week 的年份、月份和日期，并将它们转换为字符串。如果月份或日期是单数字，则用 zfill(2) 填充前导零以确保两位数格式。
        year_str_next = str(next_week.year)
        month_str_next = str(next_week.month).zfill(2)
        day_str_next = str(next_week.day).zfill(2)

        # 拼接前缀“p”
        date_str_next = "p" + year_str_next + month_str_next + day_str_next

        for dbs in db_list:
            for tbs in table_list:
                # 检查是否存在要添加的分区
                check_add_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND partition_name = '{date_str_next}'"
                result_add_exists, _ = run_query(connection, check_add_exists)
                try:
                    if not result_add_exists:
                        # 添加七天后的分区
                        sql_add = f'ALTER TABLE {dbs}.{tbs} ADD PARTITION (partition {date_str_next} values less than ({next_week_timestamp}))'
                        try:
                            run_query(connection, sql_add)
                            add_messages.append(f"Added partition {date_str_next} for table {dbs}.{tbs}.")
                        except pymysql.MySQLError as e:
                            error_messages.append(
                                f"Error adding partition {date_str_next} for table {dbs}.{tbs}: {e}")
                    else:
                        already_messages.append(
                            f"Partition {date_str_next} already exists for table {dbs}.{tbs}, skipping addition")
                except pymysql.MySQLError as e:
                    error_messages.append(f"Error executing query: {e}")


    # 执行分区管理
    try:
        for count_num in range(8): # 7天
            del_partitions(count_num, db_list, table_list)
        for count_num in range(8): # 7天
            add_partitions(count_num, db_list, table_list)
    except Exception as e:
        send_telegram_message(f"分区脚本执行报错: {e}")
    finally:
        # 关闭数据库连接
        connection.close()

    # 发送所有消息到 Telegram
    full_error_messages = "\n".join(error_messages)
    full_add_messages = "\n".join(add_messages)
    full_delete_message = "\n".join(delete_messages)
    full_does_not_exist_message = "\n".join(does_not_exist_messages)
    full_already_messages = "\n".join(already_messages)
    send_telegram_message(full_error_messages)
    #send_telegram_message(full_add_messages)
    #send_telegram_message(full_delete_message)
    #send_telegram_message(full_does_not_exist_message)
    #send_telegram_message(full_already_messages)

    # 标注环境实例
    send_telegram_message(topic)




# 使用示例

# 分区表列表
table_list = [
    'tab_financialelectronic',
    'tab_financialelectronic_jili',
    'tab_financialelectronic_pg',
    'tab_financialelectronic_pp',
    'tab_financialelectronic_spribe',
    'tab_financialelectronic_tb',
    'tab_orderelectronic',
    'tab_orderelectronic_jili',
    'tab_orderelectronic_pg',
    'tab_orderelectronic_pp',
    'tab_orderelectronic_spribe',
    'tab_orderelectronic_tb',
    'tab_ordermakeup',
    'tab_financiallottery_5d',
    'tab_financiallottery_k3',
    'tab_financiallottery_trxwingo',
    'tab_financiallottery_wingo',
    'tab_orderlottery_5d',
    'tab_orderlottery_k3',
    'tab_orderlottery_trxwingo',
    'tab_orderlottery_wingo'
]



# 印度1
topic = f"生产(印度1)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs5ig8zqt3bni2e18.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "HBho3ePONjjPn9H3CXWi"
db_list = [

    'tenant_1030',
    'tenant_1017',
    'tenant_1006',
    'tenant_1008',
    'tenant_1027',
    'tenant_1029'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)



# 南美
topic = f"生产(南美)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs5s8dc1712fyb97x.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "sN3DcLa3MDVW3Y2g9WDA"
db_list = [
    'tenant_1002',
    'tenant_1003',
    'tenant_1019',
    'tenant_1038',
    'tenant_1040'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)



# 东南亚
topic = f"生产(东南亚)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs565fd2268h22h9u.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "ZtagW7NTd3NZYaLFfMbZ"
db_list = [
    'tenant_1046',
    'tenant_1034',
    'tenant_1043',
    'tenant_1035',
    'tenant_1042',
    'tenant_1052',
    'tenant_1045',
    'tenant_1026',
    'tenant_1044',
    'tenant_1033',
    'tenant_1036',
    'tenant_1039'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)



# 印度2
topic = f"生产(印度2)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs59qv41fbw5pg2m8.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "Yn3z0oQW6mjHWNkbto4j"
db_list = [
    'tenant_1012',
    'tenant_1025',
    'tenant_1016',
    'tenant_1063',
    'tenant_1064',
    'tenant_1065',
    'tenant_1053',
    'tenant_1047',
    'tenant_1037',
    'tenant_1041',
    'tenant_1021',
    'tenant_1023',
    'tenant_1007'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)



# 印度3
topic = f"生产(印度3)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs5waf5yi191sz074.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "QDGF30HrbXmq3U8vijiq"
db_list = [
    'tenant_1066',
    'tenant_1011',
    'tenant_1018',
    'tenant_1059',
    'tenant_1001',
    'tenant_1031',
    'tenant_1062',
    'tenant_1014',
    'tenant_1058',
    'tenant_1022'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)



# 非洲
topic = f"生产(非洲)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs58ci455v130yx5a.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "ce8Q3pZi6igaFdqz87mX"
db_list = [
    'tenant_1004',
    'tenant_1009',
    'tenant_1020'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)




# 演示站
topic = f"生产(演示站)--saas系统分区调整情况如上,如无内容则表示无需调整"
db_host = "pc-gs53gs39aanc9287f.rwlb.singapore.rds.aliyuncs.com"
db_user = "polar_root"
db_pwd = "D@2QFL73vXTey3oqsf05K"
db_list = [
    'tenant_9900',
    'tenant_9901',
    'tenant_9902',
    'tenant_9903',
    'tenant_9904',
    'tenant_9905',
    'tenant_9906',
    'tenant_9907',
    'tenant_9908',
    'tenant_9909',
    'tenant_9910'
]

manage_db_partitions(db_host, db_user, db_pwd, db_list, table_list, topic)
