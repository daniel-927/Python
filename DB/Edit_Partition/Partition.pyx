#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23

import datetime
import pymysql
import requests
import pytz
import time
import logging
import os

class DBPartitionManager:
    def __init__(self, bot_token, chat_id, add_day, del_day, edit_num, interval_days, table_list):
        self.bot_token = bot_token # 消息机器人token
        self.chat_id = chat_id     # 消息机器人chat_id
        self.add_day = int(add_day)     # 添加具体多少天后分区的时间
        self.del_day = int(del_day)     # 删除多少天前分区的时间
        self.edit_num = edit_num   # 调整多少个分区
        self.interval_days = interval_days  # 分区间隔时间
        self.table_list = table_list # 分区表列表
        # 初始化消息队列
        self.error_messages = []
        self.add_messages = []
        self.delete_messages = []
        self.does_not_exist_messages = []
        self.already_messages = []
        
        # 设置日志
        self._setup_logging()
        self.logger.info(f"DBPartitionManager initialized with parameters: add_day={add_day}, del_day={del_day}, edit_num={edit_num}, interval_days={interval_days}, table_list={table_list}")

    def _setup_logging(self):
        """设置日志配置"""
        # 创建日志目录
        log_dir = "/var/log/db_partition"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # 创建logger
        self.logger = logging.getLogger('DBPartitionManager')
        self.logger.setLevel(logging.INFO)
        
        # 创建文件处理器
        log_file = os.path.join(log_dir, f'db_partition_{datetime.datetime.now().strftime("%Y%m%d")}.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # 创建格式器
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加处理器到logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def send_telegram_message(self, message):
        if not message:
            self.logger.debug("Skipping empty message")
            return  # 不发送空消息

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {'chat_id': self.chat_id, 'text': message}

        messages = self.split_message(message)
        self.logger.info(f"Sending {len(messages)} message(s) to Telegram")
        
        for idx, msg in enumerate(messages):
            payload['text'] = msg
            try:
                self.logger.debug(f"Sending message part {idx+1}/{len(messages)}")
                response = requests.post(url, data=payload)
                
                if response.status_code == 429:
                    retry_after = response.json().get('parameters', {}).get('retry_after', 30)
                    self.logger.warning(f"Rate limit hit, waiting {retry_after} seconds...")
                    print(f"速率限制，等待 {retry_after} 秒...")
                    time.sleep(retry_after + 1)  # 额外等待1秒，确保安全
                    # 重试发送
                    response = requests.post(url, data=payload)
                
                if response.status_code != 200:
                    self.logger.error(f"Failed to send message to Telegram: {response.status_code}, {response.text}")
                    print(f"Failed to send message to Telegram: {response.status_code}, {response.text}")
                else:
                    self.logger.info(f"Message part {idx+1} sent successfully")
                    
                # 添加一个短暂延迟，避免触发速率限制
                time.sleep(1)
            except Exception as e:
                self.logger.error(f"Error sending message: {e}", exc_info=True)
                print(f"发送消息时出错: {e}")

    def send_all_messages(self, topic=None):
        """发送所有收集的消息"""
        self.logger.info("Preparing to send all collected messages")
        all_messages = []
        
        if topic:
            all_messages.append(f"分区维护完毕: {topic}")
            
        if self.error_messages:
            all_messages.append("错误信息:")
            all_messages.extend(self.error_messages)
            self.logger.warning(f"Found {len(self.error_messages)} error messages")
            
        # if self.add_messages:
        #     all_messages.append("添加分区:")
        #     all_messages.extend(self.add_messages)
        #     self.logger.info(f"Found {len(self.add_messages)} add partition messages")
            
        # if self.delete_messages:
        #     all_messages.append("删除分区:")
        #     all_messages.extend(self.delete_messages)
        #     self.logger.info(f"Found {len(self.delete_messages)} delete partition messages")
            
        # if self.does_not_exist_messages:
        #     all_messages.append("不存在的分区:")
        #     all_messages.extend(self.does_not_exist_messages)
        #     self.logger.info(f"Found {len(self.does_not_exist_messages)} non-existent partition messages")
            
        # if self.already_messages:
        #     all_messages.append("已存在的分区:")
        #     all_messages.extend(self.already_messages)
        #     self.logger.info(f"Found {len(self.already_messages)} already existing partition messages")
            
        if all_messages:
            self.send_telegram_message("\n".join(all_messages))
        else:
            self.logger.info("No messages to send")

    @staticmethod
    def split_message(message, max_length=4096):
        return [message[i:i + max_length] for i in range(0, len(message), max_length)]

    @staticmethod
    def run_query(connection, query, params=None):
        with connection.cursor() as cursor:
            # 尝试用 mogrify 得到最终 SQL（适配有参数的情况）
            try:
                executed_sql = cursor.mogrify(query, params) if params else cursor.mogrify(query)
            except AttributeError:
                executed_sql = query  # mogrify 不存在时直接用原始 SQL

            cursor.execute(query, params or ())
            results = cursor.fetchall()

            # 防止 bytes 类型
            if isinstance(executed_sql, bytes):
                executed_sql = executed_sql.decode()

            return results, executed_sql

    def del_partitions(self, connection, count_num, db_list, topic=None, current_date=None, send_messages=True):
        """
        删除指定天数前的分区
        
        参数:
        connection: 数据库连接
        count_num: 计数器，用于调整删除的日期
        db_list: 数据库列表
        topic: 主题信息，用于发送消息
        current_date: 当前日期，如果为None则使用当前时间
        send_messages: 是否立即发送消息，默认为True
        """
        self.logger.info(f"Starting partition deletion process for count_num={count_num}, db_list={db_list}")
        
        if current_date is None:
            tz = pytz.timezone('Europe/London')  # 设置时区为英国伦敦
            current_date = datetime.datetime.now(tz)
            
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

        # 拼接前缀"p"
        date_str_last = "p" + year_str_last + month_str_last + day_str_last
        
        
        self.logger.info(f"Attempting to delete partition: {date_str_last} (timestamp: {last_30_days_timestamp})")
        
        for dbs in db_list:
            for tbs in self.table_list:
                # 检查是否存在要删除的分区
                check_drop_exists = f"SELECT 1 FROM information_schema.partitions WHERE table_schema = '{dbs}' AND table_name = '{tbs}' AND PARTITION_NAME = '{date_str_last}'"
                try:
                    self.logger.debug(f"Checking partition existence: {dbs}.{tbs}.{date_str_last}")
                    result_drop_exists, _ = self.run_query(connection, check_drop_exists)
                    
                    if result_drop_exists:
                        # 删除30天前的分区
                        sql_drop = f'ALTER TABLE {dbs}.{tbs} DROP PARTITION {date_str_last}'
                        try:
                            self.logger.info(f"Deleting partition {date_str_last} for table {dbs}.{tbs}")
                            self.run_query(connection, sql_drop)
                            msg = f"Deleted partition {date_str_last} for table {dbs}.{tbs}."
                            self.delete_messages.append(msg)
                            self.logger.info(msg)
                        except pymysql.MySQLError as e:
                            error_msg = f"Error deleting partition {date_str_last} for table {dbs}.{tbs}: {e}"
                            self.error_messages.append(error_msg)
                            self.logger.error(error_msg)
                    else:
                        msg = f"Partition {date_str_last} does not exist for table {dbs}.{tbs}, skipping deletion"
                        self.does_not_exist_messages.append(msg)
                        self.logger.info(msg)
                except pymysql.MySQLError as e:
                    error_msg = f"Error executing query: {e}"
                    self.error_messages.append(error_msg)
                    self.logger.error(error_msg, exc_info=True)
                    
            

    def add_partitions(self, connection, count_num, db_list, topic=None, current_date=None, send_messages=True):
        """
        添加指定天数后的分区
        
        参数:
        connection: 数据库连接
        count_num: 计数器，用于调整添加的日期
        db_list: 数据库列表
        topic: 主题信息，用于发送消息
        current_date: 当前日期，如果为None则使用当前时间
        send_messages: 是否立即发送消息，默认为True
        """
        self.logger.info(f"Starting partition addition process for count_num={count_num}, db_list={db_list}")
        
        if current_date is None:
            tz = pytz.timezone('Europe/London')  # 设置时区为英国伦敦
            current_date = datetime.datetime.now(tz)
            
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

        # 拼接前缀"p"
        date_str_next = "p" + year_str_next + month_str_next + day_str_next
        
        
        self.logger.info(f"Attempting to add partition: {date_str_next} (timestamp: {next_week_timestamp})")

        for dbs in db_list:
            for tbs in self.table_list:
                # 检查添加分区是否存在以及是否存在更大的分区
                check_add_exists = f"""
                SELECT 
                    partition_name, 
                    partition_method, 
                    partition_ordinal_position, 
                    partition_description
                FROM information_schema.partitions
                WHERE table_schema = '{dbs}' 
                AND table_name = '{tbs}' 
                AND (PARTITION_NAME = '{date_str_next}' OR PARTITION_DESCRIPTION > '{next_week_timestamp}')
                """
                try:
                    self.logger.debug(f"Checking partition existence: {dbs}.{tbs}.{date_str_next}")
                    result_add_exists, _ = self.run_query(connection, check_add_exists)
                    
                    if not result_add_exists:
                        # 添加七天后的分区
                        sql_add = f'ALTER TABLE {dbs}.{tbs} ADD PARTITION (partition {date_str_next} values less than ({next_week_timestamp}))'
                        try:
                            self.logger.info(f"Adding partition {date_str_next} for table {dbs}.{tbs}")
                            self.run_query(connection, sql_add)
                            msg = f"Added partition {date_str_next} for table {dbs}.{tbs}."
                            self.add_messages.append(msg)
                            self.logger.info(msg)
                        except pymysql.MySQLError as e:
                            error_msg = f"Error adding partition {date_str_next} for table {dbs}.{tbs}: {e}"
                            self.error_messages.append(error_msg)
                            self.logger.error(error_msg)
                    else:
                        msg = f"Partition {date_str_next} already exists for table {dbs}.{tbs}, skipping addition"
                        self.already_messages.append(msg)
                        self.logger.info(msg)
                except pymysql.MySQLError as e:
                    error_msg = f"Error executing query: {e}"
                    self.error_messages.append(error_msg)
                    self.logger.error(error_msg, exc_info=True)
        

    def manage_db_partitions(self, db_host, db_port, db_user, db_pwd, db_list, topic, mode=None):
        """
        管理数据库分区的函数。

        参数:
        db_host (str): 数据库主机地址
        db_user (str): 数据库用户名
        db_pwd (str): 数据库密码
        db_list (list): 库列表
        topic (str): 主题信息，用于发送消息
        mode (str): 控制执行行为，可选值为 'add', 'del', None（默认添加和删除）
        """
        self.logger.info(f"Starting partition management for host: {db_host}, databases: {db_list}, topic: {topic}")

        # 定义当前时间
        tz = pytz.timezone('Europe/London') # 设置时区为英国伦敦
        current_date = datetime.datetime.now(tz)
        self.logger.info(f"Current date (London timezone): {current_date}")

        # 重置消息队列
        self.error_messages = []
        self.add_messages = []
        self.delete_messages = []
        self.does_not_exist_messages = []
        self.already_messages = []

        try:
            # 打开数据库连接
            self.logger.info(f"Connecting to database: {db_host}")
            connection = pymysql.connect(host=db_host, port=db_port, user=db_user, password=db_pwd)
            self.logger.info("Database connection established successfully")
            
            # 执行分区管理
            try:
                for count_num in range(self.edit_num):  # 操作分区个数
                    self.logger.info(f"Processing partition batch {count_num + 1}/{self.edit_num}")
                    # 不立即发送消息，而是积累所有操作消息
                    if mode == 'del':
                        self.del_partitions(connection, count_num, db_list, topic, current_date, send_messages=False)
                    elif mode == 'add':
                        self.add_partitions(connection, count_num, db_list, topic, current_date, send_messages=False)
                    else:
                        self.del_partitions(connection, count_num, db_list, topic, current_date, send_messages=False)
                        self.add_partitions(connection, count_num, db_list, topic, current_date, send_messages=False)
                    
                # 操作完成后，统一发送所有消息
                topic = f"{db_list}\nPartition management summary - Added: {len(self.add_messages)}, Deleted: {len(self.delete_messages)}, Errors: {len(self.error_messages)}"
                #topic = f"{topic}\n{db_list}"
                self.logger.info("All partition operations completed, sending summary messages")
                self.send_all_messages(topic)
                
                # 记录统计信息
                self.logger.info(f"Partition management summary - Added: {len(self.add_messages)}, Deleted: {len(self.delete_messages)}, Errors: {len(self.error_messages)}")
                
            except Exception as e:
                error_msg = f"分区脚本执行报错: {e}"
                self.logger.error(error_msg, exc_info=True)
                self.send_telegram_message(error_msg)
            finally:
                # 关闭数据库连接
                connection.close()
                self.logger.info("Database connection closed")
                
        except Exception as e:
            error_msg = f"Failed to connect to database: {e}"
            self.logger.error(error_msg, exc_info=True)
            self.send_telegram_message(error_msg)
