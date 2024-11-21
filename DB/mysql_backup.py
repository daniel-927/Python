# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-05-30

import datetime
import subprocess
import boto3
import requests
import socket
import time

start_time = time.time()


def run_command(command):
    """运行系统命令并返回输出结果"""
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = process.communicate()
    return output, error


# 定义cnf文件路径
cnf_file = "/etc/percona-server.conf.d/mysqld.cnf"
file_path = '/root/dba/backup/full_backup_record.log'
full_backup_file = '/root/dba/backup/full_backup.log'
backup_dir = '/root/dba/backup/'
# 获取当前日期
current_date = datetime.datetime.now()


def perform_full_backup(target_dir, full_time_backup_file, full_backup_dir):
    """执行全量备份"""
    full_backup_command = f"xtrabackup --defaults-file={cnf_file} --login-path=root --backup --target-dir={target_dir}  && echo {full_backup_dir} >> {full_backup_file}  && echo {full_time_backup_file}.tar.gz >> /root/dba/backup/full_backup_record.log && cd {backup_dir} && tar zcvf {full_time_backup_file}.tar.gz {full_time_backup_file}"
    output, error = run_command(full_backup_command)
    if error:
        print(f"全量备份时出错：{error.decode('utf-8')}")
    else:
        print("全量备份完成。")


def perform_incremental_backup(full_backup_dir, incremental_file):
    """执行增量备份"""
    incremental_command = f"xtrabackup --defaults-file={cnf_file} --login-path=root  --backup --target-dir={incremental_dir} --incremental-basedir={full_backup_dir} && echo {inc_time_backup_file}.tar.gz >> /root/dba/backup/inc_backup_record.log && cd {backup_dir} && tar zcvf {inc_time_backup_file}.tar.gz {inc_time_backup_file}"
    output, error = run_command(incremental_command)
    if error:
        print(f"增量备份时出错：{error.decode('utf-8')}")
    else:
        print("增量备份完成。")


# 获取最新备份名
def get_latest_line(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        if lines:
            latest_line = lines[-1].strip()
            return latest_line
        else:
            return None


# 获取主机名和IP
host_name = socket.gethostname()
ip_address = socket.gethostbyname(host_name)

# s3存储桶
bucket_name = 'db-backup12345'
s3_directory = 'event-db/datafile/'

# telegram机器人
TELEGRAM_BOT_TOKEN = '5249320515:AAHibqLVtW69J6_OyJi1amDwXO1HfVTr3iw'
TELEGRAM_CHAT_ID = '-626363552'


# 上传s3
def upload_file_to_s3(file_path, bucket_name, object_name):
    s3_client = boto3.client('s3')
    exection_time = time.time() - start_time
    object_name = f'{s3_directory}{object_name}'
    msg = f"主机名: {host_name}\n"
    msg += f"主机IP: {ip_address}\n"
    msg += f"文件名: {object_name}\n"
    msg += f"耗时: {exection_time}\n"
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        del_local_backup = "find /root/dba/backup/*  -type f  -mtime +5  -exec rm -f {} \;"
        msg += f"备份上传成功"
        send_telegram_message(msg)
        subprocess.run(del_local_backup, shell=True, check=True)
    except Exception as e:
        msg += f"备份上传失败"
        send_telegram_message(msg)


# 传输消息
def send_telegram_message(message):
    telegram_api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    response = requests.post(telegram_api_url, json=payload)
    if response.status_code != 200:
        print(f'发送 Telegram 消息失败:{response.text}')


s3_client = boto3.client('s3')

# 判断当前日期执行全量备份还是增量备份
if current_date.weekday() in [6, 2]:  # 周日或周三执行全量备份
    full_time_backup_file = f"full_{current_date.strftime('%Y%m%d%H%M%S')}"  # 备份文件名
    full_backup_dir = f"{backup_dir}full_{current_date.strftime('%Y%m%d%H%M%S')}"  # 备份文件目录
    perform_full_backup(full_backup_dir, full_time_backup_file, full_backup_dir)  # 执行备份
    s3_file_path = full_backup_dir + '.tar.gz'  # 定义压缩备份名
    object_name = full_time_backup_file + '.tar.gz'  # 定义上传目标文件名
    upload_file_to_s3(s3_file_path, bucket_name, object_name)  # 执行上传备份至s3
    s3_client.upload_file('/root/dba/backup/full_backup_record.log', bucket_name,
                          s3_directory + 'full_backup_record.log')  # 上传备份日志至s3
else:  # 周二、周四、周五、周六执行增量备份
    file_path = '/root/dba/backup/full_backup.log'
    latest_line = get_latest_line(file_path)
    latest_full_backup_dir = latest_line  # 替换为最新的全量备份目录
    inc_time_backup_file = f"inc_{current_date.strftime('%Y%m%d%H%M%S')}"  # 备份文件名
    incremental_dir = f"{backup_dir}inc_{current_date.strftime('%Y%m%d%H%M%S')}"  # 备份文件目录
    perform_incremental_backup(latest_full_backup_dir, incremental_dir)  # 执行备份
    s3_file_path = incremental_dir + '.tar.gz'  # 定义压缩备份名
    object_name = inc_time_backup_file + '.tar.gz'  # 定义上传目标文件名
    upload_file_to_s3(s3_file_path, bucket_name, object_name)  # 执行上传备份至s3
    s3_client.upload_file('/root/dba/backup/inc_backup_record.log', bucket_name,
                          s3_directory + 'inc_backup_record.log')  # 上传备份日志至s3
