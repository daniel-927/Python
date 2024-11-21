# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-05-30

import subprocess
import boto3
import socket
import requests

# 获取主机名和IP
host_name = socket.gethostname()
ip_address = socket.gethostbyname(host_name)

# telegram机器人
TELEGRAM_BOT_TOKEN = '5249320515:AAHibqLVtW69J6_OyJi1amDwXO1HfVTr3iw'
TELEGRAM_CHAT_ID = '-626363552'


# 传输消息
def send_telegram_message(message):
    telegram_api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    response = requests.post(telegram_api_url, json=payload)
    if response.status_code != 200:
        print(f'发送Telegram 消息失败:{response.text}')


# 定义下载获取备份记录日志文件
def download_s3_file(bucket_name, object_name, file_name):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, object_name, file_name)
        print("下载成功")
    except Exception as e:
        print(f"下载失败{object_name} 失败：{e}")


# s3存储桶配置
bucket_name = 'db-backup12345'
s3_directory = 'event-db/datafile/'
full_name = s3_directory + 'full_backup_record.log'
inc_name = s3_directory + 'inc_backup_record.log'
download_s3_file(bucket_name, full_name, '/root/backup/full_backup_record.log')
download_s3_file(bucket_name, inc_name, '/root/backup/inc_backup_record.log')


def restore_backup(incremental_backups, target_directory):
    try:
        # 使用基本备份进行恢复
        restore_command = f"xtrabackup --prepare --apply-log-only --target-dir={target_directory}"
        subprocess.run(restore_command, shell=True, check=True)

        # 使用增量备份应用到基本备份
        for incremental_backup in incremental_backups:
            restore_command = f"xtrabackup --prepare --apply-log-only --target-dir={target_directory} --incremental-dir={incremental_backup}"
            subprocess.run(restore_command, shell=True, check=True)

        # 使用基本备份进行最终恢复
        restore_command = f"xtrabackup --prepare --target-dir={target_directory}"
        subprocess.run(restore_command, shell=True, check=True)

        # 停止MySQL服务器
        subprocess.run("systemctl stop mysqld", shell=True, check=True)

        # 删除原始数据目录
        subprocess.run(f"rm -rf /data/mysql/*", shell=True, check=True)

        # 将恢复的数据目录移回原始位置
        subprocess.run(f"mv {target_directory}/* /data/mysql", shell=True, check=True)

        # 设置正确的权限和所有者
        subprocess.run("chown -R mysql:mysql /data/mysql", shell=True, check=True)

        # 启动MySQL服务器
        subprocess.run("systemctl start mysqld", shell=True, check=True)

        msg = f"主机名: {host_name}\n"
        msg += f"主机IP: {ip_address}\n"
        msg += f"恢复成功"
        send_telegram_message(msg)
    except Exception as e:
        print(f"备份恢复失败：{str(e)}")


# 定义获取最新备份方法
def get_latest_line(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
        if lines:
            latest_line = lines[-1].strip()
            return latest_line
        else:
            return None


# 指定备份文件和目标目录
incremental_backups = []  # 定义增量备份目录，默认为空则不执行增量恢复
target_directory = '/root/full_backup'  # 恢复目标目录

# 获取最新备份
full_backup_name = s3_directory + get_latest_line('/root/backup/full_backup_record.log')
inc_backup_name = s3_directory + get_latest_line('/root/backup/inc_backup_record.log')
local_full_backup_name = '/root/backup/' + get_latest_line('/root/backup/full_backup_record.log')
local_inc_backup_name = '/root/backup/' + get_latest_line('/root/backup/full_backup_record.log')

# 下载全量备份进行恢复
download_s3_file(bucket_name, full_backup_name, local_full_backup_name)
tar_cmd = f"rm -rf /root/full_backup/*  &&  tar zxvf {local_full_backup_name} -C /root/full_backup --strip-components 1   &> /dev/null"
subprocess.run(tar_cmd, shell=True, check=True)

# 下载判断增量备份是否基于最新全量备份
download_s3_file(bucket_name, inc_backup_name, local_inc_backup_name)
if full_backup_name[5:-7] < inc_backup_name[4:-7]:
    tar_cmd = f"rm -rf /root/inc_backup/*   &&  tar zxvf {local_inc_backup_name} -C /root/inc_backup --strip-components 1   &> /dev/null"
    subprocess.run(tar_cmd, shell=True, check=True)
    incremental_backups.append('/root/inc_backup')
# 执行备份恢复
restore_backup(incremental_backups, target_directory)
