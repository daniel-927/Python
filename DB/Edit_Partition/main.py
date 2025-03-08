#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23


from saas_pro_add_partition2 import DBPartitionManager

def main():

    # telegram 配置
    bot_token = "6327237666:AAEeH1FVThAdnBeYGBkpfWG7HfLy4Jzl_8w"  # 替换为实际的 Bot Token
    chat_id = "-4578699157"      # 替换为实际的 Chat ID


### 维护按天的分区维度
    # 分区管理参数
    add_day  = 7       # 添加7天后的分区
    del_day  = 45      # 删除45天前的分区
    edit_num = 8       # 添加或删除分区个数 8等于7个
    interval_days = 1  # 间隔天数

    # 分区表列表
    table_list = [
        'tab_user',
        'tab_group'
    ]

    # 实例化，并传递公共参数
    notifier = DBPartitionManager(bot_token, chat_id, add_day, del_day, edit_num, interval_days, table_list)




   

    # 演示站
    topic = f"《按天维度》生产(演示站)--saas分区调整结束，情况如上。（正常情况下只有调整站点列表）"
    db_host = "pc-gs53gs39aanc9287f.rwlb.singapore.rds.aliyuncs.com"
    db_user = "polar_root"
    db_pwd = "D@2QFL73vXTey3oqsf05K"
    db_list = [
        'db1',
        'db2'
    ]

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, topic)









if __name__ == "__main__":
    main()
