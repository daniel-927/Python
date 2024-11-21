#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23


from saas_pro_add_partition2 import DBPartitionManager

def main():

    # telegram 配置
    bot_token = "6327237666:AAEeH1FVThAdnBeYGBkpfWG7HfLy4Jzl_8w"  # 替换为实际的 Bot Token
    chat_id = "-4578699157"      # 替换为实际的 Chat ID

    # 分区管理参数
    add_day  = 7       # 添加7天后的分区
    del_day  = 30      # 删除30天前的分区
    edit_num = 8       # 添加或删除分区个数 8等于7个
    interval_days = 1  # 间隔天数

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

    # 实例化，并传递公共参数
    notifier = DBPartitionManager(bot_token, chat_id, add_day, del_day, edit_num, interval_days, table_list)










    # 演示站
    topic = f"生产(演示站)--saas系统分区调整结束，情况如上。（如无内容则表示调整正常完成）"
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

    notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, topic)







if __name__ == "__main__":
    main()
