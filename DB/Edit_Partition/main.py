#!/usr/bin/python3

# Author        : Ives
# Date          : 2024-10-23


from saas_pro_add_partition3 import DBPartitionManager
import pymysql

def main():

    # telegram 配置
    bot_token = " "    # 替换为实际的 Bot Token
    chat_id = " "      # 替换为实际的 Chat ID


### 维护按天的分区维度
    # 分区管理参数
    add_day  = 7       # 添加7天后的分区
    del_day  = 10      # 删除10天前的分区
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






    # 实例
    topic = f"《按天维度》saas分区调整结束，情况如上。（正常情况下只有调整站点列表）"
    db_host = "pxc-spravjm833psf7-pub.polarx.singapore.rds.aliyuncs.com"
    db_user = "ives"
    db_pwd = "Cssl#123123"
    db_list = [
        'artenant'
    ]



    # # 调用分区管理函数 一键删除和新增分区
    # notifier.manage_db_partitions(db_host, db_user, db_pwd, db_list, topic)








    # 单独执行分区管理（可分别执行删除和新增）
    # 打开数据库连接
    connection = pymysql.connect(host=db_host, user=db_user, password=db_pwd)
     
    try:
        for count_num in range(edit_num):  # 操作分区个数

            # 单独调用删除分区函数
            notifier.del_partitions(connection, count_num, db_list, topic)

            # 单独调用添加分区函数
            notifier.add_partitions(connection, count_num, db_list, topic)    


        # 操作完成后，统一发送所有消息
        topic = f"{topic}\n{db_list}"
        notifier.send_all_messages(topic)
            
        
    except Exception as e:
        notifier.send_telegram_message(f"分区脚本执行报错: {e}")
    finally:
        # 关闭数据库连接
        connection.close()








if __name__ == "__main__":
    main()
