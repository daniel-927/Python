# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-09-07
import pymysql
import pysnooper

# 数据库连接配置信息
conn1 = pymysql.connect(
    host="filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
    user="admin",
    password="WATNfBJYaZ4FPVVzdYCq",
    database="filbet_dev_main"
)


#
# @pysnooper.snoop()
def dump_user():
    try:
        # 打开游标
        cursor1 = conn1.cursor()

        # 获取代理
        sql1 = f'select id from win_user where role=1;'
        agents_sql = cursor1
        agents_sql.execute(sql1)
        agents_result = agents_sql.fetchall()
        nested_list = []

        # 获取代理下的会员
        for agent_result in agents_result:
            sql2 = 'select id  from win_user where sup_uid_1="{}" AND role=0;'.format(agent_result[0])
            sql3 = f'select id  from win_user where sup_uid_1="{agent_result[0]}" AND role=0;'
            print(sql2)
            print(sql3)
            members_sql = cursor1
            members_sql.execute(sql2)
            members_result = members_sql.fetchall()
            for member_result in members_result:
                nested_list.append(member_result[0])
                #sql3 = f'insert into test_1208 (member_id, agent_id) values (%s, %s)'
                #sql_value = (member_result[0], agent_result[0])
                #cursor1.execute(sql3, sql_value)
                #conn1.commit()
        print(len(nested_list))

    except Exception as e:
        print(f"Error: {e}")

# 开始导出
dump_user()

# 关闭数据库连接
conn1.close()
