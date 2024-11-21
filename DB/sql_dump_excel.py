# /usr/bin/python3

# Author        : Daniel
# Date          : 2024-01-04
import pandas as pd
import pymysql
import pysnooper

db_config = {
    "host": "filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
    "user": "admin",
    "password": "WATNfBJYaZ4FPVVzdYCq",
    "database": "filbet_dev_main"
}


@pysnooper.snoop()
def select_excel():
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()
    # 执行查询语句
    show_databases = "show databases;"
    cursor.execute(show_databases)
    result = cursor.fetchall()
    skip_databases = {"information_schema", "mysql", "sys", "performance_schema"}
    for i in result:
        if i[0] in skip_databases:
            pass
        else:
            sql = f'SELECT * FROM {i[0]}.win_user;'
            # sql2 = f'SELECT     u.username as 用户名,    ui.mobile as 手机号,    ui.qq as QQ,    ui.email as 邮箱,    ub.bank_name as 银行名称,    ub.bank_account as 银行卡号,    ub.bank_address as 支行名称 FROM     {i[0]}.users u LEFT JOIN     {i[0]}.users_info ui ON u.id = ui.uid LEFT JOIN     {i[0]}.users_bank ub ON u.id = ub.uid;'
            df = pd.read_sql(sql, connection)

            # 将数据导出为Excel文件
            excel_filename = f'/tmp/{i[0]}.xlsx'
            df.to_excel(excel_filename, index=False)
            print(f'Data exported to {excel_filename}')
    # 关闭数据库连接
    cursor.close()
    connection.close()


# 开始
select_excel()
