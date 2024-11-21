# /usr/bin/python3

# Author        : Daniel
# Date          : 2023-09-07
import pymysql
import pysnooper


# 获得表结构
def get_table_structure(cursor, table_name):
    try:
        cursor.execute(f"DESCRIBE {table_name}")
        return cursor.fetchall()
    except pymysql.Error as e:
        print(f"Error: {e}")
        return None


# 对比表结构
def compare_table_structures(cursor1, cursor2, table_name):
    structure1 = get_table_structure(cursor1, table_name)
    structure2 = get_table_structure(cursor2, table_name)

    if structure1 == structure2:
        print(f"Table {table_name} structures are identical.")
    else:
        print(f"\033[91mTable {table_name} structures are different.\033[0m")


# 获取对比库的表数量并进行对比
# @pysnooper.snoop()
def compare_databases_structure(conn1, conn2):
    try:
        # 打开游标
        cursor1 = conn1.cursor()
        cursor2 = conn2.cursor()

        # 获取库名
        cursor1.execute("SELECT DATABASE();")
        database1 = cursor1.fetchall()
        cursor2.execute("SELECT DATABASE();")
        database2 = cursor2.fetchall()

        # 获取库下所有表名
        cursor1.execute("SHOW TABLES")
        tables1 = [table[0] for table in cursor1.fetchall()]

        cursor2.execute("SHOW TABLES")
        tables2 = [table[0] for table in cursor2.fetchall()]

        # 获取差异的数据表
        difference_tables1 = set(tables1) - set(tables2)
        difference_tables2 = set(tables2) - set(tables1)

        print(f"{database1} {difference_tables1} These are extra.")
        print(f"{database2} {difference_tables2} These are extra.")

        # 比对相同表的表结构
        common_tables = set(tables1) & set(tables2)

        for table_name in common_tables:
            compare_table_structures(cursor1, cursor2, table_name)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cursor1.close()
        cursor2.close()


try:
    # 数据库连接配置信息
    conn1 = pymysql.connect(
        host="filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
        user="admin",
        password="WATNfBJYaZ4FPVVzdYCq",
        database="filbet_dev_main"
    )

    conn2 = pymysql.connect(
        host="filbet-zi-dev-aurora-cluster.cluster-c0mmrepgi1ky.us-west-2.rds.amazonaws.com",
        user="admin",
        password="WATNfBJYaZ4FPVVzdYCq",
        database="filbet_pre_main"
    )

    # 开始比对
    compare_databases_structure(conn1, conn2)

except Exception as e:
    print(f"Error: {e}")
finally:
    conn1.close()
    conn2.close()
