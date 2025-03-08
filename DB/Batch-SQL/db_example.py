#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库执行器模块使用示例
展示如何连接MySQL、SQL Server、Oracle和PostgreSQL数据库并执行SQL语句
"""

from db_executor import MySQLConfig, SQLServerConfig, OracleConfig, PostgreSQLConfig, DBExecutor

def main():
    """主函数"""
    # 创建MySQL配置
    mysql_config = MySQLConfig(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        instance_name="local_mysql"
    )
    
    # 创建SQL Server配置 - 使用SQL Server身份验证
    sqlserver_config = SQLServerConfig(
        server="localhost",
        database="test_db",
        user="sa",
        password="password",
        instance_name="local_sqlserver"
    )
    
    # 创建Oracle配置 - 使用服务名
    oracle_config = OracleConfig(
        user="system",
        password="oracle",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
        instance_name="local_oracle"
    )
    
    # 创建Oracle配置 - 使用SID
    oracle_sid_config = OracleConfig(
        user="system",
        password="oracle",
        host="localhost",
        port=1521,
        sid="XE",
        instance_name="local_oracle_sid"
    )
    
    # 创建Oracle配置 - 使用DSN
    oracle_dsn_config = OracleConfig(
        user="system",
        password="oracle",
        dsn="custom_dsn",
        instance_name="local_oracle_dsn"
    )
    
    # 创建PostgreSQL配置
    postgresql_config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="test_db",
        instance_name="local_postgresql"
    )
    
    # 创建执行器
    executor = DBExecutor(output_dir="./db_output")
    
    # 示例1: 在不同数据库实例上执行单个SQL
    print("\n示例1: 在不同数据库实例上执行单个SQL")
    
    # 注意：不同数据库的SQL语法有所不同
    mysql_sql = "SELECT * FROM users LIMIT 5"
    sqlserver_sql = "SELECT TOP 5 * FROM users"
    oracle_sql = "SELECT * FROM users WHERE ROWNUM <= 5"
    postgresql_sql = "SELECT * FROM users LIMIT 5"
    
    # 执行查询
    try:
        mysql_result = executor.execute_sql(mysql_config, mysql_sql)
        print("\nMySQL查询结果:")
        print_result(mysql_result)
    except Exception as e:
        print(f"MySQL查询失败: {str(e)}")
    
    try:
        sqlserver_result = executor.execute_sql(sqlserver_config, sqlserver_sql)
        print("\nSQL Server查询结果:")
        print_result(sqlserver_result)
    except Exception as e:
        print(f"SQL Server查询失败: {str(e)}")
    
    try:
        oracle_result = executor.execute_sql(oracle_config, oracle_sql)
        print("\nOracle查询结果:")
        print_result(oracle_result)
    except Exception as e:
        print(f"Oracle查询失败: {str(e)}")
    
    try:
        postgresql_result = executor.execute_sql(postgresql_config, postgresql_sql)
        print("\nPostgreSQL查询结果:")
        print_result(postgresql_result)
    except Exception as e:
        print(f"PostgreSQL查询失败: {str(e)}")
    
    # 示例2: 在不同数据库实例上执行多个SQL
    print("\n示例2: 在不同数据库实例上执行多个SQL")
    
    # MySQL SQL语句
    mysql_sql_list = [
        "SELECT * FROM products LIMIT 3",
        "UPDATE users SET last_login = NOW() WHERE id = 1",
        "SELECT * FROM users WHERE id = 1"
    ]
    
    # SQL Server SQL语句
    sqlserver_sql_list = [
        "SELECT TOP 3 * FROM products",
        "UPDATE users SET last_login = GETDATE() WHERE id = 1",
        "SELECT * FROM users WHERE id = 1"
    ]
    
    # Oracle SQL语句
    oracle_sql_list = [
        "SELECT * FROM products WHERE ROWNUM <= 3",
        "UPDATE users SET last_login = SYSDATE WHERE id = 1",
        "SELECT * FROM users WHERE id = 1"
    ]
    
    # PostgreSQL SQL语句
    postgresql_sql_list = [
        "SELECT * FROM products LIMIT 3",
        "UPDATE users SET last_login = NOW() WHERE id = 1",
        "SELECT * FROM users WHERE id = 1"
    ]
    
    # 执行多个SQL
    try:
        mysql_results = executor.execute_multiple_sql(mysql_config, mysql_sql_list)
        print("\nMySQL多SQL执行结果:")
        for i, result in enumerate(mysql_results):
            print(f"\n执行MySQL SQL {i+1}:")
            print_result(result)
    except Exception as e:
        print(f"MySQL多SQL执行失败: {str(e)}")
    
    try:
        sqlserver_results = executor.execute_multiple_sql(sqlserver_config, sqlserver_sql_list)
        print("\nSQL Server多SQL执行结果:")
        for i, result in enumerate(sqlserver_results):
            print(f"\n执行SQL Server SQL {i+1}:")
            print_result(result)
    except Exception as e:
        print(f"SQL Server多SQL执行失败: {str(e)}")
    
    try:
        oracle_results = executor.execute_multiple_sql(oracle_config, oracle_sql_list)
        print("\nOracle多SQL执行结果:")
        for i, result in enumerate(oracle_results):
            print(f"\n执行Oracle SQL {i+1}:")
            print_result(result)
    except Exception as e:
        print(f"Oracle多SQL执行失败: {str(e)}")
    
    try:
        postgresql_results = executor.execute_multiple_sql(postgresql_config, postgresql_sql_list)
        print("\nPostgreSQL多SQL执行结果:")
        for i, result in enumerate(postgresql_results):
            print(f"\n执行PostgreSQL SQL {i+1}:")
            print_result(result)
    except Exception as e:
        print(f"PostgreSQL多SQL执行失败: {str(e)}")
    
    # 示例3: 在多个实例上执行相同的SQL (注意语法差异)
    print("\n示例3: 在多个实例上执行相同的SQL")
    
    # 由于不同数据库的SQL语法有所不同，我们需要为每种数据库类型准备不同的SQL语句
    # 这里我们使用一个通用的SQL语句，但在实际应用中可能需要针对不同数据库进行调整
    count_sql = "SELECT COUNT(*) as count FROM users"
    
    # 创建配置列表
    configs = [mysql_config, sqlserver_config, oracle_config, postgresql_config]
    
    # 逐个执行
    for config in configs:
        try:
            result = executor.execute_sql(config, count_sql)
            print(f"\n在 {config.instance_name} 上执行 COUNT 查询:")
            print_result(result)
        except Exception as e:
            print(f"在 {config.instance_name} 上执行失败: {str(e)}")
    
    # 示例4: 批量执行 - 在多个实例上执行多个SQL
    print("\n示例4: 批量执行 - 在多个实例上执行多个SQL")
    
    # 为简化示例，我们只使用MySQL和PostgreSQL（语法相似）
    simple_configs = [mysql_config, postgresql_config]
    simple_sql_list = [
        "SELECT * FROM orders LIMIT 2",
        "SELECT * FROM customers LIMIT 2"
    ]
    
    try:
        batch_results = executor.execute_batch(simple_configs, simple_sql_list)
        for instance_name, instance_results in batch_results.items():
            print(f"\n实例 {instance_name} 批量执行结果:")
            for i, result in enumerate(instance_results):
                print(f"  SQL {i+1}:")
                print_result(result, indent="  ")
    except Exception as e:
        print(f"批量执行失败: {str(e)}")

def print_result(result, indent=""):
    """打印执行结果"""
    if result['success']:
        if result['is_select']:
            print(f"{indent}查询成功，返回 {len(result['data']) if result['data'] else 0} 条记录")
            if result['csv_path']:
                print(f"{indent}结果已保存到: {result['csv_path']}")
        else:
            print(f"{indent}执行成功，影响 {result['affected_rows']} 行")
    else:
        print(f"{indent}执行失败: {result['error']}")

if __name__ == "__main__":
    main() 
