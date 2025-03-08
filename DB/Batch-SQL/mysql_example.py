#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL执行器模块使用示例
"""

from mysql_executor import MySQLConfig, MySQLExecutor

def main():
    """主函数"""
    # 创建MySQL配置
    config1 = MySQLConfig(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        instance_name="local_test"
    )
    
    config2 = MySQLConfig(
        host="192.168.1.100",
        port=3306,
        user="admin",
        password="admin123",
        database="prod_db",
        instance_name="prod_server"
    )
    
    # 创建执行器
    executor = MySQLExecutor(output_dir="./mysql_output")
    
    # 示例1: 在单个实例上执行单个SQL
    print("\n示例1: 在单个实例上执行单个SQL")
    result = executor.execute_sql(config1, "SELECT * FROM users LIMIT 5")
    print_result(result)
    
    # 示例2: 在单个实例上执行多个SQL
    print("\n示例2: 在单个实例上执行多个SQL")
    sql_list = [
        "SELECT * FROM products LIMIT 3",
        "UPDATE users SET last_login = NOW() WHERE id = 1",
        "SELECT * FROM users WHERE id = 1"
    ]
    results = executor.execute_multiple_sql(config1, sql_list)
    for i, result in enumerate(results):
        print(f"\n执行SQL {i+1}:")
        print_result(result)
    
    # 示例3: 在多个实例上执行单个SQL
    print("\n示例3: 在多个实例上执行单个SQL")
    configs = [config1, config2]
    results = executor.execute_on_multiple_instances(configs, "SELECT COUNT(*) as count FROM users")
    for instance_name, result in results.items():
        print(f"\n实例 {instance_name}:")
        print_result(result)
    
    # 示例4: 在多个实例上执行多个SQL (批量执行)
    print("\n示例4: 在多个实例上执行多个SQL (批量执行)")
    batch_sql_list = [
        "SELECT * FROM orders LIMIT 2",
        "SELECT * FROM customers LIMIT 2"
    ]
    results = executor.execute_batch(configs, batch_sql_list)
    for instance_name, instance_results in results.items():
        print(f"\n实例 {instance_name}:")
        for i, result in enumerate(instance_results):
            print(f"  SQL {i+1}:")
            print_result(result, indent="  ")

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
