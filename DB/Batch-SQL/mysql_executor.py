#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL执行器模块
用于连接MySQL数据库并执行SQL语句，支持批量执行和多实例执行
"""

import os
import csv
import time
import logging
import pymysql
import pandas as pd
from typing import List, Dict, Union, Tuple, Optional, Any
from logging.handlers import RotatingFileHandler

# 确保日志目录存在
log_dir = './logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# 配置日志文件路径
log_file = os.path.join(log_dir, 'mysql_executor.log')

# 创建日志处理器
file_handler = RotatingFileHandler(
    log_file,
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5,
    encoding='utf-8'
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# 创建控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# 配置日志器
logger = logging.getLogger('mysql_executor')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)
# 防止日志重复输出
logger.propagate = False

class MySQLConfig:
    """MySQL连接配置类"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str, 
                 charset: str = 'utf8mb4', instance_name: str = None):
        """
        初始化MySQL连接配置
        
        Args:
            host: MySQL主机地址
            port: MySQL端口
            user: 用户名
            password: 密码
            database: 数据库名
            charset: 字符集，默认utf8mb4
            instance_name: 实例名称，用于标识不同的MySQL实例
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.instance_name = instance_name or f"{host}_{port}_{database}"
    
    def __str__(self) -> str:
        return f"MySQLConfig(instance={self.instance_name}, host={self.host}, port={self.port}, database={self.database})"


class MySQLExecutor:
    """MySQL执行器类"""
    
    def __init__(self, output_dir: str = './output'):
        """
        初始化MySQL执行器
        
        Args:
            output_dir: 输出目录，用于保存查询结果的CSV文件
        """
        self.output_dir = output_dir
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logger.info(f"初始化MySQL执行器，输出目录: {output_dir}")
    
    def connect(self, config: MySQLConfig) -> pymysql.connections.Connection:
        """
        连接到MySQL数据库
        
        Args:
            config: MySQL连接配置
            
        Returns:
            MySQL连接对象
        """
        try:
            connection = pymysql.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                database=config.database,
                charset=config.charset,
                cursorclass=pymysql.cursors.DictCursor
            )
            logger.info(f"成功连接到MySQL实例: {config.instance_name}")
            return connection
        except Exception as e:
            logger.error(f"连接MySQL实例 {config.instance_name} 失败: {str(e)}")
            raise
    
    def is_select_query(self, sql: str) -> bool:
        """
        判断SQL语句是否为SELECT查询
        
        Args:
            sql: SQL语句
            
        Returns:
            是否为SELECT查询
        """
        return sql.strip().lower().startswith('select')
    
    def execute_query(self, connection: pymysql.connections.Connection, sql: str, 
                     params: Optional[Union[tuple, dict]] = None) -> Tuple[bool, Union[List[Dict], int]]:
        """
        执行单个SQL查询
        
        Args:
            connection: MySQL连接对象
            sql: SQL语句
            params: SQL参数
            
        Returns:
            (是否为SELECT查询, 查询结果或影响行数)
        """
        is_select = self.is_select_query(sql)
        
        try:
            with connection.cursor() as cursor:
                affected_rows = cursor.execute(sql, params)
                
                if is_select:
                    result = cursor.fetchall()
                    logger.info(f"查询成功，返回 {len(result)} 条记录")
                    return True, result
                else:
                    connection.commit()
                    logger.info(f"执行成功，影响 {affected_rows} 行")
                    return False, affected_rows
        except Exception as e:
            connection.rollback()
            logger.error(f"执行SQL失败: {str(e)}")
            logger.error(f"SQL语句: {sql}")
            raise
    
    def save_to_csv(self, data: List[Dict], instance_name: str, sql: str) -> str:
        """
        将查询结果保存为CSV文件
        
        Args:
            data: 查询结果数据
            instance_name: MySQL实例名称
            sql: 执行的SQL语句
            
        Returns:
            CSV文件路径
        """
        if not data:
            logger.warning("查询结果为空，不生成CSV文件")
            return ""
        
        # 生成文件名
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        # 从SQL中提取一个简短的标识符
        sql_identifier = sql.strip().lower().replace('select', '').split('from')[0].strip()
        sql_identifier = ''.join(c if c.isalnum() else '_' for c in sql_identifier)
        sql_identifier = sql_identifier[:30]  # 限制长度
        
        filename = f"{instance_name}_{sql_identifier}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        # 使用pandas保存为CSV
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"查询结果已保存到: {filepath}")
        return filepath
    
    def execute_sql(self, config: MySQLConfig, sql: str, 
                   params: Optional[Union[tuple, dict]] = None) -> Dict[str, Any]:
        """
        在单个MySQL实例上执行单个SQL语句
        
        Args:
            config: MySQL连接配置
            sql: SQL语句
            params: SQL参数
            
        Returns:
            执行结果字典
        """
        result = {
            'instance': config.instance_name,
            'sql': sql,
            'success': False,
            'is_select': False,
            'data': None,
            'affected_rows': 0,
            'csv_path': '',
            'error': None
        }
        
        connection = None
        try:
            connection = self.connect(config)
            is_select, query_result = self.execute_query(connection, sql, params)
            
            result['success'] = True
            result['is_select'] = is_select
            
            if is_select:
                result['data'] = query_result
                if query_result:
                    csv_path = self.save_to_csv(query_result, config.instance_name, sql)
                    result['csv_path'] = csv_path
            else:
                result['affected_rows'] = query_result
                
        except Exception as e:
            result['success'] = False
            result['error'] = str(e)
            logger.error(f"在实例 {config.instance_name} 上执行SQL失败: {str(e)}")
        finally:
            if connection:
                connection.close()
                
        return result
    
    def execute_multiple_sql(self, config: MySQLConfig, sql_list: List[str]) -> List[Dict[str, Any]]:
        """
        在单个MySQL实例上执行多个SQL语句
        
        Args:
            config: MySQL连接配置
            sql_list: SQL语句列表
            
        Returns:
            执行结果列表
        """
        results = []
        
        for sql in sql_list:
            result = self.execute_sql(config, sql)
            results.append(result)
            
        return results
    
    def execute_on_multiple_instances(self, configs: List[MySQLConfig], 
                                     sql: str) -> Dict[str, Dict[str, Any]]:
        """
        在多个MySQL实例上执行单个SQL语句
        
        Args:
            configs: MySQL连接配置列表
            sql: SQL语句
            
        Returns:
            按实例名称索引的执行结果字典
        """
        results = {}
        
        for config in configs:
            result = self.execute_sql(config, sql)
            results[config.instance_name] = result
            
        return results
    
    def execute_batch(self, configs: List[MySQLConfig], 
                     sql_list: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        在多个MySQL实例上执行多个SQL语句
        
        Args:
            configs: MySQL连接配置列表
            sql_list: SQL语句列表
            
        Returns:
            按实例名称索引的执行结果列表字典
        """
        results = {}
        
        for config in configs:
            instance_results = self.execute_multiple_sql(config, sql_list)
            results[config.instance_name] = instance_results
            
        return results


# 使用示例
if __name__ == "__main__":
    # 创建MySQL配置
    config1 = MySQLConfig(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        instance_name="local_test"
    )
    
    # 创建执行器
    executor = MySQLExecutor(output_dir="./mysql_output")
    
    # 执行单个查询
    result = executor.execute_sql(config1, "SELECT * FROM users LIMIT 10")
    
    # 打印结果
    if result['success'] and result['is_select']:
        print(f"查询成功，结果已保存到: {result['csv_path']}")
    elif not result['success']:
        print(f"查询失败: {result['error']}") 
