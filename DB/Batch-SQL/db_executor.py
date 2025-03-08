#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库执行器模块
用于连接MySQL、SQL Server、Oracle和PostgreSQL数据库并执行SQL语句，支持批量执行和多实例执行
"""

import os
import csv
import time
import logging
import pandas as pd
from typing import List, Dict, Union, Tuple, Optional, Any
from abc import ABC, abstractmethod
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

class DBConfig(ABC):
    """数据库连接配置基类"""
    
    def __init__(self, instance_name: str = None):
        """
        初始化数据库连接配置
        
        Args:
            instance_name: 实例名称，用于标识不同的数据库实例
        """
        self.instance_name = instance_name
        self.db_type = "unknown"
    
    @abstractmethod
    def get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        pass
    
    @abstractmethod
    def __str__(self) -> str:
        """字符串表示"""
        pass


class MySQLConfig(DBConfig):
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
        super().__init__(instance_name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.charset = charset
        self.instance_name = instance_name or f"mysql_{host}_{port}_{database}"
        self.db_type = "mysql"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'charset': self.charset
        }
    
    def __str__(self) -> str:
        return f"MySQLConfig(instance={self.instance_name}, host={self.host}, port={self.port}, database={self.database})"


class SQLServerConfig(DBConfig):
    """SQL Server连接配置类"""
    
    def __init__(self, server: str, database: str, user: str = None, password: str = None, 
                 trusted_connection: bool = False, driver: str = "ODBC Driver 17 for SQL Server", 
                 instance_name: str = None):
        """
        初始化SQL Server连接配置
        
        Args:
            server: SQL Server服务器地址
            database: 数据库名
            user: 用户名（如果使用SQL Server身份验证）
            password: 密码（如果使用SQL Server身份验证）
            trusted_connection: 是否使用Windows身份验证，默认False
            driver: ODBC驱动名称，默认"ODBC Driver 17 for SQL Server"
            instance_name: 实例名称，用于标识不同的SQL Server实例
        """
        super().__init__(instance_name)
        self.server = server
        self.database = database
        self.user = user
        self.password = password
        self.trusted_connection = trusted_connection
        self.driver = driver
        self.instance_name = instance_name or f"sqlserver_{server}_{database}"
        self.db_type = "sqlserver"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        params = {
            'server': self.server,
            'database': self.database,
            'driver': self.driver
        }
        
        if self.trusted_connection:
            params['trusted_connection'] = 'yes'
        else:
            params['uid'] = self.user
            params['pwd'] = self.password
            
        return params
    
    def __str__(self) -> str:
        auth_type = "Windows身份验证" if self.trusted_connection else "SQL Server身份验证"
        return f"SQLServerConfig(instance={self.instance_name}, server={self.server}, database={self.database}, auth={auth_type})"


class OracleConfig(DBConfig):
    """Oracle连接配置类"""
    
    def __init__(self, user: str, password: str, dsn: str = None, host: str = None, port: int = 1521, 
                 service_name: str = None, sid: str = None, instance_name: str = None):
        """
        初始化Oracle连接配置
        
        Args:
            user: 用户名
            password: 密码
            dsn: 数据源名称（如果提供，将直接使用此DSN连接）
            host: Oracle服务器主机地址（如果未提供dsn，则需要提供）
            port: Oracle服务器端口（如果未提供dsn，则需要提供），默认1521
            service_name: 服务名（如果未提供dsn和sid，则需要提供）
            sid: SID（如果未提供dsn和service_name，则需要提供）
            instance_name: 实例名称，用于标识不同的Oracle实例
        """
        super().__init__(instance_name)
        self.user = user
        self.password = password
        self.dsn = dsn
        self.host = host
        self.port = port
        self.service_name = service_name
        self.sid = sid
        
        if not dsn and not (host and (service_name or sid)):
            raise ValueError("必须提供dsn或者host和(service_name或sid)")
        
        self.instance_name = instance_name or f"oracle_{host or 'custom'}_{service_name or sid or 'dsn'}"
        self.db_type = "oracle"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        params = {
            'user': self.user,
            'password': self.password
        }
        
        if self.dsn:
            params['dsn'] = self.dsn
        else:
            if self.service_name:
                # 使用service_name构建dsn
                params['dsn'] = cx_Oracle.makedsn(self.host, self.port, service_name=self.service_name)
            else:
                # 使用sid构建dsn
                params['dsn'] = cx_Oracle.makedsn(self.host, self.port, sid=self.sid)
        
        return params
    
    def __str__(self) -> str:
        if self.dsn:
            return f"OracleConfig(instance={self.instance_name}, user={self.user}, dsn=custom)"
        elif self.service_name:
            return f"OracleConfig(instance={self.instance_name}, host={self.host}, port={self.port}, service_name={self.service_name})"
        else:
            return f"OracleConfig(instance={self.instance_name}, host={self.host}, port={self.port}, sid={self.sid})"


class PostgreSQLConfig(DBConfig):
    """PostgreSQL连接配置类"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str, 
                 sslmode: str = 'prefer', instance_name: str = None):
        """
        初始化PostgreSQL连接配置
        
        Args:
            host: PostgreSQL主机地址
            port: PostgreSQL端口
            user: 用户名
            password: 密码
            database: 数据库名
            sslmode: SSL模式，默认'prefer'
            instance_name: 实例名称，用于标识不同的PostgreSQL实例
        """
        super().__init__(instance_name)
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.sslmode = sslmode
        self.instance_name = instance_name or f"postgresql_{host}_{port}_{database}"
        self.db_type = "postgresql"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """获取连接参数"""
        return {
            'host': self.host,
            'port': self.port,
            'user': self.user,
            'password': self.password,
            'database': self.database,
            'sslmode': self.sslmode
        }
    
    def __str__(self) -> str:
        return f"PostgreSQLConfig(instance={self.instance_name}, host={self.host}, port={self.port}, database={self.database})"


class DBExecutor:
    """数据库执行器类"""
    
    def __init__(self, output_dir: str = './output'):
        """
        初始化数据库执行器
        
        Args:
            output_dir: 输出目录，用于保存查询结果的CSV文件
        """
        self.output_dir = output_dir
        # 确保输出目录存在
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logger.info(f"初始化数据库执行器，输出目录: {output_dir}")
    
    def connect(self, config: DBConfig):
        """
        连接到数据库
        
        Args:
            config: 数据库连接配置
            
        Returns:
            数据库连接对象
        """
        try:
            if isinstance(config, MySQLConfig):
                import pymysql
                connection = pymysql.connect(
                    **config.get_connection_params(),
                    cursorclass=pymysql.cursors.DictCursor
                )
                logger.info(f"成功连接到MySQL实例: {config.instance_name}")
                return connection
            
            elif isinstance(config, SQLServerConfig):
                import pyodbc
                conn_str_parts = []
                params = config.get_connection_params()
                
                for key, value in params.items():
                    conn_str_parts.append(f"{key}={value}")
                
                conn_str = ";".join(conn_str_parts)
                connection = pyodbc.connect(conn_str)
                logger.info(f"成功连接到SQL Server实例: {config.instance_name}")
                return connection
            
            elif isinstance(config, OracleConfig):
                import cx_Oracle
                connection = cx_Oracle.connect(**config.get_connection_params())
                logger.info(f"成功连接到Oracle实例: {config.instance_name}")
                return connection
            
            elif isinstance(config, PostgreSQLConfig):
                import psycopg2
                import psycopg2.extras
                connection = psycopg2.connect(
                    **config.get_connection_params()
                )
                logger.info(f"成功连接到PostgreSQL实例: {config.instance_name}")
                return connection
            
            else:
                raise ValueError(f"不支持的数据库配置类型: {type(config)}")
        except Exception as e:
            logger.error(f"连接数据库实例 {config.instance_name} 失败: {str(e)}")
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
    
    def execute_query(self, connection, config: DBConfig, sql: str, 
                     params: Optional[Union[tuple, dict]] = None) -> Tuple[bool, Union[List[Dict], int]]:
        """
        执行单个SQL查询
        
        Args:
            connection: 数据库连接对象
            config: 数据库配置
            sql: SQL语句
            params: SQL参数
            
        Returns:
            (是否为SELECT查询, 查询结果或影响行数)
        """
        is_select = self.is_select_query(sql)
        
        try:
            if isinstance(config, MySQLConfig):
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
            
            elif isinstance(config, SQLServerConfig):
                cursor = connection.cursor()
                cursor.execute(sql, params or [])
                
                if is_select:
                    # 获取列名
                    columns = [column[0] for column in cursor.description]
                    # 获取所有行
                    rows = cursor.fetchall()
                    # 转换为字典列表
                    result = []
                    for row in rows:
                        result.append(dict(zip(columns, row)))
                    logger.info(f"查询成功，返回 {len(result)} 条记录")
                    return True, result
                else:
                    connection.commit()
                    affected_rows = cursor.rowcount
                    logger.info(f"执行成功，影响 {affected_rows} 行")
                    return False, affected_rows
            
            elif isinstance(config, OracleConfig):
                cursor = connection.cursor()
                cursor.execute(sql, params or {})
                
                if is_select:
                    # 获取列名
                    columns = [col[0] for col in cursor.description]
                    # 获取所有行
                    rows = cursor.fetchall()
                    # 转换为字典列表
                    result = []
                    for row in rows:
                        result.append(dict(zip(columns, row)))
                    logger.info(f"查询成功，返回 {len(result)} 条记录")
                    return True, result
                else:
                    connection.commit()
                    affected_rows = cursor.rowcount
                    logger.info(f"执行成功，影响 {affected_rows} 行")
                    return False, affected_rows
            
            elif isinstance(config, PostgreSQLConfig):
                # 创建一个返回字典的游标
                cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
                cursor.execute(sql, params or {})
                
                if is_select:
                    # 获取所有行
                    rows = cursor.fetchall()
                    # 转换为字典列表
                    result = [dict(row) for row in rows]
                    logger.info(f"查询成功，返回 {len(result)} 条记录")
                    return True, result
                else:
                    connection.commit()
                    affected_rows = cursor.rowcount
                    logger.info(f"执行成功，影响 {affected_rows} 行")
                    return False, affected_rows
            
            else:
                raise ValueError(f"不支持的数据库配置类型: {type(config)}")
        except Exception as e:
            if hasattr(connection, 'rollback'):
                connection.rollback()
            logger.error(f"执行SQL失败: {str(e)}")
            logger.error(f"SQL语句: {sql}")
            raise
    
    def save_to_csv(self, data: List[Dict], instance_name: str, sql: str) -> str:
        """
        将查询结果保存为CSV文件
        
        Args:
            data: 查询结果数据
            instance_name: 数据库实例名称
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
    
    def execute_sql(self, config: DBConfig, sql: str, 
                   params: Optional[Union[tuple, dict]] = None) -> Dict[str, Any]:
        """
        在单个数据库实例上执行单个SQL语句
        
        Args:
            config: 数据库连接配置
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
            is_select, query_result = self.execute_query(connection, config, sql, params)
            
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
    
    def execute_multiple_sql(self, config: DBConfig, sql_list: List[str]) -> List[Dict[str, Any]]:
        """
        在单个数据库实例上执行多个SQL语句
        
        Args:
            config: 数据库连接配置
            sql_list: SQL语句列表
            
        Returns:
            执行结果列表
        """
        results = []
        
        for sql in sql_list:
            result = self.execute_sql(config, sql)
            results.append(result)
            
        return results
    
    def execute_on_multiple_instances(self, configs: List[DBConfig], 
                                     sql: str) -> Dict[str, Dict[str, Any]]:
        """
        在多个数据库实例上执行单个SQL语句
        
        Args:
            configs: 数据库连接配置列表
            sql: SQL语句
            
        Returns:
            按实例名称索引的执行结果字典
        """
        results = {}
        
        for config in configs:
            result = self.execute_sql(config, sql)
            results[config.instance_name] = result
            
        return results
    
    def execute_batch(self, configs: List[DBConfig], 
                     sql_list: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        在多个数据库实例上执行多个SQL语句
        
        Args:
            configs: 数据库连接配置列表
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
    mysql_config = MySQLConfig(
        host="localhost",
        port=3306,
        user="root",
        password="password",
        database="test_db",
        instance_name="local_mysql"
    )
    
    # 创建SQL Server配置
    sqlserver_config = SQLServerConfig(
        server="localhost",
        database="test_db",
        user="sa",
        password="password",
        instance_name="local_sqlserver"
    )
    
    # 创建Oracle配置
    oracle_config = OracleConfig(
        user="system",
        password="oracle",
        host="localhost",
        port=1521,
        service_name="XEPDB1",
        instance_name="local_oracle"
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
    
    # 执行单个查询
    mysql_result = executor.execute_sql(mysql_config, "SELECT * FROM users LIMIT 10")
    sqlserver_result = executor.execute_sql(sqlserver_config, "SELECT TOP 10 * FROM users")
    oracle_result = executor.execute_sql(oracle_config, "SELECT * FROM users WHERE ROWNUM <= 10")
    postgresql_result = executor.execute_sql(postgresql_config, "SELECT * FROM users LIMIT 10")
    
    # 打印结果
    for result in [mysql_result, sqlserver_result, oracle_result, postgresql_result]:
        if result['success'] and result['is_select']:
            print(f"查询成功，结果已保存到: {result['csv_path']}")
        elif not result['success']:
            print(f"查询失败: {result['error']}") 
