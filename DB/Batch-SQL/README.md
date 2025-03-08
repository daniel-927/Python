# 数据库执行器

这是一个Python模块，用于连接MySQL、SQL Server、Oracle和PostgreSQL数据库并执行SQL语句。该模块支持批量执行多个SQL语句和在多个数据库实例上执行操作，并且会将SELECT查询的结果导出为CSV文件。

## 功能特点

- 支持连接多种数据库：MySQL、SQL Server、Oracle和PostgreSQL
- 连接单个或多个数据库实例
- 执行单条或多条SQL语句
- 自动识别SELECT查询并将结果导出为CSV文件
- 支持批量在多个数据库实例上执行多条SQL语句
- 详细的日志记录
- 完整的错误处理

## 安装依赖

```bash
pip install pymysql pandas pyodbc cx_Oracle psycopg2-binary
```

或者使用requirements.txt文件：

```bash
pip install -r requirements.txt
```

### 数据库驱动程序

对于某些数据库，您可能需要安装额外的驱动程序：

#### SQL Server

- Windows: [Microsoft ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- Linux: [Microsoft ODBC Driver for SQL Server on Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- macOS: [Microsoft ODBC Driver for SQL Server on macOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos)

#### Oracle

- [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client/downloads.html)

## 使用方法

### 连接MySQL数据库

```python
from db_executor import MySQLConfig, DBExecutor

# 创建MySQL配置
mysql_config = MySQLConfig(
    host="localhost",
    port=3306,
    user="root",
    password="password",
    database="test_db",
    instance_name="local_mysql"  # 可选，默认为mysql_host_port_database
)

# 创建执行器
executor = DBExecutor(output_dir="./db_output")  # 可选，默认为./output

# 执行单个SQL语句
result = executor.execute_sql(mysql_config, "SELECT * FROM users LIMIT 10")
```

### 连接SQL Server数据库

```python
from db_executor import SQLServerConfig, DBExecutor

# 创建SQL Server配置 - 使用SQL Server身份验证
sqlserver_config = SQLServerConfig(
    server="localhost",
    database="test_db",
    user="sa",
    password="password",
    instance_name="local_sqlserver"  # 可选，默认为sqlserver_server_database
)

# 或者使用Windows身份验证
windows_auth_config = SQLServerConfig(
    server="localhost",
    database="test_db",
    trusted_connection=True,
    instance_name="windows_auth_sqlserver"
)

# 创建执行器
executor = DBExecutor(output_dir="./db_output")

# 执行单个SQL语句
result = executor.execute_sql(sqlserver_config, "SELECT TOP 10 * FROM users")
```

### 连接Oracle数据库

```python
from db_executor import OracleConfig, DBExecutor

# 创建Oracle配置 - 使用服务名
oracle_config = OracleConfig(
    user="system",
    password="oracle",
    host="localhost",
    port=1521,
    service_name="XEPDB1",
    instance_name="local_oracle"  # 可选，默认为oracle_host_service_name
)

# 或者使用SID
oracle_sid_config = OracleConfig(
    user="system",
    password="oracle",
    host="localhost",
    port=1521,
    sid="XE",
    instance_name="local_oracle_sid"
)

# 或者使用自定义DSN
oracle_dsn_config = OracleConfig(
    user="system",
    password="oracle",
    dsn="custom_dsn",
    instance_name="local_oracle_dsn"
)

# 创建执行器
executor = DBExecutor(output_dir="./db_output")

# 执行单个SQL语句
result = executor.execute_sql(oracle_config, "SELECT * FROM users WHERE ROWNUM <= 10")
```

### 连接PostgreSQL数据库

```python
from db_executor import PostgreSQLConfig, DBExecutor

# 创建PostgreSQL配置
postgresql_config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    user="postgres",
    password="postgres",
    database="test_db",
    instance_name="local_postgresql"  # 可选，默认为postgresql_host_port_database
)

# 创建执行器
executor = DBExecutor(output_dir="./db_output")

# 执行单个SQL语句
result = executor.execute_sql(postgresql_config, "SELECT * FROM users LIMIT 10")
```

### 执行多条SQL语句

```python
# 创建SQL语句列表
sql_list = [
    "SELECT * FROM products LIMIT 3",
    "UPDATE users SET last_login = NOW() WHERE id = 1",
    "SELECT * FROM users WHERE id = 1"
]

# 在MySQL实例上执行
mysql_results = executor.execute_multiple_sql(mysql_config, sql_list)

# 注意：不同数据库的SQL语法可能有所不同
# 例如，Oracle使用SYSDATE而不是NOW()，SQL Server使用GETDATE()
```

### 在多个实例上执行SQL

```python
# 创建多个配置
configs = [mysql_config, postgresql_config]  # 语法相似的数据库

# 在多个实例上执行单个SQL
results = executor.execute_on_multiple_instances(configs, "SELECT COUNT(*) as count FROM users")

# 在多个实例上执行多个SQL
batch_sql_list = [
    "SELECT * FROM orders LIMIT 2",
    "SELECT * FROM customers LIMIT 2"
]
results = executor.execute_batch(configs, batch_sql_list)
```

## 返回结果格式

执行SQL后返回的结果是一个字典，包含以下字段：

- `instance`: 数据库实例名称
- `sql`: 执行的SQL语句
- `success`: 是否执行成功
- `is_select`: 是否为SELECT查询
- `data`: 查询结果数据（仅对SELECT查询有效）
- `affected_rows`: 影响的行数（仅对非SELECT查询有效）
- `csv_path`: CSV文件路径（仅对SELECT查询有效且有结果时）
- `error`: 错误信息（仅当执行失败时）

## 完整示例

请参考 `db_example.py` 文件，其中包含了各种使用场景的示例代码。

## 注意事项

- 确保已安装所需的依赖包：`pymysql`、`pandas`、`pyodbc`、`cx_Oracle` 和 `psycopg2-binary`
- 对于SQL Server和Oracle连接，需要安装相应的客户端驱动程序
- 对于SELECT查询，结果会自动保存为CSV文件
- CSV文件名格式为：`{实例名}_{SQL标识符}_{时间戳}.csv`
- 默认情况下，CSV文件保存在 `./output` 目录下，可以通过 `output_dir` 参数修改
- 所有操作都会记录详细的日志
- 注意不同数据库的SQL语法差异，特别是在批量执行或在多个不同类型的数据库实例上执行SQL时
- 对于Oracle数据库，可以使用服务名、SID或自定义DSN进行连接
- 对于PostgreSQL数据库，可以设置SSL模式（默认为'prefer'） 
