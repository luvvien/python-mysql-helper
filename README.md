- Program : MySQL db helper
- Author : Vien
- History
  - first release, support py2&py3  2017/09/05  Vien

## Class
**Constructor**
```
def __init__(self, host, user, passwd, schema, charset='utf8mb4',
                 autocommit=True, mincached=1, maxcached=5, maxshared=5)
```
sample 1 :

```
db_conf = {
            'user' : 'root',
            'passwd' : 'moma',
            'host' : '127.0.0.1',
            'schema' : 'dbname',
            'charset' : 'utf8mb4',
            'mincached' : 1,
            'maxcached' : 3,
            'maxshared' : 3,
        }
with DBHelper(**db_conf) as db:
    db.execute(" select 1 ") 
```

sample 2:

```
db = DBHelper('127.0.0.1', 'root', 'moma', 'dbname')
db.execute(" select 1 ")
db.close()
```

> If u not use `with ... as ...` , u need to call close().



## Function

**get_last_sql(self)**

- 获取当前cursor执行的最后一条sql

**get_rowcount(self)**

- 获取当前cursor执行的最后一条sql的影响行数

**begin(self) & end(self)**

- 管理事务（默认情况下是自动管理事务）

  ```
  with DBHelper(**db_conf) as db:
      db.begin() # 开启事务
      db.execute(sql)
      db.execute_batch(sql)
      db.insert_batch(sql)
      db.end() # 关闭事务，这里一定要记得关闭操作，关闭操作包括提交事务和还原成自动管理事务
  ```

**use_schema(self, schema)**

- 切换数据库
- 其中schema是数据库名称

**get_last_id(self, table, pri='id')**

- 获取table最后一个主键的值
- 其中pri是主键名称，默认为id

**show_variable(self, var_name)**

- 获取数据库参数配置

- var_name是变量名，例如

  ```
  show_variable('max_allowed_packet') # 获取传送数据包最大值
  ```

**get_dicts(self, sql, params=None, convert=True)**

- 返回一个字典型（key-value，也就是字段名和值的组合）的list
- params是参数，可以是单个值，也可以是tuple或者list
- convert是是否转为list，默认为list，设置False会以tuple形式返回

**get_values(self, sql, params=None, convert=True)**

- 返回一个结果集list（不包括字段名称）
- params是参数，可以是单个值，也可以是tuple或者list
- convert是是否转为list，默认为list，设置False会以tuple形式返回

**execute(self, sql, params=None)**

- 执行一条sql
- params是参数，可以是单个值，也可以是tuple或者list

**execute_batch(self, sql, params, step_size=2000)**

- 批量执行sql
- params是参数，可以是tuple或者list
- step_size是一次执行数量，默认2000

sample:

```
    db_conf = {
        'user': 'vien',
        'passwd': 'test',
        'host': '127.0.0.1',
        'schema': 'test',
        'charset': 'utf8mb4'
    }
    with DBHelper(**db_conf) as db:
        print(db.show_variable("max_allowed_packet"))
        sql = 'insert into a_table(name, email) values (%s,%s)'
        params = [
            ['jj', 'jj@gmail.com'],
            ['bb', 'bb@gmail.com'],
            ['cc', 'cc@gmail.com'],
        ]
        db.execute_batch(sql, params, step_size=2000)
```

**insert_batch(self, sql, params, step_size=2000)**

- 批量插入
- params是参数，可以是tuple或者list
- step_size是一次执行数量，默认2000，如果一条记录很大，请不要将此参数设置过大，超过max_allowed_packet的大小可能会出问题

**create_table_like(self, target, origin)**

- 复制表（仅复制结构）
- target是新表的名字，origin是所要复制的表的名字

**load_data_infile(self, path, table, ignore_row=0, local=False, fields_terminated=',', enclosed='"',
                         lines_terminated='\\n', ignore=True
                         )**

- 从文件导入数据到表中
- 需要传入文件路径path和表名table，这两项是必须的
- 其他参数可选，分别是忽略的行数ignore_row，默认0，对有标头的表格可以忽略一行
- local参数是选择文件导入的位置是从mysql服务所在的主机还是从本机，默认False是与mysql服务同一主机的文件
- fields_terminated，enclosed，lines_terminated是几种分割换行字符，提供默认值
- ignore是选择是否插入重复数据，默认忽略重复数据

**select_data_outfile(self, sql, path, fields_terminated=',', enclosed='"',
                            lines_terminated='\\n')**

- 从表查找数据导出到文件
- 其中sql和path是必须的参数，分别是查询sql以及文件存放路径，如果已存在同名文件会失败。
- 后面三项是分割换行字符，提供默认值

**get_fields(self, table, no_pri=False)**

- 获取table的字段名
- no_pri是是否包含主键信息，默认返回，设置True为忽略主键

**get_fields_and_types(self, table, no_pri=False)**

- 获取table的字段名和类型
- no_pri是是否包含主键信息，默认返回，设置True为忽略主键

**get_fields_types_lens(self, table, no_pri=False)**

- 获取table的字段名、类型和长度
- no_pri是是否包含主键信息，默认返回，设置True为忽略主键

**show_create_table(self, table)**

- 获取table的建表语句
