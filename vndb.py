#!/usr/bin/env python
# -*- coding: UTF-8 -*-

try:
    import MySQLdb as DB
    xrg = xrange
except:
    import pymysql as DB
    xrg = range

" MySQL OP Helper "

__author__ = 'Vien'


class DBHelper(object):
    def __init__(self, host, user, passwd, schema, charset='utf8mb4',
                 autocommit=True):
        self._db_conf = {
            'user': user,
            'passwd': passwd,
            'host': host,
            'db': schema,
            'charset': charset
        }
        self._autocommit = autocommit
        self._conn = None
        self._dict_cursor = None
        self._tuple_cursor = None
        self._current_cursor = None

    def __enter__(self):
        self.get_conn()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def escape_str(self, s):
        return DB.escape_string(s)

    def literal(self, s):
        return self._conn.literal(s)

    def format_param(self, param, batch=False):
        if param:
            if batch:
                return [i if (isinstance(i, tuple) or isinstance(i, list)) else [i] for i in param]
            return ((isinstance(param, tuple) or isinstance(param, list)) and [param] or [[param]])[0]
        return None

    def get_last_sql(self):
        if self._current_cursor:
            return self._current_cursor._executed
        return False

    def get_rowcount(self):
        if self._current_cursor:
            return int(self._current_cursor.rowcount)
        return False

    def auto_commit(self, flag=True):
        self._autocommit = flag

    def begin(self):
        self._autocommit = False

    def end(self):
        self._conn.commit()
        self._autocommit = True

    def use_schema(self, schema):
        try:
            self._conn.select_db(schema)
        except Exception as e:
            raise e

    def get_conn(self):
        if self._conn:
            self._conn.ping()
            return self._conn
        try:
            self._conn = DB.connect(**self._db_conf)
        except Exception as e:
            raise e
        return self._conn

    def get_cursor(self, cursorclz=None):
        if not self._conn:
            self.get_conn()
        else:
            self._conn.ping()
        try:
            cursor = self._conn.cursor(cursorclz)
        except Exception as e:
            raise e
        return cursor

    def close(self):
        if self._conn:
            try:
                if self._dict_cursor:
                    self._dict_cursor.close()
                if self._tuple_cursor:
                    self._tuple_cursor.close()
                if self._conn:
                    self._conn.close()
            except Exception as e:
                raise e

    def get_last_id(self, table, pri='id'):
        last_id = self.get_values(" SELECT `%s` FROM `%s` ORDER BY `%s` DESC LIMIT 1 " % (pri, table, pri))
        return last_id[0] if last_id else False

    def show_variable(self, var_name):
        res = self.get_values(" SHOW VARIABLES LIKE '%{}%' ".format(var_name))
        return res[0] if res else False

    def get_dicts(self, sql, params=None, convert=True):
        if not self._dict_cursor:
            self._dict_cursor = self.get_cursor(DB.cursors.DictCursor)
        else:
            self._conn.ping()
        try:
            self._dict_cursor.execute(sql, self.format_param(params))
            self._current_cursor = self._dict_cursor
            res = self._dict_cursor.fetchall()
            if res:
                if convert:
                    return list(res)
                else:
                    return res
        except Exception as e:
            raise e
        return []

    def get_values(self, sql, params=None, convert=True):
        if not self._tuple_cursor:
            self._tuple_cursor = self.get_cursor()
        else:
            self._conn.ping()
        try:
            self._tuple_cursor.execute(sql, self.format_param(params))
            self._current_cursor = self._tuple_cursor
            res = self._tuple_cursor.fetchall()
            if res:
                if convert:
                    res_list = [(len(item) == 1 and [item[0]] or [item])[0] for item in res]
                    return res_list
                else:
                    return res
        except Exception as e:
            raise e
        return []

    def execute(self, sql, params=None):
        if not self._tuple_cursor:
            self._tuple_cursor = self.get_cursor()
        else:
            self._conn.ping()
        try:
            self._tuple_cursor.execute(sql, self.format_param(params))
            self._current_cursor = self._tuple_cursor
            if self._autocommit:
                self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise e
        return self.get_rowcount()

    def execute_batch(self, sql, params, step_size=2000):
        if not params:
            raise Exception("No params")
        if not self._current_cursor:
            self._tuple_cursor = self.get_cursor()
            self._current_cursor = self._tuple_cursor
        else:
            self._conn.ping()
        row_count = 0
        try:
            size = int(step_size)
            length = len(params)
            size = size if length >= size else length
            params = self.format_param(params, True)
            for i in xrg(0, (length + size - 1) // size):
                self._current_cursor.executemany(sql, params[i * size:(i + 1) * size])
                row_count = row_count + self.get_rowcount()
                if self._autocommit:
                    self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise e
        return row_count

    def insert_batch(self, sql, params, step_size=2000):
        if not params:
            raise Exception("No params")
        if not self._current_cursor:
            self._tuple_cursor = self.get_cursor()
            self._current_cursor = self._tuple_cursor
        else:
            self._conn.ping()
        try:
            size = int(step_size)
            length = len(params)
            size = size if length >= size else length
            params = self.format_param(params, True)
            pos = sql.rfind('(')
            values = []
            for i in xrg(0, (length + size - 1) // size):
                sql_left = sql[:pos]
                sql_right = sql[pos:]
                for args in params[i * size:(i + 1) * size]:
                    for arg in args:
                        values.append(arg)
                    sql_left = sql_left + sql_right + ','
                sql_final = sql_left[:sql_left.rfind(',')]
                # print(sql_final)
                self._current_cursor.execute(sql_final, values)
                if self._autocommit:
                    self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            raise e
        return self.get_rowcount()

    def get_fields_and_types(self, table, no_pri=False):
        sql = ' DESC %s ' % table
        res = self.get_dicts(sql)
        props = dict()
        for item in res:
            if no_pri and item['Key'] == 'PRI':
                continue
            type = item['Type']
            pos = type.find('(')
            type = type[:pos if pos != -1 else len(type)]  # pos != -1 and pos or len(type)
            props[item['Field']] = type
        return props

    def get_fields_types_lens(self, table, no_pri=False):
        sql = ' DESC %s ' % table
        res = self.get_dicts(sql)
        props = dict()
        for item in res:
            if no_pri and item['Key'] == 'PRI':
                continue
            props[item['Field']] = item['Type']
        return props

    def get_fields(self, table, no_pri=False):
        props = self.get_fields_types_lens(table, no_pri)
        return [k for k, v in props.items()]

    def show_create_table(self, table):
        sql = ' SHOW CREATE TABLE %s ' % table
        res = self.get_dicts(sql)
        return res[0]['Create Table'] if res else False

    def create_table_like(self, target, origin):
        return self.execute("CREATE TABLE IF NOT EXISTS `{}` LIKE `{}`".format(target, origin))

    def load_data_infile(self, path, table, ignore_row=0, charset='utf8mb4', local=False, fields_terminated=',', enclosed='"',
                         lines_terminated='\\n', ignore=True
                         ):
        sql = "LOAD{}DATA INFILE '{}'{}INTO TABLE `{}` CHARACTER SET {} " \
              "FIELDS TERMINATED BY '{}' ENCLOSED BY '{}' LINES TERMINATED BY '{}' " \
              "IGNORE {} LINES ".format(' LOCAL ' if local else ' ', path, ' IGNORE ' if ignore else ' ', table,
                                        fields_terminated, charset, enclosed, lines_terminated, ignore_row)
        return self._current_cursor.execute(sql)

    def select_data_outfile(self, sql, path, charset='utf8mb4', fields_terminated=',', enclosed='"',
                            lines_terminated='\\n'):
        sql = sql + " INTO OUTFILE '{}' CHARACTER SET {} FIELDS TERMINATED BY '{}' ENCLOSED BY '{}' " \
                    "LINES TERMINATED BY '{}' ".format(path, charset, fields_terminated, enclosed, lines_terminated)
        return self._current_cursor.execute(sql)


if __name__ == '__main__':
    db_conf = {
        'user': 'vien',
        'passwd': 'vien',
        'host': '127.0.0.1',
        'schema': 'vien',
        'charset': 'utf8mb4'
    }
    with DBHelper(**db_conf) as db:
        print(db.show_variable("max_allowed_packet"))
