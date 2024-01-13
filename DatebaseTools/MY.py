#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Time ： 2024/1/11 0011 15:14
@Auth ： Xq
@File ：MY.py
@IDE ：PyCharm
'''
from typing import Union

import pandas as pd
import pymysql
from LogInfo.LogTools import Logger
from ConfigTools.DataBaseConfig import mysql_config


class MySqlCurd(object):
    """
    mysql 增删改查
    """

    def __init__(self):
        self.log = Logger()
        self.config = mysql_config()
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.user = self.config['username']
        self.password = self.config['password']
        self.db = self.config['database']
        self.connection = self.connect()

    def connect(self):
        '''
        连接MySQL数据库
        :return: 
        '''
        try:
            connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db
            )
            self.log.info(F'成功连接数据库:{self.db}')
            return connection
        except Exception as e:
            self.log.error(F'数据库连接失败:{self.db}')
            raise F'数据库连接失败：{e}'

    def close(self):
        """
        关闭MySQL数据库连接
        :return:
        """

        if self.connection is not None:
            self.connection.close()
            self.log.info(F'关闭数据库{self.db}')

    def create_table_name_exit_delete(self, table: str, create_sql: str) -> str:
        """
        创建表,存在删除
        :param table: 表名称
        :param create_sql:
        :return:
        """

        try:
            with self.connection.cursor() as cursor:
                # 检查student表是否已存在
                check_table_query = F'SHOW TABLES LIKE "{table}"'
                cursor.execute(check_table_query)
                exists = cursor.fetchone() is not None
                if exists:
                    drop_table_query = F'DROP TABLE {table}'
                    cursor.execute(drop_table_query)
                    self.connection.commit()
                    self.log.warning(F'创建表但是表存在,已删除')
                cursor.execute(create_sql)
                self.connection.commit()
                # self.close()
                self.log.info(F'已创建:{table}')
                return F'已创建:{table}'
        except Exception as e:
            self.log.error(F'创建表失败:{e}')
            raise F'创建表失败:{e}'

    def insert_list(self, table: str, fields: list[str], data_list: list[dict]) -> str:
        """
        添加一条数据
        :param table: 表名称
        :param fields:  表头
        :param data_list:  表数据
        :return: 
        """
        try:
            with self.connection.cursor() as cursor:
                for record in data_list:
                    # 构建 VALUES 部分的参数元组
                    values_tuple = tuple(record[col] for col in fields)
                    # 构建 INSERT INTO SQL 语句
                    sql = f'INSERT INTO {table} ({", ".join(fields)}) VALUES ({", ".join(["%s" for _ in fields])})'
                    cursor.execute(sql, values_tuple)
                self.connection.commit()
                # self.close()
                self.log.info(F'{table}:添加一条数据')
                return F'{table}:添加一条数据'
        except Exception as e:
            self.log.error(F'添加一条数据有误:{e}')
            raise F'{e}'

    def delete_one(self, table: str, data: dict[str, str]) -> str:
        """
        删除一条数据
        :param table: 表名称
        :param data:  删除的数据
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                sql_last = F'DELETE FROM {table} WHERE'
                for k, v in data.items():
                    sql_last = sql_last + F" {k} = '{v}' AND "
                sql_last = sql_last[0:-4]
                cursor.execute(sql_last)
                self.connection.commit()
                self.close()
                self.log.info(F'{table}:删除一条数据')
                return F'{table}:删除一条数据'
        except Exception as e:
            self.log.error(F'请求数据有误:{e}')
            raise F'{e}'

    def delete_list(self, table: str, data_list: list[dict]) -> str:
        """
        删除多条数据
        注意:
            删除为空的数据,也显示删除为一条数据
        :param table: 表名称
        :param data_list: 删除的数据
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                for params in data_list:
                    where_conditions = []
                    for key, value in params.items():
                        where_conditions.append(f"{key} = %({key})s")
                    delete_query = f"DELETE FROM {table} WHERE {' AND '.join(where_conditions)}"
                    cursor.execute(delete_query, params)
                self.connection.commit()
                self.close()
                self.log.info(F'{table}:删除{len(data_list)}条数据')
                return F'{table}:删除{len(data_list)}条数据'
        except Exception as e:
            self.log.error(F'请求数据有误:{e}')
            raise F'{e}'

    def update_list(self, table: str, primary_key: dict, fields_update_list: list[Union[set, set]]) -> str:
        """
        查询数据fields_update_list
        :param table: 表名称
        :param primary_key: 主键
        :param fields_update_list:  请求的数据
        :return:
        """
        try:
            results_list = []

            with self.connection.cursor() as cursor:

                # 初始化SQL更新部分和参数列表
                update_clause_parts = []
                params = []

                for field_dict in fields_update_list:
                    for field_name, field_value in field_dict.items():
                        update_clause_parts.append(f"{field_name} = %s")
                        params.append(field_value)

                # 构建完整的更新SQL语句（包括WHERE子句）
                update_clause = ', '.join(update_clause_parts)
                where_clause = "WHERE id = %s"
                params.append(primary_key[list(primary_key.keys())[0]])

                update_sql = f"UPDATE student SET {update_clause} {where_clause}"

                # 假设有一个已连接并初始化的游标（cursor）对象
                num = cursor.execute(update_sql, tuple(params))
                if num:
                    results_list.append(num)
            self.connection.commit()
            self.close()
            self.log.info(F'{table}:更新{len(results_list)}条数据')
            return F'{table}:更新{len(results_list)}条数据'
        except Exception as e:
            self.log.error(F'更新数据有误:{e}')
            raise F'{e}'

    def query_list(self, table: str, data_list: list[Union[dict, dict]]) -> list:
        """
        查询数据
        :param table: 表名称
        :param data_list:  请求的数据
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                results_list = []
                for query_dict in data_list:
                    where_conditions = ['{}=%s'.format(key) for key in query_dict.keys()]
                    where_clause = ' AND '.join(where_conditions)
                    sql = f"SELECT * FROM student WHERE {where_clause}"
                    params = tuple(query_dict.values())
                    cursor.execute(sql, params)
                    result_set = cursor.fetchone()
                    if result_set is not None:
                        results_list.append(result_set)
            results_list = [{key: value} for key, value in results_list]
            self.connection.commit()
            self.close()
            self.log.info(F'{table}:查询{len(results_list)}条数据')
            return results_list
        except Exception as e:
            self.log.error(F'查询数据有误:{e}')
            raise F'{e}'

    def query_return_header_and_data_pd(self, pdsql: str) -> any:
        """
        sql查询数据,返回df带字段名称
        :param pdsql: sql 查询语句
        :return: df
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(pdsql)
                daily_data = cursor.fetchall()
                self.close()
                df = pd.DataFrame(daily_data, columns=[i[0] for i in cursor.description])
                self.log.info(F'查询数据,返回df带字段名称')
                return df
        except Exception as e:
            self.log.info(F"请求数据有误:{e}")
            raise F'{e}'


def create_table_name_exit_delete_demo():
    create_sql = F"""
    CREATE TABLE student (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        age INT
                        );
    """
    MC.create_table_name_exit_delete(table=table, create_sql=create_sql)


def insert_list_demo():
    fields = ['id', 'age']
    data_list = [{'id': 1, 'age': 2}, {'id': 2, 'age': 2}]
    MC.insert_list(table=table, fields=fields, data_list=data_list)


def delete_list_demo():
    data_dict_one = {'id': 1}
    data_dict_two = {'id': 2}
    data_list = [data_dict_one, data_dict_two]
    MC.delete_list(table=table, data_list=data_list)


def query_return_header_and_data_pd_demo():
    pdsql = F'select * from student'
    print(MC.query_return_header_and_data_pd(pdsql=pdsql))


def query_list_demo():
    query_one = {'id': 1, 'age': 2}
    query_two = {'id': 2}
    data_list = [query_one, query_two]
    print(MC.query_list(table=table, data_list=data_list))


def update_list_demo():
    primary_key = {'id': 1}
    fields_update_list = [{'age': 1}]
    print(list(primary_key.keys())[0])
    MC.update_list(table=table, primary_key=primary_key, fields_update_list=fields_update_list)


if __name__ == '__main__':
    MC = MySqlCurd()
    # print(MC.config)
    # MC.connect()
    table = 'student'
    # create_table_name_exit_delete_demo() # 创建表
    # insert_list_demo() # 添加数据
    # delete_list_demo() # 删除数据
    # query_return_header_and_data_pd_demo()  # 返回pd
    # query_list_demo()  # 查询数据
    # update_list_demo() # 更新数据
