#! /usr/bin/env python
# -*- coding: utf-8 -*-
'''
@Time ： 2024/1/11 0011 15:14
@Auth ： Xq
@File ：MY.py
@IDE ：PyCharm
'''
from typing import List, Union

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

    def create_table_name_exit_delete(self, table: str, data: str) -> str:
        """
        创建表,存在则啥拿出
        :param table: 表名称
        :param data:
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
                cursor.execute(data)
                self.connection.commit()
                self.close()
                self.log.info(F'已创建:{table}')
                return F'已创建:{table}'
        except Exception as e:
            self.log.error(F'创建表失败:{e}')
            raise F'创建表失败:{e}'

    def insert_one(self, table: str, headers: List[str], data: list) -> str:
        """
        添加一条数据
        :param table: 表名称
        :param headers:  表头
        :param data:  表数据
        :return: 
        """
        try:
            with self.connection.cursor() as cursor:
                fields = ','.join(headers)
                values = ','.join(['%s'] * len(data))
                sql = f'INSERT INTO {table}    ({fields}) VALUES ({values})'
                cursor.execute(sql, data)
                self.connection.commit()
                self.close()
                self.log.info(F'{table}:添加一条数据')
                return F'{table}:添加一条数据'
        except Exception as e:
            self.log.error(F'请求数据有误:{e}')
            raise F'{e}'

    def insert_one_exit_replace(self, table: str, headers: List[str], data: list) -> str:
        """
        添加一条数据,存在则替换
        :param table: 表名称
        :param headers:  表头
        :param data:  表数据
        :return:
        """
        try:

            with self.connection.cursor() as cursor:
                fields = ','.join(headers)
                values = ','.join(['%s'] * len(data))
                sql = f'INSERT INTO {table}    ({fields}) VALUES ({values})'
                cursor.execute(sql, data)
                self.connection.commit()
                self.close()
                self.log.info(F'{table}:添加一条数据')
                return F'{table}:添加一条数据'
        except Exception as e:
            self.log.error(F'请求数据有误:{e}')
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

    def delete_mul(self, table: str, data: list[dict]) -> str:
        """
        删除多条数据
        注意:
            删除为空的数据,也显示删除为一条数据
        :param table: 表名称
        :param data: 删除的数据
        :return:
        """
        try:
            with self.connection.cursor() as cursor:
                for params in data:
                    where_conditions = []
                    for key, value in params.items():
                        where_conditions.append(f"{key} = %({key})s")
                    delete_query = f"DELETE FROM {table} WHERE {' AND '.join(where_conditions)}"
                    cursor.execute(delete_query, params)
                self.connection.commit()
                self.close()
                self.log.info(F'{table}:删除{len(data)}条数据')
                return F'{table}:删除{len(data)}条数据'
        except Exception as e:
            self.log.error(F'请求数据有误:{e}')
            raise F'{e}'

    def update_all(self, sql):
        pass

    def query_one(self, table: str, data: list[Union[dict, dict]]) -> str:
        """
        请求一条数据
        :param table: 表名称
        :param data:  请求的数据
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

    def query_sql_return_header_and_data(self, pdsql: str) -> any:
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


if __name__ == '__main__':
    MC = MySqlCurd()
    # print(MC.config)

    # MC.connect()
    table = 'student'
    data = F"""
    CREATE TABLE student (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        age INT
                        );
    """
    MC.create_table_name_exit_delete(table=table, data=data)

    headers = ['id', 'age']
    data = [100, "100"]
    MC.insert_one(table=table, headers=headers, data=data)
    # MC.insert_one_exit_replace(table=table, headers=headers, data=data)

    # data = {'id': 183633, 'age': '1873'}
    # data = {'id': 1836}
    # MC.delete_one(table=table, data=data)
    # field_one = {'id': 1822, 'age': '1111'}
    # field_two = {'id': 186, 'age': '187'}
    # data = [field_one, field_two]
    # MC.delete_mul(table=table, data=data)

    # field_list = {'id': 183633, 'age': '1873'}

    # pdsql = F'select * from student'
    # print(MC.query_sql_return_header_and_data(pdsql=pdsql))
    # query_one = {'id': 1822, 'age': '1111'}
    # query_two = {'id': 1822, 'age': '1111'}
    # data = [query_one, query_two]
    # print(MC.query_one(table=table, data=data))
