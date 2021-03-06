# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-

import pymssql
import os
import traceback
import configparser
from sqlHelper import getSqlPathLst, checkSqlInfo


class MsSql:
    '''
    sqlserver数据库操作实体类
    '''
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls,  '_instance'):
            cls._instance = super(MsSql, cls).__new__(cls)
        return cls._instance

    def __init__(self, server_mark):
        self.server_mark = server_mark
        self.log_file = server_mark
        self.__initDbInfo()

    def __initDbInfo(self):
        '''加载配置文件，初始化数据库信息'''
        config_path = os.path.join(os.path.dirname(__file__),
                                   'config/dbConfig.config')
        cf = configparser.ConfigParser()
        cf.read(config_path)
        self.host = cf.get(self.server_mark, 'db_host')
        self.db = cf.get(self.server_mark, 'db_name')
        self.user = cf.get(self.server_mark, 'db_user')
        self.pwd = cf.get(self.server_mark, 'db_pwd')

    def __getConnect(self):

        ''' 构造数据库链接 '''
        self.conn = pymssql.connect(host=self.host, user=self.user,
                                    password=self.pwd, database=self.db,
                                    charset="utf8")
        cur = self.conn.cursor()
        return cur

    def execQuery(self, sql):

        ''' 执行sql语句并返回查询结果'''
        try:
            cur = self.__getConnect()
            cur.execute(sql)
            self.conn.commit()
            cur.nextset()  # 要加上这句才能通过fetch函数取到值
            resList = cur.fetchone()
            self.conn.close()
            return resList
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.conn.close()

    def execNonQuery(self, sql):

        ''' 执行sql语句(insert、updte、delete)'''
        try:
            cur = self.__getConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
            return True
        except Exception as e:
            print(e)
            traceback.print_exc()
            return False
        finally:
            self.conn.close()

    def execSqlScripts(self, sql_path):
        ''' 检查并执行脚本文件'''
        sql_paths = getSqlPathLst(sql_path)
        for path_item in sql_paths:
            check_result = checkSqlInfo(path_item)
            if check_result:
                if self.server_mark != 'check':
                    sql_lst = [file for file in
                               os.listdir(path_item) if file.endswith('.sql')]
                    for sql_item in sql_lst:
                        sql_path = os.path.join(path_item, sql_item).lower()
                        log_path = os.path.join(os.path.dirname(__file__)
                                                + '/runLog', self.log_file)
                        with open(log_path, 'r') as f:
                            lines = map(lambda line: line.strip('\n'), f)
                            if sql_path in lines and sql_item[0:2] in ['3_', '3-']:  # 修复数据的脚本只能执行一次
                                continue
                        with open(log_path, 'a') as f:
                            cmd = r"sqlcmd -S " + self.host + " -U " \
                                  + self.user + " -P " + self.pwd + " -d " \
                                  + self.db + " -i " + sql_path + ' -b '
                            exec_result = os.system(cmd)  # 0表示成功，1表示失败
                            if not exec_result:
                                f.write(sql_path + '\n')
                            else:
                                raise SystemError("'{sql}On failure'"
                                                  "".format(sql=sql_path))
            else:
                raise ValueError("The script for path '{path}'"
                                 " does not pass.".format(path=path_item))


if __name__ == '__main__':
    ms = MsSql('innerDev')
    path = r'D:\OceanWork\Api'
    ms.execSqlScripts(path)
