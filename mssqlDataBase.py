# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-

import pymssql
import os
import traceback
import configparser
from sqlHelper import get_sql_paths, check_sql_info


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
        self.__init_db_info()

    def __init_db_info(self):
        '''
        加载配置文件，初始化数据库信息
        :return:
        '''
        config_path = os.path.join(os.path.dirname(__file__),
                                   'config/dbConfig.config')
        cf = configparser.ConfigParser()
        cf.read(config_path)
        self.host = cf.get(self.server_mark, 'db_host')
        self.db = cf.get(self.server_mark, 'db_name')
        self.user = cf.get(self.server_mark, 'db_user')
        self.pwd = cf.get(self.server_mark, 'db_pwd')

    def __get_connect(self):

        '''构造数据库链接'''
        self.conn = pymssql.connect(host=self.host, user=self.user,
                                    password=self.pwd, database=self.db,
                                    charset="utf8")
        cur = self.conn.cursor()
        return cur

    def exec_query(self, sql):

        ''' 执行sql语句并返回查询结果'''
        try:
            cur = self.__get_connect()
            cur.execute(sql)
            self.conn.commit()
            cur.nextset()  # 要加上这句才能通过fetch函数取到值
            res_list = cur.fetchone()
            self.conn.close()
            return res_list
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.conn.close()

    def exec_non_query(self, sql):

        ''' 执行sql语句(insert、updte、delete)'''
        try:
            cur = self.__get_connect()
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

    def exec_sql_file(self,sql_path):
        cmd = r"sqlcmd -S " + self.host + " -U " \
              + self.user + " -P " + self.pwd + " -d " \
              + self.db + " -i " + sql_path + ' -b '
        exec_result = os.system(cmd)  # 0表示成功，1表示失败
        return exec_result

    def check_sql_file(self, sql_path):

        ''' 检查并执行脚本文件'''
        sql_path_lst = get_sql_paths(sql_path)
        for path_item in sql_path_lst:
            check_result = check_sql_info(path_item)
            if check_result:  # 当前路径下的脚本全部校验成功后开始执行脚本
                if self.server_mark != 'check':
                    sql_lst = [file for file in
                               os.listdir(path_item) if file.endswith('.sql')]
                    for sql_item in sql_lst:
                        sql_path = os.path.join(path_item, sql_item).lower()
                        log_path = os.path.join(os.path.dirname(__file__)
                                                + '/executeLog', self.log_file)
                        with open(log_path, 'r') as f:
                            lines = map(lambda line: line.strip('\n'), f)
                            if sql_path in lines:  # 当前脚本执行成功过不在执行
                                continue
                        with open(log_path, 'a') as f:
                            exec_result = self.exec_sql_file(sql_path)
                            if not exec_result:
                                f.write(sql_path + '\n')  # 脚本执行成功记录日志
                            else:
                                raise SystemError("'{sql}On failure'"
                                                  "".format(sql=sql_path))  # 脚本执行失败抛出异常
            else:
                raise ValueError("The script for path '{path}'"
                                 " does not pass.".format(path=path_item))




if __name__ == '__main__':
    ms = MsSql('test')
    ms.exec_non_query('''UPDATE dbo.AnalysisReportChart SET ShowRows=4 WHERE AnalysisReportChartId IN(
SELECT AnalysisReportChartId FROM dbo.AnalysisReportChart WHERE AnalysisReportId IN (
SELECT AnalysisReportId FROM dbo.DashboardDetail WHERE DashboardId='7D188AAB-26B2-4D4D-8C42-A4E7178976AD')
)''')
