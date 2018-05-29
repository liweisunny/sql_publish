# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-

import pymssql
import os
import traceback
import configparser

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


if __name__ == '__main__':
    ms = MsSql('test')
    ms.exec_non_query('''UPDATE dbo.AnalysisReportChart SET ShowRows=4 WHERE AnalysisReportChartId IN(
SELECT AnalysisReportChartId FROM dbo.AnalysisReportChart WHERE AnalysisReportId IN (
SELECT AnalysisReportId FROM dbo.DashboardDetail WHERE DashboardId='7D188AAB-26B2-4D4D-8C42-A4E7178976AD')
)''')
