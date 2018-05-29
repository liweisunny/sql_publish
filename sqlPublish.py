# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-
import os
import sys
from mssqlDataBase import MsSql

# 该路径是10.129.10.169上的脚本文件存放路径
root_path = r'D:\OceanWork\Files\DBScript\Ocean'


def main():
    ''' 主函数'''
    try:
        args = ['D:\\Nuget\\BuildScripts\\sql_publish\\sqlPublish.py', '20180515\\v2', 'Build', 'For', 'local']
        print('The program starts running and gets the '
              'input parameters of the user:{args}\n'.format(args=args))
        if len(args) == 5:
            path = args[1]
            server_mark = args[4]
            sql_path = os.path.join(root_path, path)
            ms = MsSql(server_mark)
            ms.check_sql_file(sql_path)
        else:
            raise ValueError('Enter the path of the script to execute!!')
    except IndexError:
        raise IndexError('the input Parameters is illegal.')

if __name__ == '__main__':
    main()

