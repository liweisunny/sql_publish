# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-
import os
import sys
from mssqlDataBase import MsSql

# 该路径是10.129.10.169上的脚本文件存放路径
root_path = r'D:\WorkSpace\DBScript\DBScript\Ocean'


def main():
    ''' 主函数'''

    try:
        args = sys.argv
        print('The program starts running and gets the '
              'input parameters of the user:{args}'.format(args=args))
        if len(args) == 5:
            path = args[1]
            server_mark = args[4]
            sql_path = os.path.join(root_path, path)
            ms = MsSql(server_mark)
            ms.execSqlScripts(sql_path)
        else:
            raise ValueError('Enter the path of the script to execute!!')
    except IndexError:
        raise IndexError('the input Parameters is illegal.')

if __name__ == '__main__':
    main()

