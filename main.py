# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-
import os, sys
from sqlHelper import check_sql_to_execute
from pathHelper import get_last_dir, get_sql_paths

# 该路径是10.129.10.169上的脚本文件存放路径
root_path = r'D:\OceanWork\Files\DBScript\Ocean'


def main():
    try:
        args = ['D:\\Nuget\\BuildScripts\\sql_publish\\main.py', 'Publish', 'To', 'check']  # sys.argv
        print("The program starts running and gets the "
              "input parameters of the user:'{args}'\n".format(args=args))
        if len(args) == 5:
            path = os.path.join(root_path, args[5])
        else:
            path = get_last_dir(root_path)
        print("Start checking all the scripts under "
              "the execution path: '{path}'\n".format(path=path))
        server_mark = args[3]
        sql_path_lst = get_sql_paths(path)  # 获取所有要执行的脚本路径
        check_sql_to_execute(sql_path_lst, server_mark)  # 检查SQL并执行
    except IndexError:
        raise IndexError('the input Parameters is illegal.')

if __name__ == '__main__':
    main()

