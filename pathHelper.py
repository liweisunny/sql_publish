#__author:liwei
#date:2018/7/13

# -*- coding: utf-8 -*-

import os

execute_path = ['v2', 'dw', 'dwlog']  # 按目前规约只执行这三个目录下及其子目录下的脚本


def get_last_dir(path):
    '''获取path目录下最后一个纯数字目录,并返回要执行的脚本文件所在目录'''
    folder_names = os.listdir(path)
    folder_names.reverse()
    for folder_name in folder_names:
        if folder_name.isdigit():
            return os.path.join(path, folder_name)


def get_sql_paths(parent_path, path_lst=[], temp_lst=[]):

    ''' 获取当前目录下的所有包含脚本文件的子目录'''
    global execute_path
    if not os.path.exists(parent_path):
        raise ValueError("Path:'{path}'does not "
                         "exist!!".format(path=parent_path))
    son_paths = [path for path in os.listdir(parent_path) if
                 os.path.isdir(os.path.join(parent_path, path))
                 and (path.lower() in execute_path or not execute_path)]
    for path_item in son_paths:
        path = os.path.join(parent_path, path_item)
        files = [file for file in os.listdir(path) if file.endswith('.sql')]
        if not files:  # 判断当前目录下是否直接包含脚本文件
            temp_lst.append(path)
        path_lst.append(path)
        execute_path = []
        get_sql_paths(path, path_lst, temp_lst)

    path_lst = [item for item in path_lst
                if item not in temp_lst]  # 去除掉不包含脚本文件的目录
    files = [file for file in os.listdir(parent_path) if file.endswith('.sql')]
    if files:  # 判断用户输入的顶级目录是否存在脚本文件
        path_lst.insert(0, parent_path)
    return path_lst