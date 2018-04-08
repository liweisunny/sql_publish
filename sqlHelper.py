# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-
import os
import chardet
import re


def getSqlPathLst(parent_path, path_lst=[], temp_lst=[]):

    ''' 获取当前目录下的所以包含脚本文件的子目录'''
    if not os.path.exists(parent_path):
        raise ValueError("Path:'{path}'does not "
                         "exist!!".format(path=parent_path))
    son_paths = [path for path in os.listdir(parent_path) if
                 os.path.isdir(os.path.join(parent_path, path))
                 and path != '.git']  # 获取所有一级子目录

    for path_item in son_paths:
        path = os.path.join(parent_path, path_item)
        files = [file for file in os.listdir(path) if file.endswith('.sql')]
        if not files:  # 判断当前目录下是否直接包含脚本文件
            temp_lst.append(path)
        path_lst.append(path)
        getSqlPathLst(path, path_lst, temp_lst)

    path_lst = [item for item in path_lst
                if item not in temp_lst]  # 去除掉不包含脚本文件的目录
    files = [file for file in os.listdir(parent_path) if file.endswith('.sql')]
    if files:  # 判断用户输入的顶级目录是否存在脚本文件
        path_lst.insert(0, parent_path)
    return path_lst


def checkSqlInfo(sql_paths):
    '''
     校验脚本是否合法
     步骤：
     1.读取文件内容，转码为utf-8格式；
     2.校验脚本前四行是否存在use语句；
     3.检查是否写死库名；
     4.检查是否只存在一个操作（create、alter）存储过程的逻辑；
     5.检查是否只存在一个操作（create、alter、drop）表的逻辑
    '''
    sql_lst = [file for file in os.listdir(sql_paths) if file.endswith('.sql')]
    check_result = True
    fail_count = 0
    print("Path '{path}' begins to review,total:"
          "{num} a scripts!!".format(path=sql_paths, num=len(sql_lst)))
    for sql_file in sql_lst:
        sql_path = os.path.join(sql_paths, sql_file)
        sql_info_lst = getSqlInfo(sql_path)
        error_msg = []
        # 脚本内容校验
        db_name = checkUse(sql_info_lst, error_msg)
        checkDbName(sql_info_lst, db_name, error_msg)
        checkProc(sql_info_lst,  error_msg)
        checkTable(sql_info_lst, error_msg)
        if error_msg:
            check_result = False
            fail_count += 1
            print("The '{sql_name}'check failed, the "
                  "details are as follows:".format(sql_name=sql_file))
            for i, msg in enumerate(error_msg, 1):
                print('{num}.{msg}'.format(num=i, msg=msg))
    if check_result:
        print("All scripts under path "
              "'{path}' are approved and will "
              "be executed.!!".format(path=sql_paths, num=fail_count))
    else:
        print("Path: '{path}' the script "
              "audit ends, total: {num} "
              "a failure.!!".format(path=sql_paths, num=fail_count))
    return check_result


def getSqlInfo(sql_path):
    ''' 读取脚本内容，并以列表的形式返回'''
    with open(sql_path, 'rb')as f:
        sql_content = f.read()
        # 转码逻辑
        code_style = chardet.detect(sql_content).get('encoding')
        f.seek(0, os.SEEK_SET)
        if code_style == 'UTF-16LE':
            sql_content = sql_content.decode('utf16', 'ignore').encode('utf8')
        else:
            sql_content = sql_content.decode(code_style).encode('utf8')
        # 处理换行符
        sql_info = str(sql_content, encoding='utf-8')
        if '\r\n' in sql_info:
            sql_lst = sql_info.split('\r\n')
        else:
            sql_lst = sql_info.split('\n')
    return sql_lst


def checkUse(sql_arry, error_msg):

    ''' 校验脚本前四行是否存在use语句 '''
    pattern_use = r"\s*use\s+.*"
    if len(sql_arry) >= 4:
        sql_arry = sql_arry[0:5]

    # 检查脚本前4行是否有use库名
    for sql_info in sql_arry:
        use_db = re.match(pattern_use, sql_info, re.I)
        if use_db:
            db_name = use_db.group().split()[1]\
                .replace('[', '').replace(']', '')
            return db_name
        error_msg.append('The first four lines have no use statements.!!')


def checkDbName(sql_arry, db_name, error_msg):
    db_name_lines = []
    ''' 校验脚本是否写死库名 '''
    for i, sql_info in enumerate(sql_arry, 1):
        if re.match(r".*%s.*" % db_name, sql_info, re.I):
            db_name_lines.append(i)
    if len(db_name_lines) > 1:
        db_name_lines.pop(0)
        error_msg.append("The dbName to write die，"
                         "At line {lines}!!".format(lines=db_name_lines))


def checkProc(sql_arry, error_msg):

    '''检查是否只存在一个操作（create、alter）存储过程的逻辑'''
    pattern_create_procedure = r'\s*create\s+(procedure|proc)\s+.*'
    pattern_alter_procedure = r'\s*alter\s+(procedure|proc)\s+.*'
    proc_create_num = 0
    proc_alter_num = 0
    proc_name = []
    for i, sql_info in enumerate(sql_arry, 1):
        create_proc_re = re.match(pattern_create_procedure, sql_info, re.I)
        if create_proc_re:
            proc_create_num += 1
            proc_name.append(create_proc_re.group().split()[2])

        alert_proc_re = re.match(pattern_alter_procedure, sql_info, re.I)
        if alert_proc_re:
            proc_alter_num += 1
            proc_name.append(alert_proc_re.group().split()[2])
    if proc_create_num >= 2:
        error_msg.append("There is {num} create "
                         "stored procedure.".format(num=proc_create_num))
    if proc_create_num > 0 and proc_alter_num >=\
            1 and len(set(proc_name)) != 1:
        error_msg.append("There are multiple alter or create "
                         "operations on the stored procedure,and "
                         "not the same stored procedure.")


def checkTable(sql_arry, error_msg):

    ''' 检查是否只存在一个操作（create、alter、drop）表的逻辑'''
    pattern_create_table = r'\s*create\s+table\s+.*'
    pattern_drop_table = r'\s*drop\s+table\s+.*'
    pattern_alter_table = r'\s*alter\s+table\s+.*'
    create_table_name = []
    drop_table_name = []
    alter_table_name = []
    create_table_num = 0
    drop_table_num = 0
    alter_table_num = 0
    for sql_info in sql_arry:
        # 创建表的逻辑审核
        create_table_re = re.match(pattern_create_table, sql_info, re.I)
        if create_table_re:
            create_table_name.append(create_table_re.group()
                                     .replace('(', ' ').split()[2]
                                     .replace('[', '').replace(']', '')
                                     .replace('dbo.', ''))
            create_table_num += 1
        # 删除表的逻辑审核
        drop_table_re = re.match(pattern_drop_table, sql_info, re.I)
        if drop_table_re:
            drop_table_name.append(drop_table_re.group()
                                   .split()[2].replace('[', '')
                                   .replace(']', '')
                                   .replace('dbo.', ''))
            drop_table_num += 1
        # alter表的逻辑审核
        alter_table_re = re.match(pattern_alter_table, sql_info, re.I)
        if alter_table_re:
            alter_table_name.append(alter_table_re.group()
                                    .split()[2].replace('[', '')
                                    .replace(']', '')
                                    .replace('dbo.', ''))
            alter_table_num += 1

    if create_table_num >= 2:
        error_msg.append("There is the create "
                         "operation for the table "
                         "with {num}!!".format(num=create_table_num))

    if create_table_num > 0 and alter_table_num >\
            0 and set(create_table_name) != set(alter_table_name):
        error_msg.append("There are also table "
                         "operations for alter and create,"
                         " and not the same table for operations!!")

    if drop_table_num > 0 and create_table_num >\
            0 and set(create_table_name) != set(drop_table_name):
            error_msg.append("There is also a table "
                             "operation of drop and create, "
                             "not the same table as the operation!!")

    if alter_table_num >= 2 and len(set(alter_table_name)) != 1:
        error_msg.append("There are multiple alter "
                         "table operations, and not the same table.!!")
