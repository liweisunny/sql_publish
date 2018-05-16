# author:liwei
# date:2018/3/27
# -*- coding: utf-8 -*-
import os
import chardet
import re
import copy

db_name = ''
excute_path = ['v2', 'dw', 'dwlog']  # 按目前规约只执行这三个目录下及其子目录下的脚本


def get_sql_paths(parent_path, path_lst=[], temp_lst=[]):

    ''' 获取当前目录下的所以包含脚本文件的子目录'''
    global excute_path
    if not os.path.exists(parent_path):
        raise ValueError("Path:'{path}'does not "
                         "exist!!".format(path=parent_path))
    son_paths = [path for path in os.listdir(parent_path) if
                 os.path.isdir(os.path.join(parent_path, path))
                 and (path.lower() in excute_path or not excute_path)]
    for path_item in son_paths:
        path = os.path.join(parent_path, path_item)
        files = [file for file in os.listdir(path) if file.endswith('.sql')]
        if not files:  # 判断当前目录下是否直接包含脚本文件
            temp_lst.append(path)
        path_lst.append(path)
        excute_path = []
        get_sql_paths(path, path_lst, temp_lst)

    path_lst = [item for item in path_lst
                if item not in temp_lst]  # 去除掉不包含脚本文件的目录
    files = [file for file in os.listdir(parent_path) if file.endswith('.sql')]
    if files:  # 判断用户输入的顶级目录是否存在脚本文件
        path_lst.insert(0, parent_path)
    return path_lst


def get_sql_info(sql_path):
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


def check_sql_info(sql_paths):
    '''
     校验脚本是否合法
     步骤：
     1.读取文件内容，转码为utf-8格式；
     2.检查脚本前4行是否有use库名以及当前目录下的脚本是否都是一个数据库下的 ；
     3.校验脚本是否可重复执行；
     4.检查是否写死库名；
     5.检查是否只存在一个操作（create、alter）存储过程的逻辑；
     6.检查是否只存在一个操作（create、alter、drop）表的逻辑
    '''
    global db_name
    db_name = ''
    sql_lst = [file for file in os.listdir(sql_paths) if file.endswith('.sql')]
    check_result = True
    fail_count = 0
    print("Path '{path}' begins to review,total:"
          "{num}  scripts!!\n".format(path=sql_paths, num=len(sql_lst)))
    for sql_file in sql_lst:
        sql_path = os.path.join(sql_paths, sql_file)
        sql_info_lst = get_sql_info(sql_path)
        error_msg = []
        # 脚本内容校验
        check_use(sql_info_lst, error_msg)
        check_repeat(sql_info_lst, error_msg)
        check_db_name(sql_info_lst, error_msg)
        check_procedure(sql_info_lst,  error_msg)
        check_table(sql_info_lst, error_msg)
        if error_msg:
            check_result = False
            fail_count += 1
            print("The '{sql_name}'check failed, the "
                  "details are as follows:\n".format(sql_name=sql_file))
            for i, msg in enumerate(error_msg, 1):
                print('   {num}.{msg}\n'.format(num=i, msg=msg))
            print('---------------------------------------------\n')
    if check_result:
        print("All scripts under path "
              "'{path}' are approved and will "
              "be executed.!!\n".format(path=sql_paths, num=fail_count))
    else:
        print("Path: '{path}' the script "
              "audit ends, total: {num} "
              "a failure.!!\n".format(path=sql_paths, num=fail_count))
    return check_result


def check_use(sql_info_lst, error_msg):
    ''' 检查脚本前4行是否有use库名以及当前目录下的脚本是否都是一个数据库下的 '''

    global db_name
    pattern_use = r"\s*use\s+.*"
    if len(sql_info_lst) >= 4:
        sql_info_lst = sql_info_lst[0:5]
    for sql_info in sql_info_lst:
        use_db = re.match(pattern_use, sql_info, re.I)
        if use_db:
            new_db_name = use_db.group().split()[1] \
                .replace('[', '').replace(']', '').replace(';', '').replace(' ', '')
            if db_name:
                if db_name != new_db_name:
                    error_msg.append("database name error.!!")
                return
            else:
                db_name = new_db_name
                if db_name:
                    return
    error_msg.append('The first four lines have no use statements.!!')


def check_repeat(sql_info_lst, error_msg):
    pattern_create_procedure = r'[\s\S]*create\s+(procedure|proc)\s+[\s\S]*'
    pattern_drop_procedure = r'[\s\S]*drop\s+(procedure|proc)\s+[\s\S]*'
    pattern_create_table = r'[\s\S]*create\s+table\s+[\s\S]*'
    pattern_drop_table = r'[\s\S]*drop\s+table\s+[\s\S]*'
    pattern_create_column = r'[\s\S]*add\s+column\s+[\s\S]*'
    pattern_drop_column = r'[\s\S]*drop\s+column\s+[\s\S]*'
    pattern_create_type = r'[\s\S]*create\s+type\s+[\s\S]*'
    pattern_drop_type = r'[\s\S]*drop\s+type\s+[\s\S]*'
    sql_info = ' '.join(sql_info_lst)

    msg = "The script does not support repeated execution, please modify.!!"
    if re.match(pattern_create_procedure, sql_info, re.I) and not re.match(pattern_drop_procedure, sql_info, re.I):
        error_msg.append(msg)
        return
    if re.match(pattern_create_table, sql_info, re.I) and not re.match(pattern_drop_table, sql_info, re.I):
        error_msg.append(msg)
        return
    if re.match(pattern_create_column, sql_info, re.I) and not re.match(pattern_drop_column, sql_info, re.I):
        error_msg.append(msg)
        return
    if re.match(pattern_create_type, sql_info, re.I) and not re.match(pattern_drop_type, sql_info, re.I):
        error_msg.append(msg)
        return


def check_db_name(sql_info_lst, error_msg):
    db_name_lines = []
    ''' 校验脚本是否写死库名 '''
    for i, sql_info in enumerate(sql_info_lst, 1):
        if re.match(r".*%s.*" % db_name, sql_info, re.I):
            db_name_lines.append(i)
    if len(db_name_lines) > 1:
        db_name_lines.pop(0)
        error_msg.append("The dbName to write die，"
                         "At line {lines}!!".format(lines=db_name_lines))


def check_procedure(sql_info_lst, error_msg):

    '''检查是否只存在一个操作（create、alter）存储过程的逻辑'''
    pattern_create_procedure = r'\s*create\s+(procedure|proc)\s+.*'
    pattern_alter_procedure = r'\s*alter\s+(procedure|proc)\s+.*'
    procedure_create_num = 0
    procedure_alter_num = 0
    procedure_name = []
    for i, sql_info in enumerate(sql_info_lst, 1):
        create_procedure_re = re.match(pattern_create_procedure, sql_info, re.I)
        if create_procedure_re:
            procedure_create_num += 1
            procedure_name.append(create_procedure_re.group().split()[2])

        alert_procedure_re = re.match(pattern_alter_procedure, sql_info, re.I)
        if alert_procedure_re:
            procedure_alter_num += 1
            procedure_name.append(alert_procedure_re.group().split()[2])
    if procedure_create_num >= 2:
        error_msg.append("There is {num} create "
                         "stored procedure.".format(num=procedure_create_num))
    if procedure_create_num > 0 and procedure_alter_num >=\
            1 and len(set(procedure_name)) != 1:
        error_msg.append("There are multiple alter or create "
                         "operations on the stored procedure,and "
                         "not the same stored procedure.")


def check_table(sql_info_lst, error_msg):

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
    for sql_info in sql_info_lst:
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
