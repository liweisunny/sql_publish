# sql_publish

该项目集成jenkins实现sql脚本的自动化执行。

sqlPublish.py:程序启动的入口，通过获取用户在jenkins上选择发布到的服务器信息以及输入的脚本路径信息执行脚本文件。

sqlHelper.py：负责校验脚本文件是否符合规范以及获取用户输入路径的所有子路径。

mssqlDataBase.py:封装了一系列执行sql的操作，负责执行sql文件，sql语句等。

jenkins部署：

新建一个项目，点击配置--->选择参数化构建：
![](https://i.imgur.com/wWoosfv.png)
![](https://i.imgur.com/T5y0ciJ.png)
然后拖到最下面点击增加构建步骤--windows下选择Execute Windows batch command:
![](https://i.imgur.com/CetDpdo.png)
