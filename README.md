# 基于Java实现简易MySQL

sql语法格式：
```
<begin statement>
begin [isolation level (read committed|repeatable read)]
begin isolation level read committed

<commit statement>
commit

<rollback statement>
rollback

<create statement>
create table <table name>
<field name> <field type>
<field name> <field type>
...
<field name> <field type>
[(index <field name list>)]
create table students
id int32,
name string,
age int32,
(index id name)

<drop statement>
drop table <table name>
drop table students

<select statement>
select (*|<field name list>) from <table name> [<where statement>]
select * from student where id = 1
select name from student where id > 1 and id < 4
select name, age, id from student where id = 12

<insert statement>
insert into <table name> values <value list>
insert into student values 5 "Xiao Mao" 22

<delete statement>
delete from <table name> <where statement>
delete from student where name = "Xiao Mao"

<update statement>
update <table name> set <field name>=<value> [<where statement>]
update student set name = "MXP" where id = 5

<where statement>
where <field name> (>|<|=) <value> [(and|or) <field name> (>|<|=) <value>]
where age > 10 or age < 3

<field name> <table name>
[a-zA-Z][a-zA-Z0-9_]*

<field type>
int32 int64 string

<value>
.*
```

启动方式：

1.启动服务端`com.mxp.mdb.backend.server.Server`<br>
2.启动客户端`com.mxp.mdb.client.Launcher`<br>
3.在客户端命令行输入并执行SQL语句，可得到执行结果<br>
