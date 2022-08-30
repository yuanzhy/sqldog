create schema schema;
use schema;
create table test(id int primary key, name varchar(20),age int, tt time, ts timestamp);
insert into test values(2,'lisi',12, '15:12:11', '2016-01-21 15:12:11');
insert into test values(3,'wangwu',12, '01:12:11', '2015-11-21 01:12:11');
insert into test values(1,'张三',11, '11:12:11', '2012-11-21 11:12:11');

update test set name='zs', age=15 where age<12;

update test set name='erh' where id=1 or age between 20 and 21;

delete from test where age > 20;

alter table test add birth date;
update test set birth='2021-10-11' where id < 5;
update test set name='哈哈' where id=3;

select * from schema.test t1 left join schema.test t2 on t1.id=t2.id;

-- select age, string_agg(name, ',') from test group by age;

create table schema.t2(id1 varchar(20), id2 varchar(20), name varchar(20), age int, constraint PK_t2 primary key( id1, id2 ));
insert into schema.t2 values('1000','1', 'zhangsan', 12);
insert into schema.t2 values('1000','2', 'lisi', 13);
insert into schema.t2 values('1000','3', 'wangwu', 16);
insert into schema.t2 values('1001','1', 'zhangliu', 16);
insert into schema.t2 values('1001','2', 'zhengqi', 26);
insert into schema.t2 values('1002','1', 'niuba', 22);

select * from schema.t2;