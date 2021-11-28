create schema schema;
use schema;
create table test(id int primary key, name varchar(20),age int);
insert into test values(1,'张三',11);
insert into test values(2,'lisi',12);
insert into test values(3,'wangwu',12);

update test set name='zs', age=15 where age<12;

update test set name='erh' where id=1 or age between 20 and 21;

delete from test where age > 20;

alter table test add birth date;
update test set birth='2021-10-11';
update test set name='哈哈' where id=3;

select * from schema.test t1 left join schema.test t2 on t1.id=t2.id;

-- select age, string_agg(name, ',') from test group by age;