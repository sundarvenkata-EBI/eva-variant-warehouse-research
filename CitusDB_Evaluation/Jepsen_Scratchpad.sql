select * from public.a order by id;
select * from public.b order by id;
explain select * from public.set where val = 246;

select * from pg_catalog.pg_dist_shard  where logicalrelid = ; 

select create_distributed_table('a', 'id');
select create_distributed_table('b', 'id');
select create_distributed_table('test', 'id');


drop table a

drop table b if exists accounts

create table if not exists  a (
                              id    int primary key,
                              key   int,
                              value int);

create table if not exists b (
                              id    int primary key,
                              key   int,
                              value int);


create table if not exists test (id int primary key, val
                           int)

drop table set;
create table if not exists set (val int);
select create_distributed_table('set', 'val');