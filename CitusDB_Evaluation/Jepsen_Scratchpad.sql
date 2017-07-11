select * from public.a order by id;
select * from public.b order by id;
explain select * from public.set where val = 2596;
select * from public.set where val = 2721;

select master_copy_shard_placement(105695, 'citusslave3', 5432, 'citusslave2', 5432);
select * from pg_catalog.pg_dist_shard_placement where shardid = (select get_shard_id_for_distribution_column('set',32)); 
select * from pg_catalog.pg_dist_shard_placement where shardid = 105695;
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




drop table if exists set;
create table if not exists set (val int);
select create_distributed_table('set', 'val');

drop table if exists test;
create table if not exists test (id int, val int);
select create_distributed_table('test', 'id');

drop table accounts;
create table if not exists accounts
                             (id      int not null,
                             balance bigint not null);
select create_distributed_table('accounts', 'id');