SELECT run_command_on_workers($cmd$ alter user postgres set citus.shard_replication_factor=3; $cmd$);
SELECT run_command_on_workers($cmd$ show citus.shard_replication_factor; $cmd$);
SELECT run_command_on_workers($cmd$ alter user postgres set citus.shard_max_size='10GB'; $cmd$);
SELECT run_command_on_workers($cmd$ show citus.shard_max_size; $cmd$);
drop table public_1.variant;
create table public_1.variant
(
var_id char(62),
chunk_id varchar(10),
chrom varchar(10),
start_pos bigint,
end_pos bigint,
var_len integer,
var_ref text,
var_alt text,
var_type varchar(10),
ids varchar(100)[],
hgvs jsonb,
ct_so smallint[],
ct_sift_min decimal(18,8),
ct_sift_max decimal(18,8),
ct_pphen_min decimal(18,8),
ct_pphen_max decimal(18,8),
ct_xref text[]
);
select create_distributed_table('public_1.variant', 'chunk_id');

drop table public_1.variant_sample;
create table public_1.variant_sample
(
var_id char(62),
chunk_id varchar(10),
chrom varchar(10),
start_pos bigint,
end_pos bigint,
sample_index smallint,
genotype varchar(5),
tot_num_samp integer,
sample_index_bits bit varying(1000000)
);
select create_distributed_table('public_1.variant_sample', 'chunk_id', colocate_with => 'public_1.variant');

drop table public_1.variant_files;
create table public_1.variant_files
(
var_id char(62),
chunk_id varchar(10),
chrom varchar(10),
start_pos bigint,
end_pos bigint,
sample_index smallint,
fid varchar(50),
sid varchar(50),
attrs jsonb,
fm varchar(10),
src bytea 
);
select create_distributed_table('public_1.variant_files', 'chunk_id', colocate_with => 'public_1.variant');

drop table public_1.variant_annot;
create table public_1.variant_annot
(
var_id char(62),
chunk_id varchar(10),
chrom varchar(10),
start_pos bigint,
end_pos bigint,
ct_index smallint,
gn text,
ensg varchar(30),
enst varchar(30),
codon text,
strand varchar(30),
bt text,
aachange text,
so integer[],
sift_sc decimal(18,10),
sift_desc text,
pphen_sc decimal(18,10),
pphen_desc text
);
select create_distributed_table('public_1.variant_annot', 'chunk_id', colocate_with => 'public_1.variant');

select count(*) from public_1.variant;
select count(*) from public_1.variant_files;
select count(*) from public_1.variant_annot;
select count(*) from public_1.variant_sample;
select * from public_1.variant_annot where sift_sc is not null;
select * from public_1.variant where ct_sift_min is not null and ct_sift_min <> ct_sift_max limit 100;
select * from public_1.variant limit 100;
select * from public_1.variant_files limit 100;
select * from public_1.variant_sample limit 100;

explain
select 
varf.* 
from 
public_1.variant var
inner join public_1.variant_files varf on varf.var_id = var.var_id
where var.chrom = '1' and var.start_pos between 2066706 and 2076706

select master_modify_multiple_shards('delete from public_1.variant');
select master_modify_multiple_shards('delete from public_1.variant_annot');
select master_modify_multiple_shards('delete from public_1.variant_files');
select master_modify_multiple_shards('delete from public_1.variant_sample');
delete from public_1.reg_chrom where chrom <> '10';

select *
  from pg_dist_shard_placement
 where shardid = (
   select get_shard_id_for_distribution_column('public_1.variant', '1_000227471340_000227471340')
 );
select pg_size_pretty(citus_table_size('public_1.variant_annot')); --28GB for 40M records
select pg_size_pretty(citus_table_size('public_1.variant_files')); --31GB for 40M records
select pg_size_pretty(citus_table_size('public_1.variant_sample')); --61GB for 40M records
select pg_size_pretty(citus_table_size('public_1.variant')); --13GB for 40M records
select chrom from public_1.variant group by 1;
select chrom from public_1.ct group by 1;
explain select * from public_1.variant where var_id = '21_000048119868_000048119868';
explain select * from public_1.ct where var_id = '21_000048119868_000048119868';
select count(*) from public_1.variant;
select count(*) from public_1.ct;
select count(*) from public_1.variant where chrom = 'x';
db.variant_chr21_1_1.find({"chr": "21", "start": 9411413, "ref": "t"});
select count(*) from public_1.hgv;
analyze public_1.variant(var_id);
analyze public_1.variant(chrom);
analyze public_1.variant(start_pos);
analyze public_1.variant(end_pos);
analyze public_1.variant(chunk_id);

analyze public_1.variant_files(var_id);
analyze public_1.variant_files(chrom);
analyze public_1.variant_files(start_pos);
analyze public_1.variant_files(end_pos);
analyze public_1.variant_files(chunk_id);

create index chr_start_end on public_1.variant(chrom, start_pos, end_pos);
create index chr_1 on public_1.variant(chrom);
create index start_1 on public_1.variant(start_pos);
create index end_1 on public_1.variant(end_pos);
select version();
set random_page_cost to 100;
set random_page_cost to 4;
show random_page_cost;
show citus.shard_max_size;
show seq_page_cost;
explain select * from public_1.variant where chrom = '3' and start_pos >= 11919 and end_pos <= 60263869 limit 100;
select * from public_1.variant where character_length(var_id) > 50;
select chrom, count(*) from public_1.variant  group by 1;
explain 
select cast(var_id || '_' || var_type as varchar(50)) as var_id_mod
from public_1.variant where var_id in 
('21_000022954584_000022954584',
'21_000040823941_000040823941',
'21_000021353996_000021353999',
'21_000047350070_000047350070',
'21_000038877736_000038877736'
) a
inner join public_1.variant b on b.var_id = a.var_id
group by 1;

select * from public_1.variant where var_id = '21_000021353996_000021353999';

select var_id, genotype, rng_start, rng_end from public_1.variant_sample_attrs where rng_start is not null group by 1,2,3,4 having count(*) > 1;
select var_id, genotype, norng_index from public_1.variant_sample_attrs where norng_index is not null group by 1,2,3 having count(*) > 1;
select * from public_1.stage_1 where so[1] = 1792;



drop table public_1.dummy;
create table public_1.dummy(textval text);
create table public_1.dummy2(intarr integer []);
select * from public_1.dummy2;
insert into public_1.dummy values ('x_010_020');
insert into public_1.dummy values ('x_010_015');
insert into public_1.dummy values ('x_120_013');
insert into public_1.dummy values ('x_601_413');
commit;
select * from public_1.dummy order by textval;

create table public_1.reg_chrom(chrom text, host text);
insert into public_1.reg_chrom values ('1', 'citusmaster.windows.ebi.ac.uk');
select * from public_1.reg_chrom ;

select max(textval) from public_1.dummy 

insert into variant (var_id, annot_ref) values ('some', row('6bb4a312fa295016d2c4feb89a360e46'))

delete from public_1.variant where var_id = 'some';

select * from public_1.annot_obj;
select * from public_1.ct where var_id like '1_0002122%';
select master_modify_multiple_shards('delete from public_1.ct where var_id like ''1_0002122%''');
create index chrom_idx_b on public_1.variant using btree (chrom);
create index start_idx_b on public_1.variant using btree (start_pos);
create index end_idx_b on public_1.variant using btree (end_pos);
create index chunk_idx_b on public_1.variant using btree (chunk_id);

create index chrom_idx_vf on public_1.variant_files using btree (chrom);
create index start_idx_vf on public_1.variant_files using btree (start_pos);
create index end_idx_vf on public_1.variant_files using btree (end_pos);
create index chunk_idx_vf on public_1.variant_files using btree (chunk_id);

create index chrom_idx_va on public_1.variant_annot using btree (chrom);
create index start_idx_va on public_1.variant_annot using btree (start_pos);
create index end_idx_va on public_1.variant_annot using btree (end_pos);
create index chunk_idx_va on public_1.variant_annot using btree (chunk_id);

create index chrom_idx_vs on public_1.variant_sample using btree (chrom);
create index start_idx_vs on public_1.variant_sample using btree (start_pos);
create index end_idx_vs on public_1.variant_sample using btree (end_pos);
create index chunk_idx_vs on public_1.variant_sample using btree (chunk_id);

create index var_id_idx_b on public_1.variant using btree (var_id);
create index var_id_idx_vf on public_1.variant_files using btree (var_id);
create index var_id_idx_vs on public_1.variant_sample using btree (var_id);
create index var_id_idx_va on public_1.variant_annot using btree (var_id);

create index var_id_idx_ct_b on public_1.ct using btree (var_id);
select master_modify_multiple_shards('drop index public_1.chrom_idx_b');
explain select * from public_1.variant where chrom = '19' limit 100;

cluster verbose public_1.variant using var_id_idx_b;
cluster public_1.variant_files using var_id_idx_vf;
cluster verbose public_1.variant_sample using var_id_idx_vs;

explain select * from public_1.variant where chrom = '2' and start_pos >= 47000000 and start_pos <= 49000000 and end_pos >= 48000000 and end_pos <= 50000000 and var_ref = 't' order by chrom limit 1000;

select 
ct.* 
from public_1.
    (select (annot_ref).ct_grp_id from public_1.variant where var_id = '1_758351_a_g') var
    inner join ct_grp on ct_grp.ct_grp_id = var.ct_grp_id
    inner join ct on ct.ct_id = ct_grp.ct_id
    
(select ct_id from public_1.ct_grp where ct_grp_id in (select (annot_ref).ct_grp_id from public_1.variant ));

create or replace function citus_table_size(table_name regclass)
returns bigint language plpgsql
as $function$
declare
        table_size bigint;
begin
        perform master_update_shard_statistics(shardid)
        from pg_dist_shard
        where logicalrelid = table_name;

        select sum(shard_size) into table_size
        from (
                select max(shardlength) as shard_size
                from pg_dist_shard join pg_dist_shard_placement using (shardid)
                where logicalrelid = table_name and shardstate = 1
                group by shardid
        ) shard_sizes;

        return table_size;
end;
$function$;


explain
select 
a.var_id,
array_agg(('ct_index',b.ct_index,'gn',b.gn,'ensg', b.ensg, 'so',b.so))
from 
public_1.variant a
inner join public_1.ct b on a.var_id = b.var_id
where
a.chrom = '3' and a.start_pos >= 48000000 and a.start_pos <= 49000000
group by 1
limit 100;

explain
select 
a.var_id,
array_agg(row_to_json((b.ct_index,b.gn,b.ensg,b.so)))
from 
public_1.variant a
inner join public_1.ct b on a.var_id = b.var_id
where
a.chrom = '3' and a.start_pos >= 48000000 and a.start_pos <= 49000000
group by 1
limit 100;

select 
var_id, array_agg(ct_index)
from 
public_1.ct where var_id like '3_00004817%' group by 1;

select substr(var_id, 1,1) from public_1.ct group by 1;

select * from public_1.variant limit 100;

explain select * from public_1.variant_files where VAR_ID like '22%' limit 100;
explain select * from public_1.variant_sample where VAR_ID like '22%' limit 100;
select * from public_1.variant_files limit 10;

show citus.task_executor_type;
set citus.task_executor_type to "real-time";
set citus.task_executor_type to "task-tracker";
explain
select
var.*,
varf.sample_index,
varf.fid,
varf.sid,
varf.attrs
from
public_1.variant var
left join 
public_1.variant_files varf on varf.VAR_ID = var.VAR_ID and var.VAR_ID between 'X_000003800000_000003800000_00000000000000000000000000000000          ' and 'X_000003801000_000003901000_00000000000000000000000000000000          ' 
and varf.VAR_ID between 'X_000003800000_000003800000_00000000000000000000000000000000          ' and 'X_000003801000_000003801000_00000000000000000000000000000000          ';

explain analyze
select
var.*,
varf.sample_index,
varf.fid,
varf.sid,
varf.attrs
from
public_1.variant var
left join 
public_1.variant_files varf on varf.VAR_ID = var.VAR_ID and varf.chunk_id = var.chunk_id 
and varf.chrom = var.chrom and varf.start_pos = var.start_pos and varf.end_pos = var.end_pos
where var.chrom = '2' and var.start_pos between 226462 and 426462 and var.end_pos between 226462 and 426462 and var.chunk_id = '2_0'
order by var_id, sample_index;

explain analyze
select
var.*,
vars.*
from
public_1.variant var
left join 
public_1.variant_sample vars on vars.VAR_ID = var.VAR_ID and vars.chunk_id = var.chunk_id 
and vars.chrom = var.chrom and vars.start_pos = var.start_pos and vars.end_pos = var.end_pos
where var.chrom = '2' and var.start_pos between 226462 and 246462 and var.end_pos between 226462 and 246462 and var.chunk_id = '2_0'
order by var.var_id, vars.sample_index;

explain analyze
select var_id, chrom, start_pos, end_pos, var_len, var_ref,var_alt, var_type,ids,hgvs from public_1.variant var where var.chrom = '2' and var.start_pos between 11007658 and 11207658 and var.end_pos between 11007658 and 11207658
and var.chunk_id = '2_11' order by var_id collate "C";

explain analyze
select * from public_1.variant_files var where var.chrom = '2' and var.start_pos between 11007658 and 11027658 and var.end_pos between 11007658 and 11027658
and var.chunk_id = '2_11' order by var_id;

select
var.*,
varf.sample_index,
varf.fid,
varf.sid,
varf.attrs
from
(select * from public_1.variant where  VAR_ID between 'X_000003800000_000003800000_00000000000000000000000000000000          ' and 'X_000003900000_000003900000_00000000000000000000000000000000          ') var
left join 
(select * from public_1.variant_files where VAR_ID between 'X_000003800000_000003800000_00000000000000000000000000000000          ' and 'X_000003900000_000003900000_00000000000000000000000000000000          ' ) varf
on var.VAR_ID = varf.VAR_ID

select * from public_1.variant where chrom = 'X' and start_pos between 3800000 and 4800000;
select * from public_1.variant where VAR_ID between 'X_000003800000_000003800000_00000000000000000000000000000000          ' and 'X_000003900000_000003900000_00000000000000000000000000000000          ';

select * from public_1.ct where var_id like '22_000036051%' limit 100;

select * from public_1.variant_files limit 10;

create table public_1.hybrid_table(charfield varchar(10), intfield integer, otherfield jsonb);
insert into public_1.hybrid_table values ('abc',2, '{"key1":"value1", "key2":"value2"}');
insert into public_1.hybrid_table values ('abc',2, NULL);
select * from public_1.hybrid_table where otherfield is null;

alter table public_1.variant alter column ids type varchar(100)[];
alter table public_1.variant alter column ids type varchar(100)[];
alter table public_1.variant_files add column START_POS bigint;
alter table public_1.variant_files add column END_POS bigint;
alter table public_1.variant_files add column CHROM VARCHAR(10);

alter table public_1.variant_files drop column START_POS;
alter table public_1.variant_files drop column END_POS;
alter table public_1.variant_files drop column CHROM;


select master_modify_multiple_shards('update public_1.variant_files set CHROM = split_part(VAR_ID, ''_'', 1)');
select master_modify_multiple_shards('update public_1.variant_files set START_POS = cast(split_part(VAR_ID, ''_'', 2) as bigint);');
select master_modify_multiple_shards('update public_1.variant_files set END_POS = cast(split_part(VAR_ID, ''_'', 3) as bigint);');

select * from pg_catalog.pg_dist_shard_placement order by shardid;

select CHROM from public_1.variant group by 1 order by 1;

reset work_mem;
show work_mem;
set work_mem to '40MB';

select column_name, data_type, character_maximum_length
from INFORMATION_SCHEMA.COLUMNS where table_name = 'variant';
show citus.explain_all_tasks;
set citus.explain_all_tasks = 1;

show shared_buffers;
set shared_buffers to '6024MB';

show effective_cache_size;

SELECT run_command_on_workers($cmd$ set work_mem to '400MB'; $cmd$);
SELECT run_command_on_workers($cmd$ alter user postgres set work_mem='400MB'; $cmd$);
SELECT run_command_on_workers($cmd$ show work_mem; $cmd$);
SELECT run_command_on_workers($cmd$ show shared_buffers; $cmd$);
SELECT run_command_on_workers($cmd$ show effective_cache_size; $cmd$);


select run_command_on_shards(
    'public_1.variant',
    $cmd$
      cluster public_1.%I using var_id_idx_b;
    $cmd$
  );

create table public.variant_copy as select * from public_1.variant;
select * from public.variant_copy;
delete from public.variant_copy;

SELECT * FROM pg_catalog.pg_dist_shard;
SELECT nodename, nodeport, shardid FROM pg_dist_shard 
JOIN pg_dist_shard_placement USING (shardid) WHERE logicalrelid = 'public_1.variant'::regclass