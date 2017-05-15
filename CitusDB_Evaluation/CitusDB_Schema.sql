create type public_1.annot_obj as (
    ct_grp_id varchar(50)
);

create type public_1.hgv as (
    hgv_type varchar(50),
    hgv_name varchar(50)
);

create table public_1.hgv_arrays (
hgv_arr public_1.hgv []
);

insert into public_1.hgv_arrays values (array[row('genomic','x:g.10020864c>t')::public_1.hgv,row('some other','other name')::public_1.hgv])
insert into public_1.hgv_arrays values ('{}');

select hgv_arr[2] from public_1.hgv_arrays ;


create table public_1.char_arr (
char_arr varchar(30) []
);
insert into public_1.char_arr values ('{value1, value2}');



create table public_1.variant_sample_attrs (
    var_id text,
    sample_index smallint,
    genotype varchar(5),
    rng_start smallint,
    rng_end smallint,
    norng_index smallint,
    default_marker smallint
);


create table public_1.variant_sample
(
var_id char(70),
sample_index smallint,
genotype varchar(5),
tot_num_samp integer,
sample_index_bits bit varying(1000000)
);
select create_distributed_table('public_1.variant_sample', 'var_id', 'append');


create table public_1.variant_files
(
var_id varchar(70),
sample_index smallint,
fid varchar(50),
sid varchar(50),
attrs jsonb,
fm varchar(10),
src bytea 
);
select create_distributed_table('public_1.variant_files', 'var_id', 'append');


create table public_1.variant_annot
(
var_id varchar(70),
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
select create_distributed_table('public_1.variant_annot', 'var_id', 'append');

drop table public_1.variant;
create table public_1.variant
(
var_id char(70),
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
select create_distributed_table('public_1.variant', 'var_id', 'append');
set citus.shard_max_size to '30720mb';

create table public_1.hgv
(
var_id text,
hgv_type varchar(30),
hgv_name varchar(100)
);


select count(*) from public_1.variant;
select count(*) from public_1.variant_files;
select count(*) from public_1.variant_annot;
select count(*) from public_1.variant_sample;
select * from public_1.variant_annot where sift_sc is not null;
select * from public_1.variant where ct_sift_min is not null and ct_sift_min <> ct_sift_max limit 100;
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
delete from public_1.reg_chrom ;

select *
  from pg_dist_shard_placement
 where shardid = (
   select get_shard_id_for_distribution_column('public_1.variant', '1_000227471340_000227471340')
 );
select pg_size_pretty(citus_table_size('public_1.variant_annot')); --16GB for 23M records
select pg_size_pretty(citus_table_size('public_1.variant_files')); --15GB for 23M records
select pg_size_pretty(citus_table_size('public_1.variant_sample')); --32GB for 23M records
select pg_size_pretty(citus_table_size('public_1.variant')); --7GB for 23M records
select chrom from public_1.variant group by 1;
select chrom from public_1.ct group by 1;
explain select * from public_1.variant where var_id = '21_000048119868_000048119868';
explain select * from public_1.ct where var_id = '21_000048119868_000048119868';
select count(*) from public_1.variant;
select count(*) from public_1.ct;
select count(*) from public_1.variant where chrom = 'x';
db.variant_chr21_1_1.find({"chr": "21", "start": 9411413, "ref": "t"});
select count(*) from public_1.hgv;
analyze public_1.variant(chrom);
analyze public_1.variant(start_pos);
analyze public_1.variant(end_pos);
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



delete from public_1.src_file;
delete from public_1.ct;
delete from public_1.variant;
delete from public_1.hgv;
delete from public_1.variant_sample_attrs;

drop table public_1.src_file;
drop table public_1.ct;
drop table public_1.ct_grp;
drop table public_1.variant;
drop table public_1.hgv;
drop table public_1.hgv_grp;
drop table public_1.file_grp;
drop table public_1.variant_sample_attrs;

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
create index var_id_idx_b on public_1.variant using btree (var_id);
create index var_id_idx_vf on public_1.variant_files using btree (var_id);
create index var_id_idx_vs on public_1.variant_sample using btree (var_id);

create index var_id_idx_ct_b on public_1.ct using btree (var_id);
select master_modify_multiple_shards('drop index public_1.chrom_idx_b');
explain select * from public_1.variant where chrom = '19' limit 100;

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

select * from public_1.variant where chrom = '22' limit 100;

explain select * from public_1.variant_files where VAR_ID like '22%' limit 100;
explain select * from public_1.variant_sample where VAR_ID like '22%' limit 100;

explain
select
var.var_id,
varf.sample_index,
varf.fid,
varf.sid,
varf.attrs
from
public_1.variant var 
left join public_1.variant_files varf on varf.VAR_ID = var.VAR_ID
where
var.chrom = '22' and var.start_pos between 16050036 and 16051107;

select * from public_1.ct where var_id like '22_000036051%' limit 100;

create table public_1.hybrid_table(charfield varchar(10), intfield integer, otherfield jsonb);
insert into public_1.hybrid_table values ('abc',2, '{"key1":"value1", "key2":"value2"}');
insert into public_1.hybrid_table values ('abc',2, NULL);
select * from public_1.hybrid_table where otherfield is null;

alter table public_1.variant alter column ids type varchar(100)[];
alter table public_1.variant alter column ids type varchar(100)[];
alter table public_1.variant_files add column START_POS bigint;
alter table public_1.variant_files add column END_POS bigint;


select master_modify_multiple_shards('update public_1.variant_files set START_POS = cast(split_part(VAR_ID, ''_'', 2) as bigint);');
select master_modify_multiple_shards('update public_1.variant_files set END_POS = cast(split_part(VAR_ID, ''_'', 3) as bigint);');