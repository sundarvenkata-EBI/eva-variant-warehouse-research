create TYPE public_1.ANNOT_OBJ AS (
    CT_GRP_ID VARCHAR(50)
);

create TABLE public_1.VARIANT_SAMPLE_ATTRS (
    VAR_ID TEXT,
    SAMPLE_INDEX SMALLINT,
    GENOTYPE CHAR(5),
    RNG_START SMALLINT,
    RNG_END SMALLINT,
    NORNG_INDEX SMALLINT,
    DEFAULT_MARKER SMALLINT
);


create TABLE public_1.src_file
(
VAR_ID TEXT,
SAMPLE_INDEX SMALLINT,
fid VARCHAR(50),
sid VARCHAR(50),
fm VARCHAR(10)
);


create TABLE public_1.CT
(
VAR_ID CHAR(30),
CT_INDEX SMALLINT,
gn TEXT,
ensg VARCHAR(30),
enst VARCHAR(30),
codon TEXT,
strand VARCHAR(30),
bt TEXT,
aaChange TEXT,
so INTEGER[]
);
SELECT create_distributed_table('public_1.ct', 'var_id', 'append');

create TABLE public_1.variant
(
VAR_ID CHAR(70),
CHROM CHAR(10),
START_POS BIGINT,
END_POS BIGINT,
VAR_TYPE CHAR(10),
VAR_LEN SMALLINT,
VAR_REF TEXT,
VAR_ALT TEXT
);
SELECT create_distributed_table('public_1.variant', 'var_id', 'append');
SET citus.shard_max_size TO '30720MB';

create TABLE public_1.HGV
(
VAR_ID TEXT,
HGV_TYPE VARCHAR(30),
HGV_NAME VARCHAR(100)
);

select * from public_1.hgv;
select * from public_1.hgv_grp;
select * from public_1.variant limit 100;
select (annot_ref).ct_grp_id from public_1.variant group by 1;
select * from public_1.ct limit 100;
select * from public_1.ct_grp;
select ct_id from public_1.ct_grp group by 1;
select * from public_1.src_file;
select * from public_1.file_grp;
select * from public_1.variant_sample_attrs;

SELECT *
  FROM pg_dist_shard_placement
 WHERE shardid = (
   SELECT get_shard_id_for_distribution_column('public_1.variant', '1_000227471340_000227471340')
 );
SELECT pg_size_pretty(citus_table_size('public_1.ct'));
SELECT pg_size_pretty(citus_table_size('public_1.variant'));
select chrom from public_1.variant group by 1;
select chrom from public_1.ct group by 1;
explain select * from public_1.variant where var_id = '21_000048119868_000048119868';
explain select * from public_1.ct where var_id = '21_000048119868_000048119868';
select count(*) from public_1.variant;
select count(*) from public_1.ct;
select count(*) from public_1.variant where CHROM = 'X';
db.variant_chr21_1_1.find({"chr": "21", "start": 9411413, "ref": "T"});
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
select cast(VAR_ID || '_' || VAR_TYPE as VARCHAR(50)) as VAR_ID_MOD
from public_1.variant where VAR_ID in 
('21_000022954584_000022954584',
'21_000040823941_000040823941',
'21_000021353996_000021353999',
'21_000047350070_000047350070',
'21_000038877736_000038877736'
) a
inner join public_1.variant b on b.VAR_ID = a.var_id
group by 1;

select * from public_1.variant where VAR_ID = '21_000021353996_000021353999';

select var_id, genotype, rng_start, rng_end from public_1.variant_sample_attrs where rng_start is not null group by 1,2,3,4 having count(*) > 1;
select var_id, genotype, norng_index from public_1.variant_sample_attrs where norng_index is not null group by 1,2,3 having count(*) > 1;
select * from public_1.stage_1 where so[1] = 1792;
select master_modify_multiple_shards('delete from public_1.variant where chrom = ''21''');

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

drop TABLE public_1.dummy;
create TABLE public_1.dummy(textval text);
create TABLE public_1.dummy2(intarr INTEGER []);
select * from public_1.dummy2;
insert into public_1.dummy values ('X_010_020');
insert into public_1.dummy values ('X_010_015');
insert into public_1.dummy values ('X_120_013');
insert into public_1.dummy values ('X_601_413');
commit;
select * from public_1.dummy order by textval;

create TABLE public_1.reg_chrom(chrom text, host text);
insert into public_1.reg_chrom values ('1', 'citusmaster.windows.ebi.ac.uk');
select * from public_1.reg_chrom ;
delete from public_1.reg_chrom ;

select max(textval) from public_1.dummy 

insert into variant (VAR_ID, annot_ref) values ('some', ROW('6bb4a312fa295016d2c4feb89a360e46'))

delete from public_1.variant where VAR_ID = 'some';

select * from public_1.ANNOT_OBJ;
select * from public_1.ct where VAR_ID like '1_0002122%';
select master_modify_multiple_shards('delete from public_1.ct where VAR_ID like ''1_0002122%''');
CREATE INDEX chrom_idx_b ON public_1.variant USING BTREE (chrom);
CREATE INDEX start_idx_b ON public_1.variant USING BTREE (start_pos);
CREATE INDEX end_idx_b ON public_1.variant USING BTREE (end_pos);
CREATE INDEX var_id_idx_b ON public_1.variant USING BTREE (VAR_ID);

CREATE INDEX var_id_idx_ct_b ON public_1.ct USING BTREE (VAR_ID);
select master_modify_multiple_shards('drop index public_1.chrom_idx_b');
explain select * from public_1.variant where chrom = '19' limit 100;

explain select * from public_1.variant where chrom = '2' and start_pos >= 47000000 and start_pos <= 49000000 and end_pos >= 48000000 and end_pos <= 50000000 and var_ref = 'T' order by chrom limit 1000;

select 
ct.* 
from public_1.
    (select (annot_ref).ct_grp_id from public_1.variant where var_id = '1_758351_A_G') var
    inner join ct_grp on ct_grp.ct_grp_id = var.ct_grp_id
    inner join ct on ct.ct_id = ct_grp.ct_id
    
(select ct_id from public_1.ct_grp where ct_grp_id in (select (annot_ref).ct_grp_id from public_1.variant ));

CREATE OR REPLACE FUNCTION citus_table_size(table_name regclass)
RETURNS bigint LANGUAGE plpgsql
AS $function$
DECLARE
        table_size bigint;
BEGIN
        PERFORM master_update_shard_statistics(shardid)
        FROM pg_dist_shard
        WHERE logicalrelid = table_name;

        SELECT sum(shard_size) INTO table_size
        FROM (
                SELECT max(shardlength) AS shard_size
                FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
                WHERE logicalrelid = table_name AND shardstate = 1
                GROUP BY shardid
        ) shard_sizes;

        RETURN table_size;
END;
$function$;


explain
select 
a.VAR_ID,
array_agg(('ct_index',b.ct_index,'gn',b.gn,'ensg', b.ensg, 'so',b.so))
from 
public_1.variant a
inner join public_1.ct b on a.VAR_ID = b.VAR_ID
where
a.chrom = '3' and a.START_POS >= 48000000 and a.START_POS <= 49000000
group by 1
limit 100;

explain
select 
a.VAR_ID,
array_agg(row_to_json((b.ct_index,b.gn,b.ensg,b.so)))
from 
public_1.variant a
inner join public_1.ct b on a.VAR_ID = b.VAR_ID
where
a.chrom = '3' and a.START_POS >= 48000000 and a.START_POS <= 49000000
group by 1
limit 100;

select 
VAR_ID, array_agg(CT_INDEX)
from 
public_1.ct where VAR_ID like '3_00004817%' group by 1;

select SUBSTR(VAR_ID, 1,1) from public_1.ct group by 1;

select * from public_1.variant where chrom = '22' limit 100;

select * from public_1.ct where VAR_ID like '22_000036051%' limit 100;