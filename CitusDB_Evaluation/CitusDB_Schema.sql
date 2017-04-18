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
VAR_ID TEXT,
CT_INDEX SMALLINT,
gn TEXT,
ensg VARCHAR(30),
enst VARCHAR(30),
codon TEXT,
strand VARCHAR(30),
bt TEXT,
aaChange VARCHAR(30),
so INTEGER[]
);

create TABLE public_1.variant
(
VAR_ID TEXT,
CHROM CHAR(10),
START_POS BIGINT,
END_POS BIGINT,
VAR_TYPE CHAR(10),
VAR_LEN SMALLINT,
VAR_REF TEXT,
VAR_ALT TEXT
);


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
select * from public_1.ct;
select * from public_1.ct_grp;
select ct_id from public_1.ct_grp group by 1;
select * from public_1.src_file;
select * from public_1.file_grp;
select * from public_1.variant_sample_attrs;

select count(*) from public_1.variant;
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
explain select * from public_1.variant where chrom = '3' and start_pos >= 11919 and end_pos <= 60263869 limit 100;
select * from public_1.variant where character_length(var_id) > 50;
select var_id from public_1.variant group by 1 having count(*) > 1;
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


insert into variant (VAR_ID, annot_ref) values ('some', ROW('6bb4a312fa295016d2c4feb89a360e46'))

delete from public_1.variant where VAR_ID = 'some';

select * from public_1.ANNOT_OBJ;

select 
ct.* 
from public_1.
    (select (annot_ref).ct_grp_id from public_1.variant where var_id = '1_758351_A_G') var
    inner join ct_grp on ct_grp.ct_grp_id = var.ct_grp_id
    inner join ct on ct.ct_id = ct_grp.ct_id
    
(select ct_id from public_1.ct_grp where ct_grp_id in (select (annot_ref).ct_grp_id from public_1.variant ));