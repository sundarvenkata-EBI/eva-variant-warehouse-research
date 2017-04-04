create TYPE public_1.ANNOT_OBJ AS (
    CT_GRP_ID VARCHAR(50)
);

create TABLE public_1.VARIANT_SAMPLE_ATTRS (
    VAR_ID TEXT,
    SAMPLE_INDEX SMALLINT,
    GENOTYPE CHAR(5),
    RNG_START SMALLINT,
    RNG_END SMALLINT,
    NORNG_INDEX SMALLINT
) distributed by (VAR_ID);


create TABLE public_1.src_file
(
VAR_ID TEXT,
SAMPLE_INDEX SMALLINT,
fid VARCHAR(50),
sid VARCHAR(50),
fm VARCHAR(10)
) DISTRIBUTED BY (VAR_ID);


create TABLE public_1.CT
(
VAR_ID TEXT,
CT_INDEX SMALLINT,
gn VARCHAR(30),
ensg VARCHAR(30),
enst VARCHAR(30),
codon VARCHAR(30),
strand VARCHAR(30),
bt TEXT,
aaChange VARCHAR(30),
so INTEGER[]
) DISTRIBUTED BY (VAR_ID);

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
) DISTRIBUTED BY (CHROM, START_POS, END_POS);


create TABLE public_1.HGV
(
VAR_ID TEXT,
HGV_TYPE VARCHAR(30),
HGV_NAME VARCHAR(100)
) DISTRIBUTED BY (VAR_ID);

select * from public_1.hgv;
select * from public_1.hgv_grp;
select * from public_1.variant;
select (annot_ref).ct_grp_id from public_1.variant group by 1;
select * from public_1.ct;
select * from public_1.ct_grp;
select ct_id from public_1.ct_grp group by 1;
select * from public_1.src_file;
select * from public_1.file_grp;
select * from public_1.variant_sample_attrs;
select var_id, genotype, rng_start, rng_end from public_1.variant_sample_attrs where rng_start is not null group by 1,2,3,4 having count(*) > 1;
select var_id, genotype, norng_index from public_1.variant_sample_attrs where norng_index is not null group by 1,2,3 having count(*) > 1;

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