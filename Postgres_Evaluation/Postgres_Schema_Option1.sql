create TYPE ANNOT_OBJ AS (
    CT_GRP_ID VARCHAR(50)
);

create TABLE VARIANT_SAMPLE_ATTRS (
    SAMPLE_ATTR_ID VARCHAR(50),
    GENOTYPE CHAR(5),
    RNG_START SMALLINT,
    RNG_END SMALLINT,
    NORNG_INDEX SMALLINT
) distributed by (SAMPLE_ATTR_ID);


create table src_file
(
file_unique_id VARCHAR(50),
fid VARCHAR(50),
sid VARCHAR(50),
fm VARCHAR(10),
sample_id VARCHAR(50)
) DISTRIBUTED BY (file_unique_id);

create table FILE_GRP
(
FILE_GRP_ID VARCHAR(50),
FILE_UNIQUE_ID VARCHAR(50)
) DISTRIBUTED BY (FILE_GRP_ID);

create table CT_GRP
(
CT_GRP_ID VARCHAR(50),
CT_ID VARCHAR(50)
) DISTRIBUTED BY (CT_GRP_ID);

create table CT
(
CT_ID VARCHAR(50),
gn VARCHAR(30),
ensg VARCHAR(30),
enst VARCHAR(30),
codon VARCHAR(5),
strand VARCHAR(5),
bt VARCHAR(30),
aaChange VARCHAR(5),
so INTEGER[]
) DISTRIBUTED BY (CT_ID);

create table variant
(
VAR_ID VARCHAR(100),
CHROM CHAR(10),
START_POS BIGINT,
END_POS BIGINT,
VAR_TYPE CHAR(10),
VAR_LEN SMALLINT,
VAR_REF TEXT,
VAR_ALT TEXT,
HGV_GRP_REF VARCHAR(50),
ANNOT_REF ANNOT_OBJ,
FILES_GRP_REF VARCHAR(50)
) DISTRIBUTED BY (CHROM, START_POS, END_POS);

create table HGV_GRP
(
HGV_GRP_ID VARCHAR(50),
HGV_ID VARCHAR(50)
) DISTRIBUTED BY (HGV_GRP_ID);

create table HGV
(
HGV_ID VARCHAR(50),
HGV_TYPE VARCHAR(30),
HGV_NAME VARCHAR(100)
) DISTRIBUTED BY (HGV_ID);

create table ANNOT_GRP
(
ANNOT_GRP_ID VARCHAR(50),
CT_GRP_REF VARCHAR(50)
) DISTRIBUTED BY (HGV_GRP_ID);

select * from hgv;
select * from hgv_grp;
select * from variant;
select (annot_ref).ct_grp_id from variant group by 1;
select * from ct;
select * from ct_grp;
select ct_id from ct_grp group by 1;

delete from src_file;
delete from ct;
delete from ct_grp;
delete from variant;
delete from hgv;
delete from hgv_grp;
delete from file_grp;
delete from variant_sample_attrs;

select * from src_file;
select * from file_grp;
select * from variant_sample_attrs;

insert into variant (VAR_ID, annot_ref) values ('some', ROW('6bb4a312fa295016d2c4feb89a360e46'))

delete from variant where VAR_ID = 'some';

select * from ANNOT_OBJ;

select 
ct.* 
from 
    (select (annot_ref).ct_grp_id from variant where var_id = '1_758351_A_G') var
    inner join ct_grp on ct_grp.ct_grp_id = var.ct_grp_id
    inner join ct on ct.ct_id = ct_grp.ct_id
    
(select ct_id from ct_grp where ct_grp_id in (select (annot_ref).ct_grp_id from variant ));