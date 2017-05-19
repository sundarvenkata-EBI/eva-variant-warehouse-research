create table public.mongo_doc_size(var_id varchar(70), docsize bigint);

create view public.mongo_doc_size_vw as
  select a.*, a.END_POS - a.START_POS as VAR_LEN from
  (select a.*, cast(split_part(a.var_id,'_',2) as bigint) as START_POS, cast(split_part(a.var_id,'_',3) as bigint) as END_POS
  from public.mongo_doc_size a ) a;

select count(*) from public.mongo_doc_size;

select * from
	(select a.*, cast(split_part(var_id,'_',3) as bigint) - cast(split_part(var_id,'_',2) as bigint) as VAR_LEN from public.mongo_doc_size a) Temp
	order by docsize  desc limit 100;

select max(docsize) from public.mongo_doc_size;

select * from public.mongo_doc_size where var_id like '4_%'