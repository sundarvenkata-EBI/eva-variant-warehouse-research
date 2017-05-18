drop table public_1.mongo_doc_size;
create table public_1.mongo_doc_size(var_id varchar(70), docsize bigint);

select * from public_1.mongo_doc_size order by docsize desc limit 100;