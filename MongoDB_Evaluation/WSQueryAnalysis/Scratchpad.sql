explain select request_attr -> '_source' ->> '@timestamp' as recent_ts from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' desc limit 1;
VACUUM ANALYZE public.kibana_ws_logs ;

CREATE INDEX kibana_ws_logs_expr_idx ON public.kibana_ws_logs ((request_attr-> '_source' ->> '@timestamp'));

drop INDEX kibana_ws_logs_expr_idx;

delete from public.kibana_ws_logs ;

select count(*) from public.kibana_ws_logs;

set random_page_cost to 4;

select  from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' limit 100;