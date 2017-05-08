explain select request_attr -> '_source' ->> '@timestamp' as recent_ts from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' desc limit 1;
VACUUM ANALYZE public.kibana_ws_logs ;

CREATE INDEX kibana_ws_logs_expr_idx ON public.kibana_ws_logs ((request_attr-> '_source' ->> '@timestamp'));

drop INDEX kibana_ws_logs_expr_idx;

delete from public.kibana_ws_logs ;

delete from public.ws_traffic ;

select count(*) from public.kibana_ws_logs;
select count(*) from public.ws_traffic ;

drop view public.ws_traffic_useful_cols;
create or replace view public.ws_traffic_useful_cols as (
select event_ts, client, bytes_out, user_agent, duration, request_uri_path, request_query, seg_len, http_status from public.ws_traffic);

set random_page_cost to 4;

select request_attr -> '_source' ->>'' from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' limit 100;

drop table public.ws_traffic;
create table public.ws_traffic (event_ts_txt text, event_ts timestamp with time zone, http_req_type text, host text, path text, syslog_pri text, syslog_timestamp text, syslog_hostname text, 
remote_host text, request_ts timestamp with time zone, client text, bytes_out bigint, bytes_in bigint,  duration decimal(18,8), pool_name text,
server_node text, user_agent text, request_type CHAR(30),  http_status CHAR(10), is_https CHAR(10), virtual_host text, request_uri_path text, request_query text, cookie_header text, seg_len bigint);

select cast('2017-05-03T20:07:02.755174+01:00' as timestamp with time zone)

select max(event_ts_txt) as recent_ts from public.ws_traffic;

select http_status, count(*) from public.ws_traffic group by 1;
select request_type, count(*) from public.ws_traffic group by 1;
select is_https, count(*) from public.ws_traffic group by 1;
select request_uri_path from public.ws_traffic group by 1;

select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' order by event_ts;
select * from public.ws_traffic_useful_cols where user_agent like '%WormBase%' and request_uri_path like '%/segments/%' order by seg_len desc;

select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status <> '400' order by seg_len desc;

select user_agent, max(bytes_out), max(seg_len) from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status <> '400' group by 1;

CREATE FUNCTION return_arr()
  RETURNS int[]
AS $$
return (1, 2, 3, 4, 5)
$$ LANGUAGE plpythonu;

select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status not in ('400','404') order by seg_len desc;