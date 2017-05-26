explain select request_attr -> '_source' ->> '@timestamp' as recent_ts from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' desc limit 1;
VACUUM ANALYZE public.kibana_ws_logs ;

CREATE INDEX kibana_ws_logs_expr_idx ON public.kibana_ws_logs ((request_attr-> '_source' ->> '@timestamp'));

drop INDEX kibana_ws_logs_expr_idx;

delete from public.kibana_ws_logs ;

delete from public.ws_traffic ;

select count(*) from public.kibana_ws_logs;
select count(*) from public.ws_traffic ;

CREATE INDEX ws_traffic_ts_idx ON public.ws_traffic (event_ts);

drop view public.ws_traffic_useful_cols;
create or replace view public.ws_traffic_useful_cols as (
select request_ts, client_ip, bytes_out, user_agent, duration, request_uri_path, request_query, seg_len, http_status from public.ws_traffic
	where http_status not in ('400','404', '500')
	and client_ip not in ('193.62.194.244','193.62.194.245','193.62.194.241','193.63.221.163','193.62.194.246',
	'193.62.194.251','86.130.14.35','193.62.194.242', '172.22.69.141', '172.22.69.81','172.22.71.2','172.22.68.226','172.22.69.8','172.22.69.245',
	'172.22.68.228', '172.22.68.113') and client_ip not like '172.22%'
	);

set random_page_cost to 4;

select request_attr -> '_source' ->>'' from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' limit 100;

drop table public.ws_traffic;
create table public.ws_traffic (event_ts_txt text, event_ts timestamp with time zone, http_req_type text, host text, path text, syslog_pri text, syslog_timestamp text, syslog_hostname text, 
remote_host text, request_ts timestamp without time zone, client_ip text, bytes_out bigint, bytes_in bigint,  duration decimal(18,8), pool_name text,
server_node text, user_agent text, request_type CHAR(30),  http_status CHAR(10), is_https CHAR(10), virtual_host text, request_uri_path text, request_query text, cookie_header text, seg_len bigint, historic_data smallint);

select cast('2017-05-03T20:07:02.755174+01:00' as timestamp with time zone)

select min(request_ts), max(request_ts) as recent_ts from public.ws_traffic_useful_cols;

select http_status, count(*) from public.ws_traffic group by 1;
select request_type, count(*) from public.ws_traffic group by 1;
select is_https, count(*) from public.ws_traffic group by 1;
select request_uri_path from public.ws_traffic group by 1;

select * from public.ws_traffic where request_uri_path like '%/segments/%' order by duration desc;
select * from public.ws_traffic where request_query like '%histogram=%' order by duration desc;
select a.*, getTotalSegmentLength(request_uri_path) from public.ws_traffic_useful_cols a where a.request_uri_path like '%/segments/%';
select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' order by request_ts;
select * from public.ws_traffic_useful_cols where user_agent like '%WormBase%' and request_uri_path like '%/segments/%' order by seg_len desc;

select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status <> '400' order by seg_len desc;

select user_agent, max(bytes_out), max(seg_len) from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status <> '400' group by 1;

CREATE or REPLACE FUNCTION getTotalSegmentLength(querystring text)
  RETURNS int
AS $$
totSegmentLength = 0
try:
	segments = querystring.split("/segments/")[1].split("/variants")[0].split(",")
	for segment in segments:
			segment = segment.strip()
			if segment:
					segmentLBUB = segment.split(":")[1].split("-")
					if len(segmentLBUB) < 2: return int(totSegmentLength)
					segmentLength = float(segmentLBUB[1]) - float(segmentLBUB[0])
					if (segmentLength > 0): totSegmentLength += segmentLength
except Exception:
	print(querystring)
return int(totSegmentLength)
$$ LANGUAGE plpythonu;

CREATE or REPLACE FUNCTION unquoteRequestURI(requesturi text)
	RETURNS text
AS $$
import urllib2
requesturi_new = requesturi.encode("utf8")
return urllib2.unquote(requesturi_new).decode("latin1")
$$ LANGUAGE plpythonu;


select a.*,(case when a.request_query like '%exclude=sourceEntries%' then 1 else 0 end) as SRC_EXCL from public.ws_traffic_useful_cols a where request_uri_path like '%/segments/%' and http_status not in ('400','404') order by seg_len desc;

--What fraction of the requests have source entry exclusion flag set? - 65% approximately
select (case when a.request_query like '%exclude=sourceEntries%' then 1 else 0 end) as SRC_EXCL, count(*) from public.ws_traffic_useful_cols a where request_uri_path like '%/segments/%' and http_status not in ('400','404')
	and request_ts >= '2015-04-21 14:30:28.000000' group by 1;

--What fraction of the requests have Sift score based queries? - 0.2% approximately
select (case when a.request_query like '%sift%' then 1 else 0 end) as SIFT_INCL, count(*) from public.ws_traffic_useful_cols a where request_uri_path like '%/segments/%' and http_status not in ('400','404')
	and request_ts >= (select min(request_ts)  from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%'
																																 and request_query like '%sift%') group by 1;

select * from public.ws_traffic_useful_cols a where request_uri_path like '%/segments/%' and http_status not in ('400','404')
	and request_ts >= (select min(request_ts)  from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%'
																																 and request_query like '%sift%')  and request_query like '%sift%';

--What fraction of the requests have Polyphen score based queries? - 0.1% approximately
select (case when a.request_query like '%polyphen%' then 1 else 0 end) as PPHEN_INCL, count(*) from public.ws_traffic_useful_cols a where request_uri_path like '%/segments/%' and http_status not in ('400','404')
	and request_ts >= (select min(request_ts)  from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%'
																																 and request_query like '%polyphen%') group by 1;

select * from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status not in ('400','404') 
	and request_query not like '%exclude=sourceEntries%';
select client_ip,count(*) from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and http_status not in ('400','404')
	and request_query not like '%exclude=sourceEntries%' and request_ts >= '2015-04-21 14:30:28.000000' group by 1 order by 2 desc;


select * from (select substr(cast(event_ts as text), 1, 13) as traffic_hour, count(*) as hits  from public.ws_traffic_useful_cols group by 1) a order by traffic_hour;
select * from (select substr(cast(event_ts as text), 1, 16) as traffic_hour, count(*) as hits  from public.ws_traffic_useful_cols group by 1) a order by hits desc;
select * from (select substr(cast(event_ts as text), 1, 16) as traffic_hour, count(distinct client) as hits  from public.ws_traffic_useful_cols 
	where request_uri_path like '%/segments/%' and http_status not in ('400','404') group by 1) a order by hits desc;

select * from public.ws_traffic_useful_cols where cast(event_ts as text) like '2017-05-03 09:05%';'?merge=true&exclude=sourceEntries&_dc=1493720679597&species=hsapiens_grch37&page=1&skip=0&limit=10'
select * from public.ws_traffic_useful_cols where cast(event_ts as text) like '2017-05-04 15:14%' and request_uri_path like '%/segments/%';
select * from public.ws_traffic_useful_cols where cast(event_ts as text) like '2017-05-02 11:24%' and request_uri_path like '%/segments/%';

select get_bit(B'0110111',1);

drop table public.kibana_ws_traffic_hist;
create table public.kibana_ws_traffic_hist (uniq_id VARCHAR(100), request_ts timestamp, client_ip VARCHAR(20), bytes_out bigint, 
	bytes_in bigint, duration decimal(18,8), pool_name text, server_node text, user_agent text,
	request_type VARCHAR(30), http_status VARCHAR(10), is_https VARCHAR(10), virtual_host text, 
	request_uri_path text, request_query text, cookie_header text);

insert into public.ws_traffic (request_ts, client_ip, bytes_out, bytes_in, duration, pool_name, server_node, user_agent, request_type, 
	http_status, is_https, virtual_host, request_uri_path, request_query, cookie_header, historic_data) 
	select request_ts, client_ip, bytes_out, bytes_in, duration, pool_name, server_node, user_agent, request_type, 
	http_status, is_https, virtual_host, request_uri_path, request_query, cookie_header,1 from public.kibana_ws_traffic_hist a;

select min(request_ts)  from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%'
																																 and request_query like '%exclude=sourceEntries%';

select min(request_ts)  from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%'
																																 and request_query like '%sift%';

select * from public.ws_traffic_useful_cols where request_ts = '2015-04-21 14:30:28.000000';

select * from public.ws_traffic_useful_cols order by bytes_out desc;

select * from public.ws_traffic_useful_cols where user_agent like 'libwww-perl%' order by request_ts desc;
--High frequency users of the web service
select client_ip, count(*) as NUM_HITS from public.ws_traffic_useful_cols group by 1 order by 2 desc;
--Consumers with high data downloads
select client_ip, sum(bytes_out) as TOT_BYTES_OUT from public.ws_traffic_useful_cols group by 1 order by 2 desc;
--Are consumers of the web service consciously adopting the usage of source entries exclusion? - No discernible trend
select client_ip, trim(cast(extract(year from request_ts) as VARCHAR(10))) || lpad(trim(cast(extract(month from request_ts) as VARCHAR(10))),2,'0') as MTH,
	count(*) from public.ws_traffic_useful_cols
WHERE request_query like '%exclude=sourceEntries%' and request_ts >= '2015-04-21 14:30:28.000000'
 group by 1,2 order by 1,2 ;

select avg(bytes_out), avg(duration) from public.ws_traffic_useful_cols where request_uri_path like '%/segments/%' and
	request_ts >= '2016-01-01 00:00:00.000000';

select avg(bytes_out) from public.ws_traffic_useful_cols where request_query like '%exclude=sourceEntries%' and
	request_ts >= '2016-01-01 00:00:00.000000' ;

select avg(bytes_out), avg(duration) from public.ws_traffic_useful_cols where request_query like '%histogram=%';
select * from public.ws_traffic_useful_cols where request_query like '%histogram=%';

update public.ws_traffic set request_uri_path = unquoteRequestURI(request_uri_path) where historic_data = 1;
update public.ws_traffic set request_query = unquoteRequestURI(request_query) where request_query is not null and historic_data = 1;
update public.ws_traffic set seg_len = getTotalSegmentLength(request_uri_path) where historic_data = 1 and seg_len is null
																																										 and request_uri_path like '%/segments/%' and http_status not in ('400','404', '500');

select a.*, getTotalSegmentLength(request_uri_path) from ws_traffic a where historic_data = 1 and seg_len is null and request_uri_path like '%/segments/%' and http_status not in ('400','404', '500');
select a.* from ws_traffic a where historic_data = 1 and seg_len is null and request_uri_path like '%/segments/%' and http_status not in ('400','404', '500') limit 10;
select a.* from ws_traffic a where request_query like '%&amp%';
alter table public.ws_traffic alter column request_uri_path type text charact;
show CLIENT_ENCODING ;
set CLIENT_ENCODING  to 'latin1';

select * from ws_traffic_useful_cols where request_uri_path like '%segments%' and request_uri_path like '%contig%' ;