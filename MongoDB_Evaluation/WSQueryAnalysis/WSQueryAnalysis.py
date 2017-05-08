# This script retrieves data from Kibana (tracker for Web Service requests) and dumps into a local postgres instance for analysis
import requests, json, os
import psycopg2, traceback, urllib2

# Citus credentials
postgresConnHandle = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password=''")
resultCursor = postgresConnHandle.cursor()

try:
    mostRecentTimeStamp = None
    resultCursor.execute("select max(event_ts_txt) as recent_ts from public.ws_traffic;")
    results = resultCursor.fetchall()
    if results:
        if results[0][0]:
            mostRecentTimeStamp = results[0][0]

    resultChunkSize = 1000
    logStashURLTemplate = "http://ves-ebi-2e.ebi.ac.uk:9200/_search?ignore_unavailable=true&size={0}&from={1}"
    queryConditions = [{"query_string":{"query": "request_uri_path:(\"/eva/webservices/*\")"}}]
    if mostRecentTimeStamp:
        queryConditions.append({"range": {"@timestamp": {"gt": mostRecentTimeStamp}}})
    postQuery = {
        "query": {
            "bool": {
                "must": queryConditions
            }
        }
    }

    resultSizeOffset = 0
    while(True):
        logStashURL = logStashURLTemplate.format(resultChunkSize, resultSizeOffset)
        response = requests.post(logStashURL, json.dumps(postQuery))
        resultSet = json.loads(response.content)["hits"]["hits"]
        curResultSize = len(resultSet)
        if curResultSize == 0: break
        print("Inserting {0} records into Postgres".format(curResultSize))
        for result in resultSet:
            data = result["_source"]
            totSegmentLength = 0
            data["request_uri_path"] = urllib2.unquote(data["request_uri_path"])
            if "/segments/" in data["request_uri_path"]:
                try:
                    segments = data["request_uri_path"].split("/segments/")[1].split("/variants")[0].split(",")
                    for segment in segments:
                        segment = segment.strip()
                        if segment:
                            segmentLBUB = segment.split(":")[1].split("-")
                            segmentLength = float(segmentLBUB[1]) - float(segmentLBUB[0])
                            if (segmentLength > 0): totSegmentLength += segmentLength
                except Exception:
                    pass
            resultCursor.execute("insert into public.ws_traffic values (%s, %s,%s,%s,%s,%s,%s,%s,%s, "
                                 "cast(%s as timestamp with time zone),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
                                 (data["@timestamp"], data["@timestamp"], data["type"],
                                data["host"],
                                data["path"],
                                data["syslog_pri"],
                                data["syslog_timestamp"],
                                data["syslog_hostname"],
                                data["remote_host"],
                                data["request_timestamp"],
                                data["client"],
                                data["bytes_out"],
                                data["bytes_in"],
                                data["duration"],
                                data["pool_name"],
                                data["server_node"],
                                data["user_agent"],
                                data["request_type"],
                                data["http_status"] if "http_status" in data else '',
                                data["is_https"],
                                data["virtual_host"],
                                data["request_uri_path"],
                                data["request_query"] if "request_query" in data else '',
                                data["cookie_header"] if "cookie_header" in data else '', totSegmentLength))
        postgresConnHandle.commit()
        if (curResultSize < resultChunkSize): break
        resultSizeOffset += resultChunkSize

except Exception:
    traceback.print_exc()
finally:
    resultCursor.close()
    postgresConnHandle.close()