import requests, json, os
import psycopg2

# Citus credentials
postgresConnHandle = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password=''")
resultCursor = postgresConnHandle.cursor()

mostRecentTimeStamp = None
resultCursor.execute("select request_attr -> '_source' ->> '@timestamp' as recent_ts from public.kibana_ws_logs order by request_attr -> '_source' ->> '@timestamp' desc limit 1")
results = resultCursor.fetchall()
if results:
     mostRecentTimeStamp = results[0]

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
        resultCursor.execute("insert into public.kibana_ws_logs values ('{0}');".format(json.dumps(result)))
    postgresConnHandle.commit()
    if (curResultSize < resultChunkSize): break
    resultSizeOffset += resultChunkSize

resultCursor.close()
postgresConnHandle.close()