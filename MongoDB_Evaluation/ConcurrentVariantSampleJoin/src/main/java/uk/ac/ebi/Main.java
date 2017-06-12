package uk.ac.ebi;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Projections;
import org.bson.BsonDocument;
import org.bson.Document;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalDateTime;
import java.net.URLEncoder;
import java.util.stream.IntStream;

public class Main {
    private static final ConcurrentHashMap<Integer,Vector<Document>> conHashMap = new ConcurrentHashMap<Integer,Vector<Document>>();
    public static void processStream(int batchNumber, MongoCollection variantCollHandle, MongoCollection sampleCollHandle, String chromosome, int startFirstPos, int step, int endLastPos)
    {
        LocalDateTime startTime = LocalDateTime.now();
        final BasicDBObject varQueryCrit = new BasicDBObject("chr", chromosome);
        varQueryCrit.put("start", new BasicDBObject("$gt", startFirstPos).append("$lte", startFirstPos+step));
        varQueryCrit.put("end", new BasicDBObject("$gte", startFirstPos).append("$lt", endLastPos));
        final BasicDBObject chrSortCrit = new BasicDBObject("chr", 1).append("start", 1);

        MongoCursor<Document> varCursor = variantCollHandle.find(varQueryCrit).projection(Projections.exclude("files")).sort(chrSortCrit).batchSize(1000).iterator();
        MongoCursor<Document> sampleCursor = sampleCollHandle.find(varQueryCrit).sort(chrSortCrit).batchSize(1000).iterator();

        Vector<Document> joinDocument = new Vector<Document>();

        boolean moveSampleCursor = true;
        int numRecords = 0;
        Document sampleDoc = new Document();

        while(varCursor.hasNext())
        {
            Document varDoc = (Document)varCursor.next();
            if (sampleCursor.hasNext())
            {
                if (moveSampleCursor) {sampleDoc = (Document) sampleCursor.next();}
                if (sampleDoc.get("_id").toString().equals(varDoc.get("_id").toString()))
                {
                    varDoc.put("sample", sampleDoc);
                    moveSampleCursor = true;
                }
                else
                {
                    moveSampleCursor = false;
                }
            }
            joinDocument.add(varDoc);
            numRecords += 1;
        }
        LocalDateTime endTime = LocalDateTime.now();
        long elapsedTime = Duration.between (startTime, endTime).getSeconds();
        conHashMap.put(batchNumber, joinDocument);
        System.out.println("Joined " + numRecords + " for batch " + batchNumber + " in " + elapsedTime + " seconds");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Scanner sc = new Scanner(System.in);
        String mongoProdHost = sc.next();
        String mongoProdUser = sc.next();
        String mongoProdPwd = sc.next();

        MongoClient localMongoClient = new MongoClient(new MongoClientURI("mongodb://172.22.69.141:27017/"));
        MongoClient prodMongoClient = new MongoClient(new MongoClientURI(String.format("mongodb://%s:%s@%s:27017/admin",mongoProdUser, URLEncoder.encode(mongoProdPwd) , mongoProdHost)));
        MongoCollection variantCollHandle = prodMongoClient.getDatabase("eva_hsapiens_grch37").getCollection("variants_1_2");
        MongoCollection sampleCollHandle = localMongoClient.getDatabase("eva_testing").getCollection("sample_enc");

        String chromosome = "10";
        int numScans = 0;
        int totalNumRecords = 0;
        int mongoCumulativeExecTime = 0;
        int margin = 1000000;
        Random random = new Random();
        int lowerBound = 60222;
        int upperBound = 135524743;
        int pos =  random.nextInt (upperBound - lowerBound) + lowerBound;
        int step = 20000;
        int endLastPos = pos + margin + margin;
        ExecutorService executor = Executors.newFixedThreadPool(7);
        Vector<Future> futures = new Vector<Future>();
        int batchNumber = 0;

        for (int i = pos; i < pos+margin+margin; i += step) {
            final int startPos = i;
            final int currBatchNum = batchNumber;
            batchNumber += 1;
            futures.add(executor.submit(new Runnable() {
                @Override
                public void run() {
                    processStream(currBatchNum, variantCollHandle, sampleCollHandle, chromosome, startPos, step, endLastPos);
                }
            }));
        }


        int currNumRecs = 0;
        LocalDateTime startTime = LocalDateTime.now();
        for (int i = 0; i < 100; i++) {
            while(!(conHashMap.containsKey(i))) {}
            currNumRecs = conHashMap.get(i).size();
            System.out.println("Got batch " + i + " with " + currNumRecs + " records");
            totalNumRecords += currNumRecs;
        }
        LocalDateTime endTime = LocalDateTime.now();
        System.out.println("Returned " + totalNumRecords + " records in " + Duration.between(startTime, endTime).getSeconds() + " seconds");


//        for (Future futureObj:futures) {
//            futureObj.get();
//            futureObj.wait();
//        }
        }
}
