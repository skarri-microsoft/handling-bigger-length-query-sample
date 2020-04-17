package microsoft.azure.cosmosdb;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import java.io.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.mongodb.client.model.Filters.gt;

/**
 * Hello world!
 *
 */
public class App 
{
    private static ConfigSettings configSettings = new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    private static int MaxRetries = 10;
    public static void main( String[] args ) throws Exception {
        configSettings.Init();
        InitMongoClient36();

        // Insert data
//        ExecuteMethod(new Callable<Void>() {
//            public Void call() throws Exception {
//                InsertTestData();
//                return null;
//            }
//        });

        ExecuteMethod(new Callable<Void>() {
            public Void call() throws Exception {
                DistributedQueryTest();
                return null;
            }
        });

//        ExecuteMethod(new Callable<Void>() {
//            public Void call() throws Exception {
//                ReproBiggerLengthQueryIssueFromCustomerSample();
//                return null;
//            }
//        });


    }
    public static void ExecuteMethod(
            Callable<Void> callable) throws Exception {
        long start = System.currentTimeMillis();
        callable.call();
        long finish = System.currentTimeMillis();
        System.out.println("Total time taken to execute this method in milliseconds : " + (finish - start));
    }
    private static void InsertTestData() throws Exception {

        int maxNumberOfItems=1;
        String items="";
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<maxNumberOfItems;i++)
        {

            Document index=Document.parse("{item:1}");
            Document d=Document.parse("{item:"+i+"}");
            mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                    .getCollection(configSettings.getCollName()).createIndex(index);
            mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                    .getCollection(configSettings.getCollName()).insertOne(d);

        }

    }
    private static void ReproBiggerLengthQueryIssueFromCustomerSample() throws Exception {

        List<Document> records=new ArrayList<>();
        byte[] byString=Files.readAllBytes(Paths.get("C:\\springdata\\handling-bigger-length-query-sample\\src\\main\\microsoft\\azure\\cosmosdb\\s.txt"));
        String content=new String(byString,  Charset.defaultCharset());
        BsonDocument query=BsonDocument.parse(content);
        FindIterable<Document> iterable=mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName()).find(query);

        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext())
        {
            records.add(cursor.next());

        }
    }
    private static void ReproBiggerLengthQueryIssue2() throws Exception {

        List<Document> records=new ArrayList<>();
        int maxNumberOfItems=500000;
        String items="";
        StringBuilder sb=new StringBuilder();
        //int maxLimit=262144;
        int maxLimit=20000;
        String queryTemplate="{ item: { $in: [ %s ] } }";
        int querySize=queryTemplate.length()-2;
        sb.append("{ ");
        for(int i=1;i<maxNumberOfItems;i++)
        {
            String nextElement="\""+"item"+i+"\":"+i+",";
            if(querySize+nextElement.length()>maxLimit)
            {
                break;
            }
            querySize=querySize+nextElement.length();
            sb.append(nextElement);
        }
        sb.append("\""+"item"+maxNumberOfItems+"\":"+maxNumberOfItems);
        sb.append(" }");
        String content=sb.toString();
        Files.write(Paths.get("query2.txt"),content.getBytes());
        System.out.println("Content size in kb: "+(content.length()/1000));
        BsonDocument query=BsonDocument.parse(content);
        FindIterable<Document> iterable=mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName()).find(query);

        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext())
        {
            records.add(cursor.next());

        }
        System.out.println("Number of records loaded: "+records.size());
    }

    private static void DistributedQueryTest() throws Exception {

        int maxLimit=1000;
        DistributeQuery(Event.GetSampleEvents(100000),10000);
    }

    private static void DistributeQuery(List<Event> events, int maxQueryLength) throws Exception {

        CopyOnWriteArrayList<Document> records=new CopyOnWriteArrayList<>();
        List<DBObject> parallelQueries=new ArrayList<>();
        List<DBObject> batch=new ArrayList<>();
        int itemCount=0;
        int querySize=0;
        int totalQuerySize=0;
        while(itemCount<events.size())
        {
            Event sampleEvent= events.get(itemCount);
            if(querySize+sampleEvent.sampleAndConditionLength<maxQueryLength)
            {
                // Add it to the query
                totalQuerySize=totalQuerySize+sampleEvent.sampleAndConditionLength;

                querySize=querySize+sampleEvent.sampleAndConditionLength;
                batch.add(sampleEvent.sampleAndCondition);
                itemCount++;
            }
            else
            {
              parallelQueries.add(new BasicDBObject("$or", batch));

              // Reset base variables
              batch=new ArrayList<>();
              querySize=0;
            }

        }

        System.out.println("Total query size sending in parallel mode in kbs: "+ totalQuerySize/1000);

        parallelQueries
                .parallelStream()
                .forEach(query ->
                        {
                            String content=((BasicDBObject) query).toJson();
                            BsonDocument findQuery=BsonDocument.parse(content);
                            try {
                                FindIterable<Document> iterable = mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                                        .getCollection(configSettings.getCollName()).find(findQuery);

                                MongoCursor<Document> cursor = iterable.iterator();
                                while (cursor.hasNext()) {
                                    records.add(cursor.next());
                                }
                            }catch(Exception ex)
                            {
                                // Throttling needs to be handled if server side retries not enabled
                                System.out.print(ex.getMessage());
                            }

                        }
                        );

    }

    private static void ReproBiggerLengthQueryIssue() throws Exception {

        List<Document> records=new ArrayList<>();
        int maxNumberOfItems=500000;
        String items="";
        StringBuilder sb=new StringBuilder();
        //int maxLimit=262144;
        int maxLimit=3272144;
        String queryTemplate="{ item: { $in: [ %s ] } }";
        int querySize=queryTemplate.length()-2;

        for(int i=1;i<maxNumberOfItems;i++)
        {
            String nextElement="\""+i+"\""+", ";
            if(querySize+nextElement.length()>maxLimit)
            {
                break;
            }
            querySize=querySize+nextElement.length();
            sb.append(nextElement);
        }
        sb.append(maxNumberOfItems+"");
        String content="{ item: { $in: [ "+sb.toString()+" ] } }";
        Files.write(Paths.get("query1.txt"),content.getBytes());
        System.out.println("Content size in kb: "+(content.length()/1000));
        BsonDocument query=BsonDocument.parse(content);
        FindIterable<Document> iterable=mongoClientExtension.GetClient().getDatabase(configSettings.getDbName())
                .getCollection(configSettings.getCollName()).find(query);

        MongoCursor<Document> cursor = iterable.iterator();
        while(cursor.hasNext())
        {
            records.add(cursor.next());

        }
    }
    private static void InitMongoClient36() {
        mongoClientExtension = new MongoClientExtension();
        mongoClientExtension.InitMongoClient36(
                configSettings.getUserName(),
                configSettings.getPassword(),
                10255,
                true,
                configSettings.getClientThreadsCount()
        );
    }

}
