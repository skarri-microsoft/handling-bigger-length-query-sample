package microsoft.azure.cosmosdb;

import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MongoClientExtension {

    private String userName;
    private String password;
    private int port;
    private MongoClient mongoClient;
    private Boolean isSslEnabled;
    private int clientThreadsCount;

    public MongoClientExtension(){}
    public MongoClientExtension(
            String userName,
            String password,
            int port,
            Boolean isSslEnabled,
            int clientThreadsCount)
    {
        this.userName=userName;
        this.password=password;
        this.port=port;
        this.isSslEnabled=isSslEnabled;
        this.clientThreadsCount=clientThreadsCount;
        InitClient();
    }

    public void InitMongoClient36(
            String userName,
            String password,
            int port,
            Boolean isSslEnabled,
            int clientThreadsCount)
    {
        this.userName=userName;
        this.password=password;
        this.port=port;
        this.isSslEnabled=isSslEnabled;
        this.clientThreadsCount=clientThreadsCount;

        this.mongoClient = new MongoClient(new MongoClientURI(GetMongoEndpoint()));

    }
    public MongoClient GetClient()
    {
        return this.mongoClient;
    }

    public String GetMongoEndpoint()
    {
        return  String.format(
                "mongodb://%s:%s@%s.mongo.cosmos.azure.com:%s/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@%s@",
                this.userName,
                this.password,
                this.userName,
                this.port,
                this.userName);
    }
    public String GetMongoHost()
    {
        return  String.format(
                "%s.mongo.cosmos.azure.com",
                this.userName);
    }


    public void InitClient()
    {
        MongoClientOptions clientOptions = MongoClientOptions.builder()
                .connectionsPerHost(this.clientThreadsCount)
                .sslEnabled(isSslEnabled)
                .build();

        MongoCredential cred =
                MongoCredential.createCredential(
                        this.userName,
                        "globaldb",
                        this.password.toCharArray());
        List<MongoCredential> credentials = new ArrayList<MongoCredential>();
        credentials.add(cred);

        ServerAddress serverAddress= new ServerAddress(this.GetMongoHost(), this.port);
        this.mongoClient = new MongoClient(serverAddress, credentials, clientOptions);
    }

    public Document FindOne(String dbName, String CollectionName)
    {
        return this.mongoClient.getDatabase(dbName).getCollection(CollectionName).find().first();
    }

    public UpdateResult UpdateOne(String dbName, String CollectionName, BsonDocument findFilter, Bson updateFields)
    {
        return this.mongoClient.getDatabase(dbName).getCollection(CollectionName).updateOne(findFilter,updateFields);
    }

    public RuCharge GetLatestOperationRus(String dbName)
    {
        BsonDocument cmd = new BsonDocument();
        cmd.append("getLastRequestStatistics", new BsonInt32(1));
        Document requestChargeResponse = this.mongoClient.getDatabase(dbName).runCommand(cmd);
        if(requestChargeResponse.containsKey("RequestCharge"))
        {
            return new RuCharge(
                    requestChargeResponse.getString("CommandName"),
                    requestChargeResponse.getDouble("RequestCharge"));
        }
        return null;
    }

    public void UpdateRus(String dbName,String collName,int rus)
    {
        Document cmd = new Document("customAction", "UpdateCollection").
                append("collection", collName).append("offerThroughput", rus);
        Document output = this.mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    public void ShowRus(String dbName,String collName)
    {
        Document cmd = new Document("customAction", "GetCollection").
                append("collection", collName);
        Document output = this.mongoClient.getDatabase(dbName).runCommand(cmd);
        System.out.println("Response: "+output);

    }

    //db.runCommand({customAction:"createCollection",collection:"test2", shardKey:"_id",offerThroughput:20000})
    public void CreatePartitionedCollection(String dbName, String collectionName, String partitionKey)
    {
        System.out.println(
                String.format(
                        "Creating partitioned collection: %s in db:%s with partition key: %s",
                        collectionName,
                        dbName,
                        partitionKey));

        BsonDocument cmd = new BsonDocument();
        String fullCollName = dbName+"."+collectionName;
        cmd.append("shardCollection", new BsonString(fullCollName));
        BsonDocument keyDoc = new BsonDocument();
        keyDoc.append(partitionKey, new BsonString("hashed"));
        cmd.append("key", keyDoc);
        this.mongoClient.getDatabase(dbName).runCommand(cmd);
    }

    public void PrintIndexes()
    {
        ListIndexesIterable<Document> indexes= this.mongoClient.getDatabase("").getCollection("").listIndexes();

        for (Document doc : indexes) {

            System.out.println(doc.toJson());
        }

    }


    public void CreatePartitionedCollection(
            String dbName,
            String collectionName,
            String partitionKey,
            int rus,
            String[] indexes)
    {
        System.out.println(
                String.format(
                        "Creating partitioned collection: %s in db:%s with partition key: %s",
                        collectionName,
                        dbName,
                        partitionKey));

        BsonDocument cmd = new BsonDocument();
        cmd.append("customAction",new BsonString("createCollection"))
                .append("collection",new BsonString(collectionName))
                .append("shardKey",new BsonString(partitionKey))
                .append("offerThroughput",new BsonInt32(rus));
        this.mongoClient.getDatabase(dbName).runCommand(cmd);

        if(indexes!=null)
        {
            this.mongoClient.getDatabase(dbName).getCollection(collectionName).dropIndexes();
            for (String indexTerm:indexes) {

                this.mongoClient.getDatabase(dbName).getCollection(collectionName).createIndex(new BsonDocument().append(indexTerm,new BsonInt32(1)));
            }
        }
    }

    public void BulkWrite(
            List<InsertOneModel<Document>> docs,
            BulkWriteOptions bulkWriteOptions,
            String dbName,
            String collectionName)
    {
        this.mongoClient.getDatabase(dbName).getCollection(collectionName).bulkWrite(docs,bulkWriteOptions);
    }

    public void InsertOne(String dbName,String collectionName,Document docToInsert)
    {
        this.mongoClient.
                getDatabase(dbName).
                getCollection(collectionName).
                insertOne(docToInsert);
    }

    public boolean IsCollectionExists(String dbName, String collectionName)
    {
        MongoIterable<String> colls= this.mongoClient.getDatabase(dbName).listCollectionNames();
        for (String s : colls) {

            if(s.equals(collectionName))
            {
                return true;
            }
        }
        return  false;
    }

    public ArrayList<Document> Find(String dbName,String collectionName)
    {
        FindIterable<Document> documents = this.mongoClient.getDatabase(dbName).getCollection(collectionName).find();

        return Lists.newArrayList(documents);

    }

    public ArrayList<Document> FindSkipLimit(String dbName,String collectionName,int skip,int limit)
    {
        FindIterable<Document> documents =
                this.mongoClient.getDatabase(dbName).getCollection(collectionName).
                        find().skip(skip).limit(limit);

        return Lists.newArrayList(documents);

    }



    public void GetDocumentsCount(String dbName,String collectionName)
    {
        List<Bson> filters = new ArrayList<>();
        this.mongoClient.getDatabase(dbName).getCollection(collectionName).aggregate(
                Arrays.asList(
                        Aggregates.match(new BsonDocument() {})
                )
        ).allowDiskUse(true).forEach(printBlock);
    }

    Block<Document> printBlock = new Block<Document>() {
        @Override
        public void apply(final Document document) {
            System.out.println(document.toJson());
        }
    };
}
