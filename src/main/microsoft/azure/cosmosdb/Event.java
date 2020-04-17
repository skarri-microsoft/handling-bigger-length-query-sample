package microsoft.azure.cosmosdb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

public class Event {

    public String subClassId;
    public String classId;
    public String departmentId;
    public String divisionId;
    public int sampleAndConditionLength;
    public DBObject sampleAndCondition;

    public static Event Get() {
        Random rand = new Random();
        int upperBound = 100;
        Event event = new Event();

        event.subClassId = Integer.toString(rand.nextInt(upperBound));
        event.classId = Integer.toString(rand.nextInt(upperBound));
        event.departmentId = Integer.toString(rand.nextInt(upperBound));
        event.divisionId = Integer.toString(rand.nextInt(upperBound));
        List<DBObject> criteria = new ArrayList<DBObject>();
        criteria.add(new BasicDBObject("subClassId", new BasicDBObject("$eq", event.subClassId)));
        criteria.add(new BasicDBObject("classId", new BasicDBObject("$eq", event.classId)));
        criteria.add(new BasicDBObject("departmentId", new BasicDBObject("$eq", event.departmentId)));
        criteria.add(new BasicDBObject("divisionId", new BasicDBObject("$eq", event.divisionId)));
        event.sampleAndCondition= new BasicDBObject("$and", criteria);
        event.sampleAndConditionLength=((BasicDBObject) event.sampleAndCondition).toJson().length();
        return event;
    }

    public static List<Event> GetSampleEvents(int size) {
        CopyOnWriteArrayList<Event> events=new CopyOnWriteArrayList<>();
        Random rand = new Random();
        int upperBound = 100;
        IntStream.range(1, size).parallel().forEach(i->
                {
                    Event event = new Event();
                    event.subClassId = Integer.toString(rand.nextInt(upperBound));
                    event.classId = Integer.toString(rand.nextInt(upperBound));
                    event.departmentId = Integer.toString(rand.nextInt(upperBound));
                    event.divisionId = Integer.toString(rand.nextInt(upperBound));
                    List<DBObject> criteria = new ArrayList<DBObject>();
                    criteria.add(new BasicDBObject("subClassId", new BasicDBObject("$eq", event.subClassId)));
                    criteria.add(new BasicDBObject("classId", new BasicDBObject("$eq", event.classId)));
                    criteria.add(new BasicDBObject("departmentId", new BasicDBObject("$eq", event.departmentId)));
                    criteria.add(new BasicDBObject("divisionId", new BasicDBObject("$eq", event.divisionId)));
                    event.sampleAndCondition = new BasicDBObject("$and", criteria);
                    event.sampleAndConditionLength = ((BasicDBObject) event.sampleAndCondition).toJson().length();
                    events.add(event);
                }
        );

        return events;
    }
}
