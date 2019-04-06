import com.mongodb.client.AggregateIterable;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;

import java.util.Arrays;
import java.util.List;


public class Q1 {

    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static MongoCollection<Document> peopleColl;
    private static MongoCollection<Document> companyColl;
    private static MongoCursor<Document> cursor;

    public static void open() {
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test");
        peopleColl = db.getCollection("People");
    }

    public static void execute() {
        open();
        cursor = peopleColl.find().iterator();

        Document person;
        Document comp;
        String domain = "not found";
        try {
            while (cursor.hasNext()) {
                person = cursor.next();

                AggregateIterable<Document> q1 = peopleColl.aggregate(Arrays.asList(
                        new Document("$lookup", new Document()
                                .append("from", "Companies")
                                .append("localField", "worksIn")
                                .append("foreignField", "_id")
                                .append("as", "result")
                        )
                ));

                for (Document d : q1) {
                    Document company = (Document) d.get("result", List.class).get(0);
                    System.out.println(d.get("firstName") + " " + d.get("lastName") + " works in " + company.get("_id"));
                }

                // bulk load or single load?? --> change to bulk
//				comp = companyColl.find(eq("_id", person.get("worksIn"))).first();
//				if(comp !=null) {
//					domain = comp.get("domain").toString();
//				}
//				System.out.println(person.get("firstName") + " " + person.get("lastName") + " works in " + domain);
            }
        } finally {
            cursor.close();
        }
        mongoClient.close();
    }
}