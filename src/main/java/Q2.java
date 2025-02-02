import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Arrays;

public class Q2 {

    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static MongoCollection<Document> peopleColl;
    private static MongoCollection<Document> companyColl;
    private static MongoCursor<Document> cursor;

    public static void open() {
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test");
        peopleColl = db.getCollection("People");
        companyColl = db.getCollection("Companies");
    }

    public static void execute() {
        open();
        cursor = companyColl.find().iterator();
        AggregateIterable<Document> q2 = companyColl.aggregate(Arrays.asList(
                new Document("$lookup", new Document()
                        .append("from", "People")
                        .append("localField", "_id")
                        .append("foreignField", "worksIn")
                        .append("as", "employees")
                ),
                new Document("$project", new Document()
                        .append("_id", 1)
                        .append("employees", new Document("$size", "$employees"))
                ),
                new Document("$sort", new Document()
                        .append("employees", -1)
                )
        ));

        System.out.println("Q2: For each company, the name and the number of employees");

        for (Document d : q2) {
            System.out.println(d.get("_id") + " has " + d.get("employees")
                    + (d.getInteger("employees") == 1 ? " employee." : " employees."));
        }

        mongoClient.close();
    }
}
