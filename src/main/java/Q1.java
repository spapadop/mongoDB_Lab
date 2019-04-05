import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;

public class Q1 {

	private static MongoClient mongoClient;
	private static MongoDatabase db;
	private static MongoCollection<Document> peopleColl;
	private static MongoCollection<Document> companyColl;
	private static MongoCursor<Document> cursor;

	public static void execute() {
		mongoClient = new MongoClient();
		db = mongoClient.getDatabase("test");
		peopleColl = db.getCollection("People");
		companyColl = db.getCollection("Companies");
		cursor = peopleColl.find().iterator();

		Document person;
		Document comp;
		String domain = "not found";
		try {
			while (cursor.hasNext()) {
				person = cursor.next();
				// bulk load or single load?? --> change to bulk
				comp = companyColl.find(eq("name", person.get("company"))).first();
				if(comp !=null) {
					domain = comp.get("domain").toString();
				}
				System.out.println(person.get("firstname") + " " + person.get("lastname") + "\t\t\t" + domain);
			}
		} finally {
			cursor.close();
		}
		mongoClient.close();
	}
}