import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.company.Company;
import com.devskiller.jfairy.producer.person.Person;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.client.MongoCursor;

import java.util.List;
import java.util.Set;

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
		Document person = null;
		Document comp = null;
		int counter = 0;
		String domain = null;
		try {

			while (cursor.hasNext()) {
				person = cursor.next();
				// bulk load or single load?? --> change to bulk
				comp = companyColl.find(eq("name", person.get("company"))).first();
				domain = "not found";
				if(comp !=null) {
					domain = comp.get("name").toString();
				}
				System.out.println("firstname" + person.get("firstname")
						+ " lastname: "+ person.get("lastname") + " domain: "
				);


				counter++;
			}
		} finally {
			cursor.close();
		}
		System.out.println("Read " + counter + "lines");


	}

}