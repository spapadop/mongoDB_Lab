import com.mongodb.MongoClient;
import com.mongodb.client.*;
import util.Utils;
import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;

public class Q2 {

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
		cursor = companyColl.find().iterator();

		AggregateIterable<Document> q2 = companyColl.aggregate(Arrays.asList(
				new Document("$lookup", new Document()
						.append("from","People")
						.append("localField","name")
						.append("foreignField","company")
						.append("as","employees")
				),
				new Document("$project", new Document()
						.append("_id",1)
						.append("employees", new Document("$size","$employees"))
				)
		));

		System.out.println("Q2: For each company, the name and the number of employees");
		for (Document d : q2 ) {
			System.out.println(d.get("_id") + " has " + d.get("employees")
					+ (d.getInteger("employees") == 1 ? " employee." : " employees."));
		}

		mongoClient.close();
	}
}
