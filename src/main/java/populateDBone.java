import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.company.Company;
import com.devskiller.jfairy.producer.person.Person;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class populateDBone {

    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static MongoCollection<Document> peopleColl;
    private static MongoCollection<Document> companyColl;

    private static Fairy fairy;
    private static Person person;
    private static Company company;


    private static void init() {
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test");

        peopleColl = db.getCollection("People");
        companyColl = db.getCollection("Companies");
        peopleColl.drop();
        companyColl.drop();

        fairy = Fairy.create();
    }


    public static void populate(int N) {
        init();

        for (int i = 0; i < N; i++) {
            person = fairy.person();
            company = person.getCompany();

            // we better use the passportNumber as _id, rather than letting mongoDB define it.
            Document personDocument = new Document();
            personDocument.put("_id", person.getPassportNumber());

            if (peopleColl.countDocuments(personDocument) == 0) {
                personDocument.put("firstName", person.getFirstName());
                personDocument.put("lastName", person.getLastName());
                personDocument.put("email", person.getEmail());
                personDocument.put("age", person.getAge());
                personDocument.put("worksIn", company.getName());
                personDocument.put("nationality", person.getNationality().toString());
                personDocument.put("sex", person.getSex().toString());

                peopleColl.insertOne(personDocument);
            }

            Document companyDocument = new Document();
            companyDocument.put("_id", company.getName());
            if (companyColl.countDocuments(companyDocument) == 0) {
                companyDocument.put("domain", company.getDomain());
                companyDocument.put("email", company.getEmail());
                companyDocument.put("url", company.getUrl());

                companyColl.insertOne(companyDocument);
            }
        }

        mongoClient.close();
    }
}