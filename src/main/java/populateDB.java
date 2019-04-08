import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.company.Company;
import com.devskiller.jfairy.producer.person.Person;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class populateDB {

    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static MongoCollection<Document> peopleColl;
    private static MongoCollection<Document> personCoColl;
    private static MongoCollection<Document> companyColl;
    private static MongoCollection<Document> companyPeColl;
    private static MongoCollection<Document> companyPeIDColl;

    private static Fairy fairy;
    private static Person person;
    private static Company company;


    private static void init() {
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test2");

        peopleColl = db.getCollection("person");
        companyColl = db.getCollection("company");
        personCoColl = db.getCollection("personCo");
        companyPeColl = db.getCollection("companyPe");
        companyPeIDColl = db.getCollection("companyPeID");

        peopleColl.drop();
        companyColl.drop();
        personCoColl.drop();
        companyPeColl.drop();
        companyPeIDColl.drop();

        fairy = Fairy.create();
    }


    public static void populate(int N) {
        init();

        for (int i = 0; i < N; i++) {
            person = fairy.person();
            company = person.getCompany();

            Document personDocument = new Document();
            personDocument.put("_id", person.getPassportNumber());

            // Insert normal person collection
            if (peopleColl.countDocuments(personDocument) == 0) {
                personDocument.put("firstName", person.getFirstName());
                personDocument.put("lastName", person.getLastName());
                personDocument.put("email", person.getEmail());
                personDocument.put("age", person.getAge());
                personDocument.put("worksIn", company.getName());
                personDocument.put("nationality", person.getNationality().toString());
                personDocument.put("sex", person.getSex().toString());
                peopleColl.insertOne(personDocument);

                // Insert person with embed company collection
                personDocument.append("company", new Document()
                        .append("name", company.getName())
                        .append("domain", company.getDomain())
                        .append("email", company.getEmail())
                        .append("url", company.getUrl())
                );
                personCoColl.insertOne(personDocument);
            }

            // Insert normal company collection
            Document companyDocument = new Document();
            companyDocument.put("_id", company.getName());
            if (companyColl.countDocuments(companyDocument) == 0) {
                companyDocument.put("domain", company.getDomain());
                companyDocument.put("email", company.getEmail());
                companyDocument.put("url", company.getUrl());
                companyColl.insertOne(companyDocument);

                // Insert company with embed person collection
                List<BasicDBObject> ppl = new ArrayList<>();
                ppl.add(new BasicDBObject("_id", person.getPassportNumber())
                            .append("firstName", person.getFirstName())
                            .append("lastName", person.getLastName())
                            .append("email", person.getEmail())
                            .append("age", person.getAge())
                            .append("worksIn", company.getName())
                            .append("nationality", person.getNationality().toString())
                            .append("sex", person.getSex().toString())
                );
                companyDocument.put("people", ppl);
                companyPeColl.insertOne(companyDocument);

                // insert company with list of person IDs
                List<String> ppl2 = new ArrayList<>();
                ppl2.add(person.getPassportNumber());
                companyDocument.put("peopleID", ppl2);
                companyPeIDColl.insertOne(companyDocument);

            } else {
                // Insert company with embed person collection
                BasicDBObject match = new BasicDBObject();
                match.put("_id", companyDocument.get("_id")); //to find the relevant document

                Document pers = new Document(); //create the new entry
                pers.put("_id", person.getPassportNumber());
                pers.put("firstName", person.getFirstName());
                pers.put("lastName", person.getLastName());
                pers.put("email", person.getEmail());
                pers.put("age", person.getAge());
                pers.put("worksIn", company.getName());
                pers.put("nationality", person.getNationality().toString());
                pers.put("sex", person.getSex().toString());

                BasicDBObject update = new BasicDBObject();
                update.put( "$push", new BasicDBObject( "people", pers) );
                companyPeColl.updateOne(match, update);

                // insert company with list of person IDs
                BasicDBObject match2 = new BasicDBObject();
                match2.put("_id", companyDocument.get("_id")); //to find the relevant document
                BasicDBObject update2 = new BasicDBObject();
                update2.put( "$push", new BasicDBObject( "peopleID", person.getPassportNumber()) );
                companyPeIDColl.updateOne(match, update);
                companyPeIDColl.updateOne(match2, update2);
            }
        }
        mongoClient.close();
    }
}