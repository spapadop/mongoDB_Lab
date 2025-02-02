import com.devskiller.jfairy.Fairy;
import com.devskiller.jfairy.producer.company.Company;
import com.devskiller.jfairy.producer.person.Person;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class populateDBmemory {

    private static MongoClient mongoClient;
    private static MongoDatabase db;
    private static MongoCollection<Document> peopleColl;
    private static MongoCollection<Document> personCoColl;
    private static MongoCollection<Document> companyColl;

    private static Fairy fairy;
    private static Person person;
    private static Company company;

    private static List<Document> personDocs;
    private static List<Document> compDocs;
    private static Set<String> comps;
    private static Set<String> people;


    private static void init() {
        mongoClient = new MongoClient();
        db = mongoClient.getDatabase("test2");

        peopleColl = db.getCollection("person");
        companyColl = db.getCollection("company");
        personCoColl = db.getCollection("personCo");

        peopleColl.drop();
        companyColl.drop();
        personCoColl.drop();

        personDocs = new ArrayList<Document>();
        compDocs = new ArrayList<Document>();
        comps = new HashSet<String>();
        people = new HashSet<String>();

        fairy = Fairy.create();
    }


    public static void populate(int N) {
        init();

        for (int i = 0; i < N; i++) {
            person = fairy.person();
            company = person.getCompany();

            if (!people.contains(person.getPassportNumber())) {
                people.add(person.getPassportNumber());
                Document doc = new Document("_id", person.getPassportNumber())
                        .append("firstName", person.getFirstName())
                        .append("lastName", person.getLastName())
                        .append("email", person.getEmail())
                        .append("age", person.getAge())
                        .append("worksIn", company.getName());
                personDocs.add(doc);
            }

            if (!comps.contains(company.getName())) {
                comps.add(company.getName());
                Document doc2 = new Document("_id", company.getName())
                        .append("domain", company.getDomain())
                        .append("email", company.getEmail())
                        .append("url", company.getUrl());
                compDocs.add(doc2);
            }
        }
        System.out.println("Trying to insert " + personDocs.size() + " People and  " + compDocs.size() + " Companies");

        peopleColl.insertMany(personDocs);
        companyColl.insertMany(compDocs);

        System.out.println("Finished Insert method");
        mongoClient.close();
    }

}