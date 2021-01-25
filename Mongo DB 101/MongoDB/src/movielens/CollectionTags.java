/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package movielens;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import org.bson.Document;
/**
 *
 * @author kaush
 */
public class CollectionTags {
        public static void main(String[] args) throws IOException {
        // Step 1. Connect to MongoDB
        MongoClient connection =  MongoClients.create();
        // Step 2. Access the Database
        MongoDatabase db = connection.getDatabase("movielens");
        // Step 3. Select the Collection
        MongoCollection<Document> collection = db.getCollection("tags");
        // Step 4. Create a Document
        File file = new File("C:\\Users\\kaush\\Documents\\NetBeansProjects\\MongoDB\\data\\ml-10M100K\\tags.dat");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String Line = "";
        while ((Line = br.readLine()) != null) {
            String[] words = Line.split("::");
            Document doc = new Document();
            doc.append("UserID", words[0]);
            doc.append("MovieID",words[1]);
            doc.append("Tag", words[2]);      
            doc.append("Timestamp", words[3]);
            //Step 5. Insert the Document
            collection.insertOne(doc); 
        }
        
    }    
}
