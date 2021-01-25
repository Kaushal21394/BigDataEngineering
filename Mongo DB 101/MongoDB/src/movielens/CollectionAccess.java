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
public class CollectionAccess {
    public static void main(String[] args) throws IOException {
        // Step 1. Connect to MongoDB
        
        MongoClient connection =  MongoClients.create();
        // Step 2. Access the Database
        MongoDatabase db = connection.getDatabase("movielens");
        // Step 3. Select the Collection
        MongoCollection<Document> collection = db.getCollection("access");
        // Step 4. Create a Document
        File file = new File("C:\\Users\\kaush\\Documents\\NetBeansProjects\\MongoDB\\data\\access.log");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String Line = "";
        while ((Line = br.readLine()) != null) {
            String[] words = Line.split("[\\[ - - \" :\\] ]");
            
            Document doc = new Document();
            Document date = new Document();
            
            date.append("Day",words[6].split("/")[0]);
            date.append("Month",words[6].split("/")[1]);
            date.append("Year",words[6].split("/")[2]);
            
            doc.append("IP", words[0]);
            doc.append("Date",date);
            doc.append("Request", words[14]);        
            doc.append("Website", words[15]);
            doc.append("Status", words[17]);
            
            //Step 5. Insert the Document
            collection.insertOne(doc); 

        }
        
    }
    
}
