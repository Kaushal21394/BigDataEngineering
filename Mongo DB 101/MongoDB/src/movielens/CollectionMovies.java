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
public class CollectionMovies {
    public static void main(String[] args) throws IOException {
        
        // Step 1. Connect to MongoDB
        MongoClient connection =  MongoClients.create();
        // Step 2. Access the Database
        MongoDatabase db = connection.getDatabase("movielens");
        // Step 3. Select the Collection
        MongoCollection<Document> collection = db.getCollection("movies");
        // Step 4. Create a Document
        File file = new File("C:\\Users\\kaush\\Documents\\NetBeansProjects\\MongoDB\\data\\ml-10M100K\\movies.dat");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String Line = "";
        while ((Line = br.readLine()) != null) {
            String[] words = Line.split("::");
            String[] title = words[1].split("[()]");
            Document doc = new Document();
            doc.append("MovieID", words[0]);
            doc.append("Title",words[1].substring(0,words[1].length()-6));
            doc.append("Year",title[title.length-1]);
            if(words[2].indexOf("|")>=0){
                doc.append("Genre", Arrays.toString(words[2].split("\\|")));
            }
            else{
                doc.append("Genre", words[2]);      
            }
            //Step 5. Insert the Document
            collection.insertOne(doc); 
        }
        
    }
}
