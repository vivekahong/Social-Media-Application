package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;


import org.json.JSONObject;
import org.json.JSONArray;

public class HomepageServlet extends HttpServlet {
    private static MongoDatabase db;
    
    public HomepageServlet() {
            MongoClient mongoClient = new MongoClient("ec2-52-91-184-14.compute-1.amazonaws.com", 27017);
            db = mongoClient.getDatabase("project");
    }

    @Override
    protected void doGet(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {

        String id = request.getParameter("id");
        JSONObject result = new JSONObject();
        final JSONArray posts = new JSONArray();
        
        System.out.println("ID: " + id);
        /*
            Task 3:
            Implement your logic to return all the posts authored by this user.
            Return this posts as-is, but be cautious with the order.

            You will need to sort the posts by Timestamp in ascending order
	     (from the oldest to the latest one). 
        */
      
        // Query to get all post base on user id
       FindIterable<Document> iterable = db.getCollection("posts").find(
                new Document("uid", Integer.parseInt(id))).sort(new Document("timestamp", 1));
        
       // Iterate through all results
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                JSONObject temp = new JSONObject();
                temp.put("content", (String) document.get("content"));
                temp.put("timestamp", (String) document.get("timestamp"));
                temp.put("uid", document.get("uid"));
                temp.put("name", (String) document.get("name"));
                temp.put("image", (String) document.get("image"));
                temp.put("pid",  document.get("pid"));
                temp.put("comments", document.get("comments"));
                temp.put("profile", (String) document.get("profile"));
                System.out.println("User: " + document.get("uid") + " Timestamp: " + document.get("timestamp"));
                posts.put(temp);
            }
        });
        
        result.put("posts", posts);
        
        PrintWriter writer = response.getWriter();           
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, 
            final HttpServletResponse response) throws ServletException, IOException {
        doGet(request, response);
    }
}