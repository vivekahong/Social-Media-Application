package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.HTable;

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

import org.json.JSONObject;

import org.json.JSONArray;

public class TimelineServlet extends HttpServlet {
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.41.233";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "followers";
    private static String tableName2 = "followees";
    /**
     * HTable handler.
     */
    private static HTableInterface followersTable;
    private static HTableInterface followeesTable;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    private static HConnection conn2;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();
    private static Configuration conf;
    
    /**
     * Database set up
     */
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project";
    private static final String URL = "jdbc:mysql://project.cicfpnjbqg7h.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;
    private static Connection conn1;
    private static final String DB_USER = "root";
    private static final String DB_PWD = "sd15752a";
    
    /**
     * MongoDB set up
     */
    private static MongoDatabase db;
    
    public TimelineServlet() throws Exception {
       
        System.out.println("Task4 started");
        // Initialize HBase
        logger.setLevel(Level.ERROR);
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        
        // If tables are open, close them.
        if (followersTable != null) {
            followersTable.close();
        }
        if (followeesTable != null) {
            followeesTable.close();
        }
        
        // clean up the table
        if (conn != null) {
            conn.close();
        }
        
        // Initialize HBase and table's connection
        try{
            conn = HConnectionManager.createConnection(conf);
           
            followersTable = new HTable(conf, Bytes.toBytes(tableName)); //conn.getTable(Bytes.toBytes(tableName));
            System.out.println("followersTable debugg");
            followeesTable = new HTable(conf, Bytes.toBytes(tableName2)); //conn.getTable(Bytes.toBytes(tableName2));
            System.out.println("followeesTable debugg");
        
        }catch(IOException e){
            e.printStackTrace();
        }
       
        // Initialize MySQL
        try{
            Class.forName(JDBC_DRIVER);
            conn1 = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        }catch(Exception e){
            e.printStackTrace();
        }
        
        // Initialize MongoDB
        MongoClient mongoClient = new MongoClient("ec2-52-91-184-14.compute-1.amazonaws.com", 27017);
        db = mongoClient.getDatabase("project");
    }

    @Override
    protected void doGet(final HttpServletRequest request,
            final HttpServletResponse response) throws ServletException, IOException {
       
        JSONObject result = new JSONObject();
        String id = request.getParameter("id");
        System.out.println("Started ID: " + id);
        /*
            Task 4 (1):
            Get the name and profile of the user as you did in Task 1
            Put them as fields in the result JSON object
        */
        Statement stmt = null;
        int rowCount = -1;
        String name = null;
        String image = null;
        try {
            // Authentication
            stmt = conn1.createStatement();
            
            // SQL query for user information
            String tableName = "userinfo";
            String sql = "SELECT name AS name, profile_image AS image FROM " + tableName + " WHERE user_id = '" + id + "'";
            ResultSet rs1 = stmt.executeQuery(sql);
            if (rs1.next()) {
                name  = rs1.getString("name");
                image = rs1.getString("image");
                System.out.println("User: " + name + " with " + image);
            }
            
            // Put name and profile image into result JSON
            result.put("name", name);
            result.put("profile", image);
            System.out.println("MySQL debugg");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        
        /*
            Task 4 (2);
            Get the follower name and profiles as you did in Task 2
            Put them in the result JSON object as one array
        */
        // Array for followers
        JSONArray followers = new JSONArray();

        // Get Request to HBase
        Get g = new Get(Bytes.toBytes(id));
        System.out.println("Get debugg");
        Result r = followersTable.get(g);
        System.out.println("Result debugg");
        byte[] value = r.getValue(Bytes.toBytes("data"), Bytes.toBytes("followee"));
        String valueStr = Bytes.toString(value);
        System.out.println("Get from Followee: " + valueStr);
      
        // Process retrieved information
        try{
            // number of followers
            String[] people = valueStr.split(":");
            int numberOfFollowers = people.length;
            Person[] allFollowers = new Person[numberOfFollowers];
            
            // establish MySQL connection
            stmt = conn1.createStatement();
           
            // SQL query for each followers for their profile
            int count = 0;
            for(String each : people){
                tableName = "userinfo";
                String sql = "SELECT name AS name, profile_image AS image FROM " + tableName + " WHERE user_id = '" + each + "'" ;
                ResultSet rs1 = stmt.executeQuery(sql);
                if (rs1.next()) {
                    name  = rs1.getString("name");
                    image = rs1.getString("image");
                    allFollowers[count++] = new Person(name, image);
                }
            }
            
            // Sort followers base on their name and then profile in ascending order
            Arrays.sort(allFollowers);
            
            // Put each followers into the followers' array
            for(Person each : allFollowers){
                JSONObject follower = new JSONObject();
                follower.put("name", each.name);
                follower.put("profile", each.image);
                followers.put(follower);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        
        // Put all followers into the result
        result.put("followers", followers);

        /*
            Task 4 (3):
            Get the 30 LATEST followee posts and put them in the
            result JSON object as one array.

            The posts should be sorted:
            First in ascending timestamp order
            Then numerically in ascending order by their PID (PostID) 
	    if there is a tie on timestamp
        */
        
        // Get request for followees
        Get e = new Get(Bytes.toBytes(id));
        Result s = followeesTable.get(e);
        byte[] values = s.getValue(Bytes.toBytes("data"), Bytes.toBytes("follower"));
        String valueStrs = Bytes.toString(values);
        System.out.println("Get from follower: " + valueStrs);
       
        // Array for all posts
        final ArrayList<Post> list = new ArrayList<Post>();
        // JSON array for final posts output
        final JSONArray posts = new JSONArray();
        
        // Retrieve post for each followees
        String[] people = valueStrs.split(":");
        for(String each : people){

           System.out.println("ID: " + each);
            
           FindIterable<Document> iterable = db.getCollection("posts").find(
                    new Document("uid", Integer.parseInt(each))).sort(new Document("timestamp", 1));
            
           // Iterate through all results
            iterable.forEach(new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    JSONObject temp = new JSONObject();
                    String timestamp = (String) document.get("timestamp");
                    int pid = (Integer) document.get("pid");
                    temp.put("content", (String) document.get("content"));
                    temp.put("timestamp", (String) document.get("timestamp"));
                    temp.put("uid", document.get("uid"));
                    temp.put("name", (String) document.get("name"));
                    temp.put("image", (String) document.get("image"));
                    temp.put("pid",  document.get("pid"));
                    temp.put("comments", document.get("comments"));
                    temp.put("profile", (String) document.get("profile"));
                    list.add(new Post(timestamp, pid, temp));

                }
            });
           
        }
        
        // Sort in Descending order
        Collections.sort(list);
        
        // Latest 30 posts or all posts if less than 30
        int size = list.size();
        if(size >= 30){
            for(int i=29; i>=0; i--){
                posts.put(list.get(i).json);
            }
        }else{
            for(int i= size-1; i>=0; i--){
                posts.put(list.get(i).json);
            }
        }
        
        // Adding posts into result
        result.put("posts", posts);
        
        PrintWriter out = response.getWriter();
        out.print(String.format("returnRes(%s)", result.toString()));
        out.close();
        if (conn != null) {
            conn.close();
        }
    }

    @Override
    protected void doPost(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        doGet(req, resp);
    }
    
}

/**
 * Post class for each followee's posts
 * @author LeoHong
 *
 */
class Post implements Comparable<Post>{
    public JSONObject json;
    public String timestamp;
    public int pid;
    Post(String t, int pid, JSONObject j){
        this.timestamp = t;
        this.json = j;
        this.pid = pid;
    }
    
    /**
     * Customized comparator
     * @param o
     * @return
     */
    @Override
    public int compareTo(Post o) {
        
        /* For Descending order*/
        if(o.timestamp.compareTo(this.timestamp) != 0)
        return o.timestamp.compareTo(this.timestamp);
        // If timestamp is tie, then sort by pid
        return ((Integer)o.pid).compareTo(((Integer)this.pid));
    }
    
}
