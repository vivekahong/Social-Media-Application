package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.client.HTable;

import org.json.JSONObject;

import org.json.JSONArray;


public class FollowerServlet extends HttpServlet {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.41.233";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "followers";
    /**
     * HTable handler.
     */
    private static HTableInterface followersTable;
    /**
     * HBase connection.
     */
    private static HConnection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private final static Logger logger = Logger.getRootLogger();
    
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project";
    private static final String URL = "jdbc:mysql://project.cicfpnjbqg7h.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;
    private static Connection conn1;
    private static final String DB_USER = "root";
    private static final String DB_PWD = "sd15752a";

    public FollowerServlet() {
        /*
            Your initialization code goes here
        */
        
        // Remember to set correct log level to avoid unnecessary output.
        logger.setLevel(Level.ERROR);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        
        // Establish connection
        try{
            conn = HConnectionManager.createConnection(conf);
            followersTable = conn.getTable(Bytes.toBytes(tableName));
        }catch(IOException e){
            e.printStackTrace();
        }
        try{
            Class.forName(JDBC_DRIVER);
            conn1 = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {

        String id = request.getParameter("id");
        System.out.println("Id: " + id);
        JSONObject result = new JSONObject();
        JSONArray followers = new JSONArray();

        /*
            Task 2:
            Implement your logic to retrive the followers of this user. 
            You need to send back the Name and Profile Image URL of his/her Followers.

            You should sort the followers alphabetically in ascending order by Name. 
            If there is a tie in the followers name, 
	    sort alphabetically by their Profile Image URL in ascending order. 
        */
        // Get request for HBase
        Get g = new Get(Bytes.toBytes(id));
        Result r = followersTable.get(g);
        byte[] value = r.getValue(Bytes.toBytes("data"), Bytes.toBytes("followee"));
        String valueStr = Bytes.toString(value);
        System.out.println("Get from HBase: " + valueStr);
        
        Statement stmt = null;
        int rowCount = -1;
        String name = null;
        String image = null;
        try{
            
            String[] people = valueStr.split(":");
            // number of followers
            int numberOfFollowers = people.length;
            Person[] allFollowers = new Person[numberOfFollowers];
            
            stmt = conn1.createStatement();
           
            // MySQL query to get followers' profile
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
            
            // Sort followers base on their name and then profile ascending order
            Arrays.sort(allFollowers);
            
            // Put each followers into the final output JSON array
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
        result.put("followers", followers);
        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        System.out.println(followers.toString());
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response)
            throws ServletException, IOException {
        doGet(request, response);
    }   
    
}

/**
 * Person class to store each follower's name and profile
 * @author LeoHong
 *
 */
class Person implements Comparable<Person> {
    public String name;
    public String image;

    Person(String name, String image) {
        this.name = name;
        this.image = image;
    }
    
    /**
     * Customized compareTo
     */
    @Override
    public int compareTo(Person o){
        if(this.name.compareTo(o.name) != 0){
            return this.name.compareTo(o.name);
        }else{
            return this.image.compareTo(o.image);
        }
    }
}


