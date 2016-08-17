package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

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



public class RecommendationServlet extends HttpServlet {
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.41.233";
    /**
     * The name of your HBase table.
     */
    private static String tableName = "followees";
    /**
     * HTable handler.
     */
    private static HTableInterface followeesTable;
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
    private static Configuration conf;
    
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project";
    private static final String URL = "jdbc:mysql://project.cicfpnjbqg7h.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;
    private static Connection conn1;
    private static final String DB_USER = "root";
    private static final String DB_PWD = "sd15752a";
    
	public RecommendationServlet () throws Exception {
        /*
        	Your initialization code goes here
         */
	    logger.setLevel(Level.ERROR);
        conf = HBaseConfiguration.create();
        conf.set("hbase.master", zkAddr + ":60000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("HBase not configured!");
            return;
        }
        
  
        if (followeesTable != null) {
            followeesTable.close();
        }
        
        // clean up the table
        if (conn != null) {
            conn.close();
        }
	    conn = HConnectionManager.createConnection(conf);
        
        followeesTable = new HTable(conf, Bytes.toBytes(tableName)); //conn.getTable(Bytes.toBytes(tableName2));
        System.out.println("followeesTable debugg");
        
        // Establish connection with MySQL
        try{
            Class.forName(JDBC_DRIVER);
            conn1 = DriverManager.getConnection(URL, DB_USER, DB_PWD);
        }catch(Exception e){
            e.printStackTrace();
        }
		
	}

	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
			throws ServletException, IOException {

		JSONObject result = new JSONObject();
	    String id = request.getParameter("id");

		/**
		 * Bonus task:
		 * 
		 * Recommend at most 10 people to the given user with simple collaborative filtering.
		 * 
		 * Store your results in the result object in the following JSON format:
		 * recommendation: [
		 * 		{name:<name_1>, profile:<profile_1>}
		 * 		{name:<name_2>, profile:<profile_2>}
		 * 		{name:<name_3>, profile:<profile_3>}
		 * 		...
		 * 		{name:<name_10>, profile:<profile_10>}
		 * ]
		 * 
		 * Notice: make sure the input has no duplicate!
		 */
        // For first degree relation
	    HashMap<String, Integer> S = new HashMap<String, Integer>();
	    // For second degree relation (may duplicate with first degree)
        HashMap<String, Integer> R = new HashMap<String, Integer>();
        
        // Get request for followees
        Get e = new Get(Bytes.toBytes(id));
        Result s = followeesTable.get(e);
        byte[] values = s.getValue(Bytes.toBytes("data"), Bytes.toBytes("follower"));
        String valueStrs = Bytes.toString(values);
        System.out.println("Get from follower: " + valueStrs);
       

        // Retrieve post for each followees
        String[] people = valueStrs.split(":");
        for(String each : people){
            // put each first degree into map
            S.put(each, 1);
            
            // Get request find followees for each first degree
            e = new Get(Bytes.toBytes(each));
            s = followeesTable.get(e);
            values = s.getValue(Bytes.toBytes("data"), Bytes.toBytes("follower"));
            valueStrs = Bytes.toString(values);
            
            // All followees of client's followee
            String[] secondDegree = valueStrs.split(":");
            for(String eachSecond : secondDegree){
            
                if(!R.containsKey(eachSecond)){
                    R.put(eachSecond, 1);
                }else{
                    int edges = R.get(eachSecond);
                    R.put(eachSecond, ++edges);
                }
            }
        }
        

        // Array for all recommenders
        final ArrayList<Candidates> list = new ArrayList<Candidates>();
        
        // Filter base on criteria
        Set<Map.Entry<String, Integer>> set = R.entrySet();
        for(Map.Entry<String, Integer> each : set){
            String key = each.getKey();
            if(!S.containsKey(key) && !key.equals(id)){
                Integer edges = each.getValue();
                list.add(new Candidates(Integer.parseInt(key), edges));
            }
        }
        
        // Sort base on edges in decesending order and then key numeric in ascending order
        Collections.sort(list);
        
        // SQL query for each candidates
        Statement stmt = null;
        int rowCount = -1;
        String name = null;
        String image = null;
        JSONArray recommendation = new JSONArray();
        
        try{
           
            stmt = conn1.createStatement();
            
            // MySQL query to get followers' profile
            int count = 0;
            for(Candidates each : list){
                JSONObject temp = new JSONObject();
                tableName = "userinfo";
                String sql = "SELECT name AS name, profile_image AS image FROM " + tableName + " WHERE user_id = '" + each.key + "'" ;
                ResultSet rs1 = stmt.executeQuery(sql);
                if (rs1.next()) {
                    name  = rs1.getString("name");
                    image = rs1.getString("image");
                    temp.put("name", name);
                    temp.put("profile", image);
                }
                count++;
                recommendation.put(temp);
                
                // Only need 10 candidates
                if(count >= 10){
                    break;
                }
            }
            
            // put recommendation array into result
           result.put("recommendation", recommendation);

        } catch (SQLException f) {
            f.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException p) {
                    p.printStackTrace();
                }
            }
        }

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        writer.close();

	}

	@Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}

/**
 * Candidiate for the recommendation system
 * @author LeoHong
 *
 */
class Candidates implements Comparable<Candidates>{
    public Integer key;
    public Integer edges;
    
    Candidates(Integer k, Integer e){
        this.key = k;
        this.edges = e;
    }
    
    /**
     * Customized comparator
     * @param o
     * @return
     */
    @Override
    public int compareTo(Candidates o) {
        
        /* For Descending order*/
        if(o.edges.compareTo(this.edges) != 0)
        return o.edges.compareTo(this.edges);
        // If timestamp is tie, then sort by pid
        return this.key.compareTo(o.key);
    }
}
