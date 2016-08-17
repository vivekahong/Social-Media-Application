package cc.cmu.edu.minisite;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;
import org.json.JSONArray;

public class ProfileServlet extends HttpServlet {
    
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "project";
    private static final String URL = "jdbc:mysql://project.cicfpnjbqg7h.us-east-1.rds.amazonaws.com:3306/" + DB_NAME;

    private static final String DB_USER = "root";
    private static final String DB_PWD = "sd15752a";
    
    private static Connection conn;

    public ProfileServlet() throws ClassNotFoundException, SQLException{

        Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, DB_USER, DB_PWD);
    }

    @Override
    protected void doGet(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        JSONObject result = new JSONObject();

        String id = request.getParameter("id");
        String pwd = request.getParameter("pwd");

        /*
            Task 1:
            This query simulates the login process of a user, 
            and tests whether your backend system is functioning properly. 
            Your web application will receive a pair of UserID and Password, 
            and you need to check in your backend database to see if the 
	    UserID and Password is a valid pair. 
            You should construct your response accordingly:

            If YES, send back the user's Name and Profile Image URL.
            If NOT, set Name as "Unauthorized" and Profile Image URL as "#".
        */
        
        Statement stmt = null;
        int rowCount = -1;
        String name = null;
        String image = null;
        try {
            // Authentication
            stmt = conn.createStatement();
            String tableName = "users";
            String sql = "SELECT user_id AS uid FROM " + tableName + " WHERE user_id = '" + id + "' AND " + "password = '" + pwd + "'";
            ResultSet rs = stmt.executeQuery(sql);
            
            if (rs.next()) {
                rowCount = rs.getInt("uid");
                System.out.println("User: " + rowCount + " is retrieved.");
            }
            
            // If there is such user, do the query for user profile
            if(rowCount != -1){
              
                tableName = "userinfo";
                sql = "SELECT name AS name, profile_image AS image FROM " + tableName + " WHERE user_id = '" + id + "'";
                ResultSet rs1 = stmt.executeQuery(sql);
                if (rs1.next()) {
                    name  = rs1.getString("name");
                    image = rs1.getString("image");
                    System.out.print("User: " + name + " with " + image);
                }
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
        
        // If user is not authenticated
        if(name == null){
            result.put("name", "Unauthorized");
            result.put("profile", "#");
        }else{ // authenticated
            result.put("name", name);
            result.put("profile", image);
        }

        PrintWriter writer = response.getWriter();
        writer.write(String.format("returnRes(%s)", result.toString()));
        System.out.println(result.toString());
        writer.close();
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) 
            throws ServletException, IOException {
        doGet(request, response);
    }
}
