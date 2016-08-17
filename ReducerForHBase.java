import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;

/**
 * Reducer run locally for the HBase database design which combine user id and
 * hashtag as one row key
 * 
 * @author LeoHong
 *
 */
public class ReducerForHBase {
    public static void main (String args[]) {
		try{

		    //BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		    BufferedWriter write = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("optimizedLinksToFollowee.csv"), "UTF-8"));
		   
		    /* For follower to followee
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                // Initialize variables
                String input;
                String tempKey="";
                String outputString = "";
                
                try{
                    // While we have input on stdin
                    while ((input = br.readLine()) != null) {
                            String[] line=input.split(",");
                            String followee = line[0];
                            String follower = line[1];
                        
                            if(tempKey.equals(followee)){
                                outputString = outputString + ":" + follower;
                            }else{
                                if(!outputString.equals("")){
                                    write.write(outputString);
                                    write.newLine();
                                }
                                
                                tempKey = followee;
                                outputString = followee + "," + follower;
                            }
                           
                        }
                    }catch(IOException e){
                        e.printStackTrace();
                    }
                
                    write.write(outputString);
                    write.newLine();
    			
    			write.close();
    			br.close();
    			
    		}catch( IOException io){
            io.printStackTrace();
        }
        
        */
		    // For follower to followee
		    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            // Initialize variables
            String input;
            String tempKey="";
            String outputString = "";
            
            try{
                // While we have input on stdin
                while ((input = br.readLine()) != null) {
                    String[] line=input.split(",");
                    String followee = line[0];
                    String follower = line[1];
                    
                    if(tempKey.equals(follower)){
                        outputString = outputString + ":" + followee;
                    }else{
                        if(!outputString.equals("")){
                            write.write(outputString);
                            write.newLine();
                        }
                        
                        tempKey = follower;
                        outputString = follower + "," + followee;
                    }
                   
                }
            }catch(IOException e){
                e.printStackTrace();
            }
        
            write.write(outputString);
            write.newLine();
        
        write.close();
        br.close();
        
    }catch( IOException io){
    io.printStackTrace();
}
}}

/**
 * Profile to assist reduce
 * @author LeoHong
 *
 */
class Profile {
    public String name;
    public String image;

    Profile(String name, String image) {
        this.name = name;
        this.image = image;
    }
}
