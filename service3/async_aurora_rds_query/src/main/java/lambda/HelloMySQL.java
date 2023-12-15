package lambda;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import org.json.JSONObject;
import org.json.JSONArray;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import saaf.Inspector;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloMySQL implements RequestHandler<Request, HashMap<String, Object>> {

    /**
     * Lambda Function Handler
     * 
     * @param request Request POJO with defined variables from Request.java
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(Request request, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();        

        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        //****************START FUNCTION IMPLEMENTATION*************************
        //Add custom key/value attribute to SAAF's output. (OPTIONAL)
        
        //Create and populate a separate response object for function output. (OPTIONAL)
        Response r = new Response();

        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream("db.properties"));
            
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            String dbName = request.getDatabaseName();
            String tableName = request.getTableName();
            String filter = request.getFilter();
            String aggregation = request.getAggregation();

            Connection con = DriverManager.getConnection(url,username,password);

            if (invalidDatabaseOrTable(con, logger, dbName, tableName)) {
                con.close();
                throw new Exception("Database "+ dbName +" or "+ tableName +" does not exist");
            }
            Statement stmt = con.createStatement();
            stmt.executeUpdate("USE " + dbName);

            // perform client query 
            String query = "";
            if (filter == null || filter.equals("") || filter.equals("*")) {
                query = "SELECT "+ aggregation +" FROM "+ tableName;
            } else { 
                query = "SELECT "+ aggregation +" FROM "+ tableName +" WHERE "+ filter;
            }
            PreparedStatement ps = con.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            List<String> columnNames = new ArrayList<String>();
            for (int i = 1; i <= columnsNumber; i++) {
                columnNames.add(rsmd.getColumnName(i));
            }
            JSONArray result = new JSONArray();
            while (rs.next()) {
                JSONObject obj = new JSONObject();
                for (String column : columnNames) {
                    try {
                        obj.put(column, rs.getString(column));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
                result.put(obj);
            }

            logger.log("result=" + result.toString());
            con.close();
            rs.close();
            // set the array of JSON objects as the query result in the response
            r.setValue(result.toString()); 
            
            // sleep to ensure that concurrent calls obtain separate Lambdas
            try {Thread.sleep(200);}
            catch (InterruptedException ie) {
                logger.log("interrupted while sleeping...");
            }

        } catch (SQLException sqle) {
            logger.log("MySQL exception: " + sqle.getMessage());
        } catch (Exception e) {
            logger.log("General exception :" + e.getMessage());
        }

        //Print log information to the Lambda log as needed
        //logger.log("log message...");
        
        inspector.consumeResponse(r);
        
        //****************END FUNCTION IMPLEMENTATION***************************
        
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    /** returns TRUE if the database or table DOES NOT exist */
    private boolean invalidDatabaseOrTable(Connection con, LambdaLogger logger, String dbName, String tableName) {
        try {
            boolean databaseExists = false;
            boolean tableExists = false;

            Statement stmt = con.createStatement();
            String sqlDB = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '" + dbName + "'";
            ResultSet rs = stmt.executeQuery(sqlDB);
            if (rs.next()) databaseExists = true;
            rs.close();

            if (!databaseExists) {
                logger.log("Database "+ dbName +" DOES NOT EXIST! ");
                return true;
            }

            stmt = con.createStatement();
            String sqlTable = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+ dbName +"' AND TABLE_NAME = '" + tableName + "'";
            rs = stmt.executeQuery(sqlDB);
            if (rs.next()) tableExists = true;
            rs.close();

            if (!tableExists) {
                logger.log("Table "+ tableName +" DOES NOT EXIST! ");
                return true;
            }
        } catch (SQLException sqle) {
            logger.log("MySQL exception: " + sqle.getMessage());
        } 

        return false;
    }

}
