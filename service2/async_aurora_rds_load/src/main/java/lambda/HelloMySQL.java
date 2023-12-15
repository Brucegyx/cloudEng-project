package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import lambda.SqsMessages;
import com.google.gson.*;
import saaf.Inspector;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Scanner;
import java.util.HashMap;




/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloMySQL implements RequestHandler<SQSEvent, HashMap<String, Object>> {

    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

    /**
     * Lambda Function Handler
     * 
     * @param sqsevent
     * @param context 
     * @return HashMap that Lambda will automatically convert into JSON.
     */
    public HashMap<String, Object> handleRequest(SQSEvent sqsevent, Context context) {

        // Create logger
        LambdaLogger logger = context.getLogger();        
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();

        HashMap<String,Object> response = new HashMap<String, Object>();

        try {
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            Connection con = DriverManager.getConnection(url,username,password);

            for (SQSEvent.SQSMessage msg : sqsevent.getRecords()) {
                String body = msg.getBody();
                logger.log("msg from service 1: " + body);

                try {
                    Gson gson = new Gson();
                    SqsMessages sqsMessages = gson.fromJson(body, SqsMessages.class);
                    
                    assert(sqsMessages != null);

                    Properties properties = new Properties();
                    properties.load(new FileInputStream("db.properties"));
                    
                    // String dbName = request.getDatabaseName();
                    // String tableName = request.getTableName();
                    // String bucket = request.getBucketName();
                    // String filename = request.getFileName();

                    String dbName = "MAIN";
                    String tableName = "async";
                    String bucket = sqsMessages.getTransformedBucketName();
                    String filename = sqsMessages.getTransformedFileName();
                    String filter = sqsMessages.getFilter();
                    String aggregation = sqsMessages.getAggregation();

                    Statement stmt = con.createStatement();

                    // url += "/"+dbName; // for test
                    // try {
                    //     stmt.executeUpdate("DROP DATABASE IF EXISTS TEST");
                    // } catch (Exception e) {
                    //     logger.log("Failed to drop DB: " + e.getMessage());
                    // }
                    // logger.log(dbName + " Dropped");


                    String sql_createDB = "CREATE DATABASE IF NOT EXISTS " + dbName;
                    stmt.executeUpdate(sql_createDB);
                    stmt.executeUpdate("USE " + dbName);

                    logger.log("Database "+ dbName +" created");

                    try {
                        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
                    } catch (Exception e) {
                        logger.log("Failed to drop table: " + e.getMessage());
                    }

                    logger.log(tableName + " Dropped");

                    String sql_createTable = "CREATE TABLE IF NOT EXISTS `" + tableName + "` ("
                                + "`OrderID` VARCHAR(255) PRIMARY KEY, "
                                + "`Region` VARCHAR(255), "
                                + "`Country` VARCHAR(255), "
                                + "`ItemType` VARCHAR(255), "
                                + "`SalesChannel` VARCHAR(255), "
                                + "`OrderPriority` VARCHAR(255), "
                                + "`OrderDate` VARCHAR(255), "
                                + "`ShipDate` VARCHAR(255), "
                                + "`UnitsSold` INT, "
                                + "`UnitPrice` DECIMAL(16, 6), "
                                + "`UnitCost` DECIMAL(16, 6), "
                                + "`TotalRevenue` DECIMAL(16, 6), "
                                + "`TotalCost` DECIMAL(16, 6), "
                                + "`TotalProfit` DECIMAL(16, 6), "
                                + "`OrderProcessingTime` INT, "
                                + "`GrossMargin` DECIMAL(16, 6))";
            
                    stmt.executeUpdate(sql_createTable);
                    logger.log(tableName + " created");


                    try {
                        // Get the object from the S3 bucket
                        // S3Object s3Object = s3Client.getObject(bucket, filename);
                        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, filename));
                        S3ObjectInputStream s3InputStream = s3Object.getObjectContent();

                        String insertDataQuery = "INSERT INTO "+ tableName +" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        PreparedStatement prepStmt = con.prepareStatement(insertDataQuery);

                        // Process the CSV file line by line
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8))) {

                            // Skip the first line (header)
                            reader.readLine();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                // Process each line of the CSV file
                                String[] fields = line.split(",");

                                prepStmt.setString(1, fields[6]); // OrderID (Primary Key)
                                prepStmt.setString(2, fields[0]); // Region
                                prepStmt.setString(3, fields[1]); // Country
                                prepStmt.setString(4, fields[2]); // ItemType
                                prepStmt.setString(5, fields[3]); // SalesChannel
                                prepStmt.setString(6, fields[4]); // OrderPriority
                                prepStmt.setString(7, fields[5]); // OrderDate
                                prepStmt.setString(8, fields[7]); // ShipDate
                                prepStmt.setInt(9, Integer.parseInt(fields[8]));        // UnitsSold
                                prepStmt.setDouble(10, Double.parseDouble(fields[9]));  // UnitPrice
                                prepStmt.setDouble(11, Double.parseDouble(fields[10])); // UnitCost
                                prepStmt.setDouble(12, Double.parseDouble(fields[11])); // TotalRevenue
                                prepStmt.setDouble(13, Double.parseDouble(fields[12])); // TotalCost
                                prepStmt.setDouble(14, Double.parseDouble(fields[13])); // TotalProfit
                                prepStmt.setInt(15, Integer.parseInt(fields[14]));      // OrderProcessingTime
                                prepStmt.setDouble(16, Double.parseDouble(fields[15])); // GrossMargin
                                // Set prepared statement fields based on CSV fields
                                prepStmt.executeUpdate();
                            }
                        } catch(Exception e) {
                            logger.log("Error reading csv file: " + e.getMessage());
                        }

                    } catch (Exception e) {
                        logger.log("Error processing S3 object: " + e.getMessage());
                    }


                    // // retrieves only the first 10 rows of table
                    // String selectSql = "SELECT * FROM " + tableName + " LIMIT 10";
                    // try (Statement selectStmt = con.createStatement();
                    //     ResultSet rs = selectStmt.executeQuery(selectSql)) {
                    //     int row = 1;
                    //     while (rs.next()) {
                    //         // Retrieve each column value
                    //         String c1 = rs.getString("OrderID");
                    //         String c2 = rs.getString("Region");
                    //         String c3 = rs.getString("Country");
                    //         String c4 = rs.getString("ItemType");
                    //         String c5 = rs.getString("SalesChannel");
                    //         String c6 = rs.getString("OrderPriority");
                    //         String c7 = rs.getString("OrderDate");
                    //         String c8 = rs.getString("ShipDate");
                    //         int c9 = rs.getInt("UnitsSold");
                    //         double c10 = rs.getDouble("UnitPrice");
                    //         double c11 = rs.getDouble("UnitCost");
                    //         double c12 = rs.getDouble("TotalRevenue");
                    //         double c13 = rs.getDouble("TotalCost");
                    //         double c14 = rs.getDouble("TotalProfit");
                    //         int c15 = rs.getInt("OrderProcessingTime");
                    //         double c16 = rs.getDouble("GrossMargin");
                    //         logger.log("Row"+ row++ +": "+ c1 +", "+ c2 +", "+ c3 +", "+ c4 +", "+ c5 +", "+ 
                    //                 c6 +", "+ c7 +", "+ c8 +", "+ c9 +", "+ c10 +", "+ c11 +", "+ c12 +", "+ 
                    //                 c13 +", "+ c14 +", "+ c15 +", "+ c16);
                    //     }
                    // } catch (SQLException e) {
                    //     logger.log("SQL Exception while selecting data: " + e.getMessage());
                    // } catch (Exception e) {
                    //     logger.log("Got an exception working with MySQL! ");
                    //     logger.log(e.getMessage());
                    // }

                    
                    AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
                    SendMessageRequest send_msg_request = new SendMessageRequest().withQueueUrl("https://sqs.us-east-2.amazonaws.com/338749838656/562queueLoadToQuery").withMessageBody("{\"dbBucketName\":\"" + outputBucketName + "\",\"dbFile\":\"" + outputFileName+ "\",\"aggregation\":\"" + aggregation+"\",\"filter\":\""+filter+"\"}").withDelaySeconds(0);
                    sqs.sendMessage(send_msg_request);
                    
                    logger.log("msg from service 1: " + body);
                    // s3Client.putObject("562project-query-async", "asyncResult.json", body);
                } catch(JsonParseException e) {
                    logger.log("Error parsing JSON: " + e.getMessage());
                    throw e;
                }
            }
            con.close();
        } catch (Exception e) {
            logger.log("Got an exception working with MySQL! ");
            logger.log(e.getMessage());
        }

        inspector.consumeResponse(r);
        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

}
