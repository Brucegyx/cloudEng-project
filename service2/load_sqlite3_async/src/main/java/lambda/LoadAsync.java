package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import saaf.Inspector;
import com.amazonaws.services.lambda.runtime.ClientContext;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.HashMap;
import java.util.UUID;
import lambda.SqsMessages;

public class LoadAsync implements RequestHandler<SQSEvent, HashMap<String, Object>> {
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
    public HashMap<String, Object> handleRequest(SQSEvent sqsevent, Context context) {
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        // Create logger
        LambdaLogger logger = context.getLogger();
        HashMap<String,Object> response = new HashMap<String, Object>();
        try {
            for (SQSEvent.SQSMessage msg : sqsevent.getRecords()) {
                String body = msg.getBody();
                logger.log("msg from service 1: " + body);

                try {
                    Gson gson = new Gson();
                    SqsMessages sqsMessages = gson.fromJson(body, SqsMessages.class);
                    
                    assert(sqsMessages != null);
                    String csvFileName = sqsMessages.getTransformedFileName();
                    String csvBucketName =sqsMessages.getTransformedBucketName();
                    String filter = sqsMessages.getFilter();
                    String aggregation = sqsMessages.getAggregation();
                    String outputBucketName = "sqlite-db562";
                    String outputFileName = csvFileName.substring(0, csvFileName.lastIndexOf('.')) + ".db";

                    // Download CSV file from S3
                    File csvFile = downloadFileFromS3(csvBucketName, csvFileName);

                    logger.log("Downloaded file from bucket: " + csvBucketName + ", filename: " + csvFileName);

                    // Create SQLite database file
                    File sqliteFile = new File("/tmp/" + outputFileName);

                    // Load CSV data into SQLite database
                    loadCsvDataIntoDatabase(csvFile, sqliteFile, logger);
                    
                    logger.log("Loaded CSV data into SQLite database.");

                    // Export SQLite database to S3
                    exportDatabaseToS3(sqliteFile, outputBucketName, outputFileName, logger);

                    logger.log("Exporting SQLite database to bucket: " + outputBucketName + ", filename: " + outputFileName);
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
        } catch (Exception e) {
            logger.log("Exception: " + e);
        }
        
        inspector.inspectAllDeltas();
        logger.log("Service2 SQS: " + inspector.finish().toString());
        return response;
    }
     /*
     * Download CSV file from S3
     * param bucketName S3 bucket name
     * param fileName S3 file name
     * return CSV file
     */
    private File downloadFileFromS3(String bucketName, String fileName) {
        try {
            // Download CSV file from S3
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
            // Create temporary file
            File csvFile = File.createTempFile("csv-", ".tmp");
            // Copy S3Object content to temp file
            Files.copy(s3Object.getObjectContent(), csvFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return csvFile;
        } catch (IOException e) {
            throw new RuntimeException("Error downloading file from S3: " + e.getMessage(), e);
        }
    }
        /*
     * Load CSV data into SQLite database
     * param csvFile CSV file to load into SQLite database
     * param sqliteFile SQLite database file
     * param logger LambdaLogger object
     */
    private void loadCsvDataIntoDatabase(File csvFile, File sqliteFile, LambdaLogger logger) {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + sqliteFile.getAbsolutePath())) {
            String dropTableQuery = "DROP TABLE IF EXISTS orders";
            connection.createStatement().executeUpdate(dropTableQuery);
            String createTableQuery = "CREATE TABLE IF NOT EXISTS orders ("
                    + "OrderID INTEGER PRIMARY KEY, "
                    + "Region TEXT, "
                    + "Country TEXT, "
                    + "ItemType TEXT, "
                    + "SalesChannel TEXT, "
                    + "OrderPriority TEXT, "
                    + "OrderDate TEXT, "
                    + "ShipDate TEXT, "
                    + "UnitsSold INTEGER, "
                    + "UnitPrice REAL, "
                    + "UnitCost REAL, "
                    + "TotalRevenue REAL, "
                    + "TotalCost REAL, "
                    + "TotalProfit REAL, "
                    + "OrderProcessingTime INTEGER, "
                    + "GrossMargin REAL)";
            connection.createStatement().executeUpdate(createTableQuery);

            // Insert data into SQLite database
            String insertDataQuery = "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (Scanner scanner = new Scanner(csvFile)) {
                PreparedStatement preparedStatement = connection.prepareStatement(insertDataQuery);
                scanner.nextLine(); // Skip the header line

                while (scanner.hasNext()) {
                    String[] fields = scanner.nextLine().split(",");
                    preparedStatement.setString(1, fields[6]); // OrderID (Primary Key)
                    preparedStatement.setString(2, fields[0]); // Region
                    preparedStatement.setString(3, fields[1]); // Country
                    preparedStatement.setString(4, fields[2]); // ItemType
                    preparedStatement.setString(5, fields[3]); // SalesChannel
                    preparedStatement.setString(6, fields[4]); // OrderPriority
                    preparedStatement.setString(7, fields[5]); // OrderDate
                    preparedStatement.setString(8, fields[7]); // ShipDate
                    preparedStatement.setInt(9, Integer.parseInt(fields[8])); // UnitsSold
                    preparedStatement.setDouble(10, Double.parseDouble(fields[9])); // UnitPrice
                    preparedStatement.setDouble(11, Double.parseDouble(fields[10])); // UnitCost
                    preparedStatement.setDouble(12, Double.parseDouble(fields[11])); // TotalRevenue
                    preparedStatement.setDouble(13, Double.parseDouble(fields[12])); // TotalCost
                    preparedStatement.setDouble(14, Double.parseDouble(fields[13])); // TotalProfit
                    preparedStatement.setInt(15, Integer.parseInt(fields[14])); // OrderProcessingTime
                    preparedStatement.setDouble(16, Double.parseDouble(fields[15])); // GrossMargin
                    // Set prepared statement fields based on CSV fields
                    preparedStatement.executeUpdate();
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException("Error loading data into SQLite database: "+ e.getMessage(), e);
        }
    }
 
    /*
     * Export SQLite database to S3
     * param sqliteFile SQLite database file
     * param bucketName S3 bucket name
     * param fileName S3 file name
     * param logger LambdaLogger object
     */
    private void exportDatabaseToS3(File sqliteFile, String bucketName, String fileName, LambdaLogger logger) {
        try {
            s3Client.putObject(bucketName, fileName, sqliteFile);
            logger.log("Database exported to S3 successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Error exporting SQLite database to S3: " + e.getMessage(), e);
        }
    }

}