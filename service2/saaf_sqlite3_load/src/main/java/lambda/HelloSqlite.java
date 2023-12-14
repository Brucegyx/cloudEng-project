package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Set;
import java.util.UUID;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class HelloSqlite implements RequestHandler<Request, HashMap<String, Object>> {
    //private static final String DB_URL = "jdbc:sqlite:/tmp/sqlite.db";
    private static AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

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

        String bucketName = request.getOutputBucketName();
        String fileName = request.getTransformedFileName();
        logger.log("Finding file in bucketname: " + bucketName + ", filename: " + fileName);

        // Export SQLite DB file to S3
        String outputBucketName = "sqlite-db562";
        String outputFileName = fileName.substring(0, fileName.lastIndexOf('.')) + ".db";

        // Download CSV file from S3
        File csvFile = downloadFileFromS3(bucketName, fileName);

        logger.log("Downloaded file from bucket: " + bucketName + ", filename: " + fileName);

        // Create SQLite database file
        File sqliteFile = new File("/tmp/" + outputFileName);

        // Load CSV data into SQLite database
        loadCsvDataIntoDatabase(csvFile, sqliteFile, logger);
        
        logger.log("Loaded CSV data into SQLite database.");

        // Export SQLite database to S3
        exportDatabaseToS3(sqliteFile, outputBucketName, outputFileName, logger);

        logger.log("Exporting SQLite database to bucket: " + outputBucketName + ", filename: " + outputFileName);

        //Create and populate a response object
        Response r = new Response(); 
        r.setValue("Status:Success exported:" + outputBucketName + "filename:" + outputFileName);
        
        inspector.consumeResponse(r);

        inspector.inspectAllDeltas();
        return inspector.finish();
        //return response;
        
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


    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        // Create an instance of the class
        HelloSqlite lt = new HelloSqlite();

        // Create a request object
        Request req = new Request();

        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getName());

        // Run the function
        HashMap resp = lt.handleRequest(req, c);
        try
        {
            Thread.sleep(10);
        }
        catch (InterruptedException ie)
        {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }

    
}
