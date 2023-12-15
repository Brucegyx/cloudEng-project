package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    private String databaseName;
    private String tableName;
    private String bucketName;
    private String fileName;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String filename) {
        this.fileName = filename;
    }



    // ************* OLD ************************
    // String name;

    // public String getName() {
    //     return name;
    // }
    
    // public String getNameALLCAPS() {
    //     return name.toUpperCase();
    // }

    // public void setName(String name) {
    //     this.name = name;
    // }

    // public Request(String name) {
    //     this.name = name;
    // }

    // public Request() {

    // }
}
