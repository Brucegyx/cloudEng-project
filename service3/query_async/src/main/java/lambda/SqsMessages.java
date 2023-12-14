package lambda;

public class SqsMessages {
    private String dbBucketName;
    private String dbFile;
    private String aggregation;
    private String filter;
    public String getDbBucketName() {
        return dbBucketName;
    }
    public void setDbBucketName(String dbBucketName) {
        this.dbBucketName = dbBucketName;
    }
    public String getDbFile() {
        return dbFile;
    }
    public void setDbFile(String dbFile) {
        this.dbFile = dbFile;
    }
    public String getAggregation() {
        return aggregation;
    }
    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }
    public String getFilter() {
        return filter;
    }
    public void setFilter(String filter) {
        this.filter = filter;
    }
}
