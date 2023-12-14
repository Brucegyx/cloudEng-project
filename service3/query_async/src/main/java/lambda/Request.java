package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    String dbBucket;
    String dbFile;
    String aggregation;
    String filter;

    public String getDbBucket() {
        return dbBucket;
    }
    public void setDbBucket(String dbBucket) {
        this.dbBucket = dbBucket;
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
    public String getName() {
        return name;
    }
    
    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name, String dbFile, String aggregation, String filter, String dbBucket) {
        this.name = name;
        this.dbFile = dbFile;
        this.aggregation = aggregation;
        this.filter = filter;
        this.dbBucket = dbBucket;
    }

    public Request() {

    }
}
