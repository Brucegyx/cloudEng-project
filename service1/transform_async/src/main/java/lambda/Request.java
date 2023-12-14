package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {
    private String bucketname;
    private String filename;
    private String aggregation;
    private String filter;
    // create getter and setter for these fields
    String name;
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

    public Request(String name) {
        this.name = name;
    }

    public Request( ) {

    }

    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    
}
