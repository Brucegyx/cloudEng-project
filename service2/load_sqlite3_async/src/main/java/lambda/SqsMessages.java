package lambda;

public class SqsMessages {
    private String transformedBucketName;
    private String transformedFileName;
    private String aggregation;
    private String filter;
    public String getTransformedBucketName() {
        return transformedBucketName;
    }
    public void setTransformedBucketName(String transformedBucketName) {
        this.transformedBucketName = transformedBucketName;
    }
    public String getTransformedFileName() {
        return transformedFileName;
    }
    public void setTransformedFileName(String transformedFileName) {
        this.transformedFileName = transformedFileName;
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
