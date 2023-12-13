package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    String name;
    String outputBucketName;
    String transformedFileName;

    public String getOutputBucketName() {
        return outputBucketName;
    }

    public void setOutputBucketName(String outputBucketName) {
        this.outputBucketName = outputBucketName;
    }

    public String getTransformedFileName() {
        return transformedFileName;
    }

    public void setTransformedFileName(String transformedFileName) {
        this.transformedFileName = transformedFileName;
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

    public Request() {

    }
}
