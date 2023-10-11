import java.io.IOException;
import java.net.URISyntaxException;

public class clustering {


    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        kmeans.simple(input, temp, output);

    }
    public static void basic(){

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/datasetTest.csv";

        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/single";

        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeansTest.csv";
        single(input, temp, output);
    }
}
