import java.io.IOException;
import java.net.URISyntaxException;

public class clustering {


    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        kmeans.simple(input, temp, output);

    }
    public static void basic(int iterations, String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        basicMultiIteration.looping(iterations, input, temp, output);

    }
    public static void advanced(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        advancedMulti.looping(iterations, input, temp, output, threshold);

    }
    public static void optimization(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        optimization.looping(iterations, input, temp, output, threshold);

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeans.csv";

        String singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/single";
        String basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/basic";
        String advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/advanced";
        String optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/optimization";


        single(input, temp, singleOutput);

        basic(3, input, temp, basicOutput);

        advanced(100, input, temp, advancedOutput, 500);

        optimization(100, input, temp, optimizationOutput, 500);
    }
}
