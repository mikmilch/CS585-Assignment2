import java.io.IOException;
import java.net.URISyntaxException;

public class BYOD {

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

    public static void outputVariation(int iterations, String input, String temp, String output, int threshold, String variation) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        outputVariations.looping(iterations, input, temp, output, threshold, variation);

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String input = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/Mall_Customers.csv";
        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/BYOD_kmeans_dataset.csv";

        String singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/single";
        String basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/basic";
        String advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/advanced";
        String optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/optimization";
        String variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/variation/1";
        String variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/BYOD/variation/2";

        single(input, temp, singleOutput);

        basic(3, input, temp, basicOutput);

        advanced(100, input, temp, advancedOutput, 500);

        optimization(100, input, temp, optimizationOutput, 500);

        String variation1 = "Only Cluster Points";
        String variation2 = "Final Clustered Points";

        outputVariation(100, input, temp, variationOutput1, 500, variation1);
        outputVariation(100, input, temp, variationOutput2, 500, variation2);
    }
}
