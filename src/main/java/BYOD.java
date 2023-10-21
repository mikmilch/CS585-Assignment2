import java.io.IOException;
import java.net.URISyntaxException;

public class BYOD {

    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nSingle Iteration KMeans");
        single.simple(input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void basic(int iterations, String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Basic) KMeans (Iterations r = " + iterations + ")");
        basicMulti.looping(iterations, input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void advanced(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Advanced) KMeans (Iterations r = " + iterations + ", Threshold = " + threshold + ")");
        advancedMulti.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void optimization(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Optimized) KMeans (Iterations r = " + iterations + ", Threshold = " + threshold + ")");
        optimization.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }

    public static void outputVariation(int iterations, String input, String temp, String output, int threshold, String variation) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMeans (Iterations r = " + iterations + ", Threshold = " + threshold + ", Variation = " + variation + ")");
        outputVariations.looping(iterations, input, temp, output, threshold, variation);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

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


        System.out.println("BYOD, where X = Income and Y = Spending Score");
        single(input, temp, singleOutput);

        basic(10, input, temp, basicOutput);

        advanced(10, input, temp, advancedOutput, 5);

        optimization(10, input, temp, optimizationOutput, 5);

        String variation1 = "Only Cluster Points";
        String variation2 = "Final Clustered Points";

        outputVariation(10, input, temp, variationOutput1, 5, variation1);
        outputVariation(10, input, temp, variationOutput2, 5, variation2);

    }
}
