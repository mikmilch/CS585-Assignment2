import java.io.IOException;
import java.net.URISyntaxException;

public class clustering {


    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nSingle Iteration KMeans");
        kmeans.simple(input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void basic(int iterations, String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMeans (Iterations r = " + iterations);
        basicMultiIteration.looping(iterations, input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void advanced(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMeans (Iterations r = " + iterations + " With a threshold = " + threshold);
        advancedMulti.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void optimization(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMeans (Iterations r = " + iterations + " With a threshold = " + threshold);
        optimization.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }

    public static void outputVariation(int iterations, String input, String temp, String output, int threshold, String variation) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMeans (Iterations r = " + iterations + " With a threshold = " + threshold + ". Variation = " + variation);
        outputVariations.looping(iterations, input, temp, output, threshold, variation);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeans.csv";

        String singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/single";
        String basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/basic";
        String advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/advanced";
        String optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/optimization";
        String variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/variation/1";
        String variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/clustering/variation/2";


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
