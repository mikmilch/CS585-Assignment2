package kmedoids;

import java.io.IOException;
import java.net.URISyntaxException;

public class kmedoids {

    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nSingle Iteration KMedoids");
        single.simple(input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void basic(int iterations, String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Basic) KMedoids (Iterations r = " + iterations + ")");
        basicMulti.looping(iterations, input, temp, output);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void advanced(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Advanced) KMedoids (Iterations r = " + iterations + ", Threshold = " + threshold + ")");
        advancedMulti.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void optimization(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration (Optimized) KMedoids (Iterations r = " + iterations + ", Threshold = " + threshold + ")");
        optimization.looping(iterations, input, temp, output, threshold);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }

    public static void outputVariation(int iterations, String input, String temp, String output, int threshold, String variation) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();

        System.out.println("\nMulti-Iteration KMedoids (Iterations r = " + iterations + ", Threshold = " + threshold + ", Variation = " + variation + ")");
        outputVariations.looping(iterations, input, temp, output, threshold, variation);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmedoids5.csv";

        String singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/single";
        String basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/basic";
        String advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/advanced";
        String optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/optimization";
        String variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/variation/1";
        String variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k5/variation/2";

        String variation1 = "Only Cluster Points";
        String variation2 = "Final Clustered Points";

        System.out.println("Running with k = 5 Initial Centroids");

        single(input, temp, singleOutput);

        basic(4, input, temp, basicOutput + "/4");
        basic(10, input, temp, basicOutput + "/10");
        basic(100, input, temp, basicOutput + "/100");

        advanced(10, input, temp, advancedOutput + "/10", 200);
        advanced(100, input, temp, advancedOutput + "/100/200", 200);
        advanced(100, input, temp, advancedOutput + "/100/2000", 2000);

        optimization(10, input, temp, optimizationOutput + "/10", 200);
        optimization(100, input, temp, optimizationOutput + "/100/200", 200);
        optimization(100, input, temp, optimizationOutput + "/100/2000", 2000);


        outputVariation(10, input, temp, variationOutput1, 200, variation1);
        outputVariation(10, input, temp, variationOutput2, 200, variation2);

        System.out.println("\n");


        input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmedoids10.csv";

        singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/single";
        basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/basic";
        advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/advanced";
        optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/optimization";
        variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/variation/1";
        variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k10/variation/2";

        System.out.println("Running with k = 10 Initial Centroids");

        single(input, temp, singleOutput);

        basic(4, input, temp, basicOutput + "/4");
        basic(10, input, temp, basicOutput + "/10");
        basic(100, input, temp, basicOutput + "/100");

        advanced(10, input, temp, advancedOutput + "/10", 200);
        advanced(100, input, temp, advancedOutput + "/100/200", 200);
        advanced(100, input, temp, advancedOutput + "/100/2000", 2000);

        optimization(10, input, temp, optimizationOutput + "/10", 200);
        optimization(100, input, temp, optimizationOutput + "/100/200", 200);
        optimization(100, input, temp, optimizationOutput + "/100/2000", 2000);


        outputVariation(10, input, temp, variationOutput1, 200, variation1);
        outputVariation(10, input, temp, variationOutput2, 200, variation2);

        System.out.println("\n");


        input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmedoids100.csv";

        singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/single";
        basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/basic";
        advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/advanced";
        optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/optimization";
        variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/variation/1";
        variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/k100/variation/2";

        System.out.println("Running with k = 100 Initial Centroids");

        single(input, temp, singleOutput);

        basic(4, input, temp, basicOutput + "/4");
        basic(10, input, temp, basicOutput + "/10");
        basic(100, input, temp, basicOutput + "/100");

        advanced(10, input, temp, advancedOutput + "/10", 200);
        advanced(100, input, temp, advancedOutput + "/100/200", 200);
        advanced(100, input, temp, advancedOutput + "/100/2000", 2000);

        optimization(10, input, temp, optimizationOutput + "/10", 200);
        optimization(100, input, temp, optimizationOutput + "/100/200", 200);
        optimization(100, input, temp, optimizationOutput + "/100/2000", 2000);


        outputVariation(10, input, temp, variationOutput1, 200, variation1);
        outputVariation(10, input, temp, variationOutput2, 200, variation2);


    }

}
