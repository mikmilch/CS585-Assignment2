package kmedoids;

import java.io.IOException;
import java.net.URISyntaxException;

public class kmedoids {

    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        single.simple(input, temp, output);

    }


    public static void basic(int iterations, String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        basicMulti.looping(iterations, input, temp, output);

    }

    public static void advanced(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        advancedMulti.looping(iterations, input, temp, output, threshold);

    }

    public static void optimization(int iterations, String input, String temp, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        optimization.looping(iterations, input, temp, output, threshold);

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmedoidsTest.csv";

        String singleOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/single";
        String basicOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/basic";
        String advancedOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/advanced";
        String optimizationOutput = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/optimization";
        String variationOutput1 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/variation/1";
        String variationOutput2 = "file:///C:/Users/nickl/OneDrive/Desktop/output/medoids/variation/2";


        single(input, temp, singleOutput);

        basic(3,input, temp, basicOutput);

        advanced(100, input, temp, advancedOutput, 3000);

        optimization(100, input, temp, optimizationOutput, 3000);
    }

}
