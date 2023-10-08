import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;
import java.net.URISyntaxException;

public class kmeans {


//    Step 2 (Clustering the Data):
//    Develop the K-means clustering strategies described below using java map-reduce
//    jobs. You should document the differences between your solutions, i.e., what changes
//    were made to the mapper, at the reducer, number of mappers/reducers, or in the
//    main control program. These algorithms should include:
//    a) A single-iteration K-means algorithm (R=1) [5 pts]
//    b) A (basic) multi-iteration K-means algorithm (remember to set the parameter R
//            to terminate the process, e.g., R=10). [5 pts]
//    c) An additional (advanced) multi-iteration K-means algorithm that terminates
//    potentially earlier if it converges based on some threshold. [5 pts]
//    d) An additional (advanced) algorithm that also uses Hadoop Map Reduce
//    optimizations as discussed in class (e.g., a combiner). [5 pts]
//    e) For your K-means solution in subproblem (d) above, design two output
//    variations:
//    i. return only cluster centers along with an indication if convergence has
//    been reached; [5 pts]
//    ii. return the final clustered data points along with their cluster centers. [5
//    pts]
//    f) Provide a description for each of your above five solutions in your report and
//    conduct experiments over them, for example by choosing different K values
//    and different R values. describe the relative performance, explain, and analyze
//    your findings. [10 pts]
//    Note: Since the algorithm is iterative, you need your main program that generates the
//    map-reduce jobs to also control whether it should start another iteration.



    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outkey = new Text();
        private final Text outvalue = new Text();

        ArrayList<String> centroidsList = new ArrayList<String>();

        /*
        Read in K means data set and store in local memory
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
            String line;

            while (StringUtils.isNotEmpty(line = br.readLine())) {
                try {
                    centroidsList.add(line);
//                    String[] split = line.split("/t");
//                    accessLogMap.put(split[0], split[1]);
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
            // close the stream
            IOUtils.closeStream(br);
        }

        /*
        Map each data point to a k-means cluster based on Euclidean distance formula
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            double minDistance = Double.MAX_VALUE;
            String centroid = "";
            // Data point x and y values
            String point = value.toString();

            // Split by Column
            String[] split = point.split(",");

            int x = Integer.parseInt((split[0]));
            int y = Integer.parseInt((split[1]));

            for (int i = 0; i < centroidsList.size(); i++){

                String current = centroidsList.get(i);

                String[] currentSplit = current.split(",");

                int currentx = Integer.parseInt(currentSplit[0]);
                int currenty = Integer.parseInt(currentSplit[1]);

                //Euclidean distance formula
                double distance = Math.sqrt((Math.pow((currentx - x), 2)) + (Math.pow((currenty - y), 2)));

                if (distance < minDistance){
                    minDistance = distance;
                    centroid = current;
                }

            }

            outkey.set(centroid);
            outvalue.set(point);
            context.write(outkey, outvalue);

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        private Text newCentroid = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int newCentroidX = 0;
            int newCentroidY = 0;
            int count = 0;

            System.out.println(values);

            // For each relationship of a user
            for (Text value : values) {
                String current = value.toString();

                String[] split = current.split(",");

                int currentx = Integer.parseInt(split[0]);
                int currenty = Integer.parseInt(split[1]);

                newCentroidX += currentx;
                newCentroidY += currenty;
                count++;
            }

            newCentroidX = (int) newCentroidX / count;
            newCentroidY = (int) newCentroidY / count;

//            System.out.println(newCentroidX);
            newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY

            context.write(newCentroid, null); // Write <key, value> = <User, Count of Relationships>
        }
    }

    private static void simple(String input, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {


        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Test");

        job1.setJarByClass(kmeans.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addCacheFile(new URI("file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeansTest.csv"));

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);

    }

    public static void main(String[] args) throws Exception {

//        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/datasetTest.csv";

        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/output";

        simple(input, output);

    }
}
