import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class outputVariations {
    public static boolean end = false;

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
                    if (line.contains("Threshold")){
                        continue;
                    }
                    String[] split = line.split("\t");
                    System.out.println(split[0]);
                    centroidsList.add(split[0]);

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
//            System.out.println("Map: " + point);

            int x;
            int y;
            // Split by Column
            String[] split = point.split(",");

            if (split.length > 2) {
                x = Integer.parseInt((split[3]));
                y = Integer.parseInt((split[4]));

            } else {
                x = Integer.parseInt((split[0]));
                y = Integer.parseInt((split[1]));
            }

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
//            System.out.println(centroid);
            outkey.set(centroid);
            outvalue.set(point);
            context.write(outkey, outvalue);

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text newCentroid = new Text();
        private Text outPoints = new Text();

        private ArrayList<Text> newList = new ArrayList<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int newCentroidX = 0;
            int newCentroidY = 0;
            int count = 0;

            String points = "";



            // For each relationship of a user
            for (Text value : values) {
                String current = value.toString();

                String[] split = current.split(",");
                int partialSumX = Integer.parseInt(split[0]);
                int partialSumY = Integer.parseInt(split[1]);
                int partialCount = Integer.parseInt(split[2]);

                if (split.length > 3) {
                    String[] currentPoints = split[3].split(";");

                    for (int i = 0; i < currentPoints.length; i++) {
                        points += "(" + currentPoints[i].split(":")[0] + ", " + currentPoints[i].split(":")[1] + ");";
                    }
                }


                newCentroidX += partialSumX;
                newCentroidY += partialSumY;
                count += partialCount;
            }

            newCentroidX = (int) newCentroidX / count;
            newCentroidY = (int) newCentroidY / count;

            int oldCentroidX = Integer.parseInt(key.toString().split(",")[0]);
            int oldCentroidY = Integer.parseInt(key.toString().split(",")[1]);

            double distance = Math.sqrt((Math.pow((newCentroidX - oldCentroidX), 2)) + (Math.pow((newCentroidY - oldCentroidY), 2)));
            System.out.println("Old Centroid: (" + oldCentroidX + ", " + oldCentroidY + "). New Centroid: (" + newCentroidX + ", " + newCentroidY + "). Distince: " + distance + ". Threshold: " + Integer.parseInt(context.getConfiguration().get("threshold")));
            if (distance >= Integer.parseInt(context.getConfiguration().get("threshold"))){
                end = false;
            }

            if (context.getConfiguration().get("variation").equals("Only Cluster Points")) {
                newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY

                context.write(newCentroid, null); // Write <key, value> = <User, Count of Relationships>

            }
            else{
                newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY
                outPoints.set(points);
                context.write(newCentroid, outPoints);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            if (context.getConfiguration().get("variation").equals("Only Cluster Points")) {
                if (end) {
                    context.write(new Text("Convergence Threshold Met"), null);
                    System.out.println("End");
                } else {
                    context.write(new Text("Convergence Threshold NOT Met"), null);
                    System.out.println("NOT End");
                }
            }
        }

    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int xSum = 0;
            int ySum = 0;
            int count = 0;
            String points = "";



            // For each relationship of a user
            for (Text value : values) {

                String current = value.toString();

                int currentx;
                int currenty;

                String[] split = current.split(",");

                if (split.length > 2) {
                    currentx = Integer.parseInt((split[3]));
                    currenty = Integer.parseInt((split[4]));

                } else {
                    currentx = Integer.parseInt((split[0]));
                    currenty = Integer.parseInt((split[1]));
                }

                String currentPoint = currentx + ":" + currenty + ";";
                points += currentPoint;

                xSum += currentx;
                ySum += currenty;
                count++;
            }

            if (context.getConfiguration().get("variation").equals("Only Cluster Points")) {

                // Emit the partial sum and count
                result.set(xSum+","+ySum+","+count);
                context.write(key, result);
            }
            else{
                result.set(xSum+","+ySum+","+count +"," + points);
                context.write(key, result);

            }

        }
    }

    private static void simple(String input, String tempOutput, String output, int threshold, String variation) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        end = true;
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Test");

        job1.setJarByClass(optimization.class);
        job1.setMapperClass(Map.class);
        job1.setCombinerClass(Combiner.class);
        job1.setReducerClass(Reduce.class);

        job1.getConfiguration().set("threshold", String.valueOf(threshold));
        job1.getConfiguration().set("variation", variation);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addCacheFile(new URI(tempOutput));

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

    }
    //        job2.getConfiguration().set("join.type", "inner");
    public static void looping(int r, String startInput, String tempOutput, String output, int threshold, String variation) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String currentTemp = tempOutput;
        String Output = output;

        for (int i = 0 ; i < r; i++){
            String currentOutput = Output + "/" + i;

            simple(startInput, currentTemp, currentOutput, threshold, variation); //

            if (end){
                System.out.println("Threshold Met");
                break;
            }

            currentTemp = currentOutput + "/part-r-00000";
        }
    }

    public static void main(String[] args) throws Exception {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
//        String input = "/Users/mikaelamilch/Library/CloudStorage/OneDrive-WorcesterPolytechnicInstitute(wpi.edu)/2023-2024/CS 585/CS585-Assignment2/src/main/python/datasetTest.csv";
//        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/output/optimization";
//        String output = "/Users/mikaelamilch/Desktop/output";

        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output/variations";

        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeans.csv";

        String variation1 = "Only Cluster Points";
        String variation2 = "Final Clustered Points";

        long start = System.currentTimeMillis();
//        looping(100, input, temp, output, 500, variation1);
        looping(100, input, temp, output, 500, variation2);

        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);
    }

}
