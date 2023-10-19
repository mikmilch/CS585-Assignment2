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

/**
 * d) An additional (advanced) algorithm that also uses Hadoop Map Reduce
 * optimizations as discussed in class (e.g., a combiner). [5 pts]
 */

public class optimization {

    // Boolean to keep track of threshold and whether to stop
    public static boolean end = false;

    // Mapper that takes in dataset points with k centroids and maps each point to the nearest centroids
    // Consumes the dataset points and k initial points
    // Produces <Centroid, Point>
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outkey = new Text();
        private final Text outvalue = new Text();

        // List to keep centroids (k initial points)
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

                int currentx;
                int currenty;

                if (split.length > 2) {
                    currentx = Integer.parseInt((currentSplit[3]));
                    currenty = Integer.parseInt((currentSplit[4]));

                } else {
                    currentx = Integer.parseInt((currentSplit[0]));
                    currenty = Integer.parseInt((currentSplit[1]));
                }

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

    // Takes in centroid and partial sum from the Combiner and calculate for the new centroid points based on points of each k cluster
    // Consumes <Centroid, partial sum>
    // Produces <New Centroid, >
    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        private Text newCentroid = new Text();

        /*
        Calculate the average of all the points in the cluster to get new centroids
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int newCentroidX = 0;
            int newCentroidY = 0;
            int count = 0;

            // For each point belonging in the kmeans cluster
            for (Text value : values) {

                // Current Point
                String current = value.toString();
                String[] split = current.split(",");

                // Add to sum and count to get average
                int partialSumX = Integer.parseInt(split[0]);
                int partialSumY = Integer.parseInt(split[1]);
                int partialCount = Integer.parseInt(split[2]);

                newCentroidX += partialSumX;
                newCentroidY += partialSumY;
                count += partialCount;
            }

            // New Centroid is the average of all the points in the cluster
            newCentroidX = (int) newCentroidX / count;
            newCentroidY = (int) newCentroidY / count;

            // Old Centroid
            int oldCentroidX = Integer.parseInt(key.toString().split(",")[0]);
            int oldCentroidY = Integer.parseInt(key.toString().split(",")[1]);

            // Distance Between new and old centroids
            double distance = Math.sqrt((Math.pow((newCentroidX - oldCentroidX), 2)) + (Math.pow((newCentroidY - oldCentroidY), 2)));
//            System.out.println("Old Centroid: (" + oldCentroidX + ", " + oldCentroidY + "). New Centroid: (" + newCentroidX + ", " + newCentroidY + "). Distince: " + distance + ". Threshold: " + Integer.parseInt(context.getConfiguration().get("threshold")));

            // If threshold Not Met (Some of the newer centroids move more than the threshold)
            if (distance >= Integer.parseInt(context.getConfiguration().get("threshold"))){
                end = false;
            }
            newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY

            context.write(newCentroid, null); // Write <key, value> = <User, Count of Relationships>
        }
    }

    // Combiner that takes in the outputs from the mapper and calculates the partial sum
    // Comsumes <Centroid, Point>
    // Produces <Centroid, Partial Sum>
    public static class Combiner extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int xSum = 0;
            int ySum = 0;
            int count = 0;

            // For each point belonging in the kmeans cluster
            for (Text value : values) {

                // Current Point
                String current = value.toString();
                String[] split = current.split(",");

                int currentx;
                int currenty;

                if (split.length > 2) {
                    currentx = Integer.parseInt((split[3]));
                    currenty = Integer.parseInt((split[4]));

                } else {
                    currentx = Integer.parseInt((split[0]));
                    currenty = Integer.parseInt((split[1]));
                }

                // Add to Partial Sum
                xSum += currentx;
                ySum += currenty;
                count++;
            }

            // Emit the partial sum and count
            result.set(xSum+","+ySum+","+count);
            context.write(key, result);
        }
    }

    private static void simple(String input, String tempOutput, String output, int threshold) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        end = true;
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Single Iteration KMeans");

        job1.setJarByClass(optimization.class);
        job1.setMapperClass(Map.class);
        job1.setCombinerClass(Combiner.class);
        job1.setReducerClass(Reduce.class);

        job1.getConfiguration().set("threshold", String.valueOf(threshold));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addCacheFile(new URI(tempOutput));

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);

    }

    // Multi-Iteration KMean given r, the number of iterations, and ending threshold
    public static void looping(int r, String startInput, String tempOutput, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String currentTemp = tempOutput;
        String Output = output;

        for (int i = 0 ; i < r; i++){
            String currentOutput = Output + "/" + i;

            simple(startInput, currentTemp, currentOutput, threshold); //

            // If Threshold Met, End
            if (end){
//                System.out.println("Threshold Met");
                break;
            }

            currentTemp = currentOutput + "/part-r-00000";
        }
    }

}