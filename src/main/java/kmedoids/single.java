package kmedoids;

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
 * a) A single-iteration K-means algorithm (R=1) [5 pts]
 */

public class single {


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

            outkey.set(centroid);
            outvalue.set(point);
            context.write(outkey, outvalue);

        }
    }

    // Takes in centroid and point from the mapper and calculate for the new centroid points based on points of each k cluster
    // Instead of Taking the Average / Calculate the best medoid from existing points by getting the total distance from that point of all points
    // Consumes <Centroid, Point>
    // Produces <New Centroid, >
    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        private Text newCentroid = new Text();

        /*
        Calculate the total distance of all of the points from a existing point (current medoid)
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int newCentroidX = 0;
            int newCentroidY = 0;
            int count = 0;

            int minDistance = Integer.MAX_VALUE;

            // For each point belonging in the kmeans cluster
            for (Text value : values) {
                int sumDistance = 0;

                String current = value.toString();

                String[] split = current.split(",");

                int currentx = Integer.parseInt(split[0]);
                int currenty = Integer.parseInt(split[1]);

                // For every other point
                for (Text other : values){
                    String otherPoint = other.toString();

                    String[] otherSplit = otherPoint.split(",");

                    int otherx = Integer.parseInt(otherSplit[0]);
                    int othery = Integer.parseInt(otherSplit[1]);

                    //Euclidean distance formula
                    double distance = Math.sqrt((Math.pow((currentx - otherx), 2)) + (Math.pow((currenty - othery), 2)));

                    // Get Total distance
                    sumDistance += (int) distance;
                    if (sumDistance > minDistance){
                        break;
                    }
                }

                // Get Best medoid point
                if (sumDistance < minDistance){
                    minDistance = sumDistance;
                    newCentroidX = currentx;
                    newCentroidY = currenty;
                }

            }
            newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY

            context.write(newCentroid, null); // Write <key, value> = <User, Count of Relationships>
        }
    }

    public static void simple(String input, String temp, String output) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Single Iteration KMedoid");

        job1.setJarByClass(single.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addCacheFile(new URI(temp));

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
        long end = System.currentTimeMillis();


    }

}
