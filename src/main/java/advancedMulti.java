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

public class advancedMulti {

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
            System.out.println(centroid);
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

//            System.out.println(key);

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

            int oldCentroidX = Integer.parseInt(key.toString().split(",")[0]);
            int oldCentroidY = Integer.parseInt(key.toString().split(",")[1]);

            double distance = Math.sqrt((Math.pow((newCentroidX - oldCentroidX), 2)) + (Math.pow((newCentroidY - oldCentroidY), 2)));
            System.out.println("Old Centroid: (" + oldCentroidX + ", " + oldCentroidY + "). New Centroid: (" + newCentroidX + ", " + newCentroidY + "). Distince: " + distance + ". Threshold: " + Integer.parseInt(context.getConfiguration().get("threshold")));
            if (distance >= Integer.parseInt(context.getConfiguration().get("threshold"))){
                end = false;
            }
            newCentroid.set(String.valueOf(newCentroidX) + "," + String.valueOf(newCentroidY)); // Key = averageX , averageY

            context.write(newCentroid, null); // Write <key, value> = <User, Count of Relationships>
        }
    }

    private static void simple(String input, String tempOutput, String output, int threshold) throws IOException, URISyntaxException,ClassNotFoundException, InterruptedException {

        long start = System.currentTimeMillis();
        end = true;
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Test");

        job1.setJarByClass(advancedMulti.class);
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.getConfiguration().set("threshold", String.valueOf(threshold));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.addCacheFile(new URI(tempOutput));

        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));
        job1.waitForCompletion(true);
        long end = System.currentTimeMillis();
        long timeTaken = end - start;
        System.out.println("Time Taken: " + timeTaken);
    }
//        job2.getConfiguration().set("join.type", "inner");
    private static void looping(int r, String startInput, String tempOutput, String output, int threshold) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        String currentTemp = tempOutput;
        String Output = output;

        for (int i = 0 ; i < r; i++){
            String currentOutput = Output + "/" + i;

            simple(startInput, currentTemp, currentOutput, threshold); //

            if (end){
                System.out.println("Threshold Met");
                break;
            }

            currentTemp = currentOutput + "/part-r-00000";
        }
    }

    public static void main(String[] args) throws Exception {

        String input = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/src/main/python/dataset.csv";
//        String output = "file:///C:/Users/nickl/OneDrive/Desktop/WPI Graduate/CS585 Big Data Management/Project2/output";
        String output = "file:///C:/Users/nickl/OneDrive/Desktop/output/advanced";

        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/kmeans.csv";

        looping(100, input, temp, output, 500);

    }
}
