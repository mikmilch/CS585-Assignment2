import java.io.IOException;
import java.net.URISyntaxException;

public class BYOD {

    public static void single(String input, String temp, String output) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        kmeans.simple(input, temp, output);

    }
    public static void basic(){

    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String input = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/Mall_Customers.csv";
        String output = "C:/Users/nickl/OneDrive/Desktop/output/BYOD/single";

        String temp = "file:///C:/Users/nickl/OneDrive/Desktop/Testing/BYOD_kmeans_dataset.csv";

        single(input, temp, output);
    }
}
