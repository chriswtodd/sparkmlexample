package Base.DelayTime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //????????
//        System.setProperty("hadoop.home.dir", "C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\spark-2.4.5-bin-hadoop2.7");

//        DecisionTreeExec dt = new DecisionTreeExec();

//        dt.main(new String[1]);

        String name = "Spark Decision Tree";

        SparkSession spark = SparkSession.builder()
                .appName(name)
//                .master("local")
                .getOrCreate();

        Configuration conf = new Configuration();

        Path inputPath = new Path("/user/toddchri1/input");
        Path outputPath = new Path("/user/toddchri1/output");
//        Path inputPath = new Path(args[0]);
//        Path outputPath = new Path(args[1]);

//        if (args.length > 2) {
//            if (args[2].equals("yes") || args[2].equals("y")) {
//                deleteOutputs(args[1]);
//            } else {
//                System.out.println("Last arg for clearing output directory. Values accepted: [\"yes\", \"y\"]");
//            }
//        }

        Job job = Job.getInstance(conf, "Spark Decision Tree");

        //Yes
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //Create the read and write types of the input and output, both text types
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
