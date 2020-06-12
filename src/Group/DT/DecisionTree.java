package Group.DT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

//Some helpful notes maybe: https://stackoverflow.com/questions/7612347/running-java-hadoop-job-on-local-remote-cluster

public class DecisionTree {
    static Long[] seeds = {0L};
//    1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

    public static boolean deleteOutputs(String outputDirName) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path o = new Path(outputDirName);
        if (!hdfs.delete(o, true)) {
            throw new FileNotFoundException("Failed to delete file : " + o);
        }
        return true;
    }

    //Run the program with job initialising for the hadoop cluster
    public static void main(String[] args) throws Exception {
        //????????
        System.setProperty("hadoop.home.dir", "C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\spark-2.4.5-bin-hadoop2.7");

        Configuration conf = new Configuration();

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        if (args.length > 2) {
            if (args[2].equals("yes") || args[2].equals("y")) {
                deleteOutputs(args[1]);
            } else {
                System.out.println("Last arg for clearing output directory. Values accepted: [\"yes\", \"y\"]");
            }
        }

        Job job = Job.getInstance(conf, "Spark Decision Tree");
        job.setJarByClass(DecisionTreeExec.class);

        //Yes
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //Create the read and write types of the input and output, both text types
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }

    //Decision Tree class to create JAR for hadoop cluster execution
    //Maybe merge with class above and use DecisionTree class as execution wrapper
    public static class DecisionTreeExec {
        static String name = "Spark Decision Tree";

        static SparkSession spark = SparkSession.builder()
                .appName(name)
                .master("local")
                .getOrCreate();

        //Take file and file contents and load into a denseVector format with labels for the data
        public static void main(String[] args) {
            //remote cluster file path "/users/toddchri1/input/kdd.data"
            //"/users/toddchri1/output"
            JavaRDD<String> lines = spark.read().textFile("C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\Ass3\\data\\kdd.data").toJavaRDD();
            //Split into class Map extends Map
            JavaRDD<LabeledPoint> linesRDD = lines.map(line -> {
                String[] tokens = line.split(",");
                double[] features = new double[tokens.length - 1];
                for (int i = 0; i < features.length; i++) {
                    features[i] = Double.parseDouble(tokens[i]);
                }
                Vector v = new DenseVector(features);
                if (tokens[features.length].equals("anomaly")) {
                    return new LabeledPoint(0.0, v);
                } else {
                    return new LabeledPoint(1.0, v);
                }
            });

            //The data
            Dataset<Row> data = spark.createDataFrame(linesRDD, LabeledPoint.class);

            //Run an algorithm
            ArrayList<String> output = runDecisionTree(data);

            //Write the results to a file
            //In the hdfs?
        }

        //The actual decision tree
        public static ArrayList<String> runDecisionTree(Dataset data) {
            ArrayList<String> results = new ArrayList<>();

            for (Long seed : seeds) {
                //Data splits
                Dataset[] splits = data.randomSplit(new double[]{0.7, 0.3}, seed);
                Dataset trainingData = splits[0];
                Dataset testData = splits[1];

                DecisionTreeClassifier dt = new DecisionTreeClassifier()
                        .setLabelCol("label")
                        .setFeaturesCol("features");

                Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{dt});

                PipelineModel model = pipeline.fit(trainingData);

                Dataset<Row> predictions = model.transform(testData);

                predictions.select("prediction", "label", "features").show(10);

                MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                        .setLabelCol("label")
                        .setPredictionCol("prediction")
                        .setMetricName("accuracy");

                double accuracy = evaluator.evaluate(predictions);
                System.out.println("Test Error = " + (1.0 - accuracy));

                DecisionTreeClassificationModel treeModel = (DecisionTreeClassificationModel) (model.stages()[0]);
                System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());

                results.add("Seed: " + seed + ", Accuracy: " + accuracy);
            }

            return results;
        }
    }
}
