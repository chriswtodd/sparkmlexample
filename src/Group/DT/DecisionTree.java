package Group.DT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.*;
import java.util.ArrayList;

public class DecisionTree {
    static Long[] seeds = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

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
        //*************************************************************************************************************
        if (args.length < 1) {
            throw new IllegalArgumentException("Please provide input path");
        }

        String name = "Spark Decision Tree";
        SparkSession spark = SparkSession.builder()
                .appName(name)
//                .master("local")
                .getOrCreate();

        //Take file and file contents and load into a denseVector format with labels for the data
        //remote cluster file path "/users/toddchri1/input/kdd.data"
        //"/users/toddchri1/output"
        JavaRDD<String> lines = spark.read().textFile(args[0]).toJavaRDD();
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

        System.out.println(output);

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path file = new Path(args[1]);
        OutputStream os = hdfs.create(file);
        BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );


        //Write the results to a file
        //In the hdfs?
        if (args.length == 2) {
            try {
                for (String s : output) {
                    System.out.println(s);
                    br.write(s);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        br.close();
        hdfs.close();
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
