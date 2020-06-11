package Group.DT;

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

import java.util.ArrayList;

public class DecisionTree {
    static Long[] seeds = {0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L};

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\spark-2.4.5-bin-hadoop2.7");

        String name = "Spark Decision Tree";

        SparkSession spark = SparkSession.builder()
                .appName(name)
                .master("local")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\Ass3\\data\\kdd.data").toJavaRDD();
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

        //Write the results to a file
        //In the hdfs?
    }

    public static ArrayList<String> runDecisionTree(Dataset data) {
        ArrayList<String> results = new ArrayList<>();

        for (Long seed : seeds) {
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
