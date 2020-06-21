package Base.DelayTime;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        String name = "Spark Clustering";
//        SparkSession spark = SparkSession.builder()
//                .appName(name)
////                .master("local")
//                .getOrCreate();

//        Dataset<Row> censusCSV = spark.read().format("csv").option("inferSchema"
//                , "true").option("header", "true").load(args[0]);
//        StructField[] fields = {new StructField("features", new VectorUDT(), false, Metadata.empty())};
//        StructType schema = new StructType(fields);

        SparkConf conf = new SparkConf().setAppName("Spark Clustering");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

        int[] count = {0};
        JavaRDD<String> points = jsc.textFile(args[0]);
        JavaRDD<Row> rowRDD = points.map(point -> {
            if (count[0] >= 1) {
                return RowFactory.create();
            } else {
                count[0]++;
                System.out.println(count[0] + " " + point);
                return null;
            }
        }).filter(Objects::nonNull);
        List<StructField> fields = Collections.singletonList(
                DataTypes.createStructField("features", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> df = sqlContext.createDataFrame(rowRDD, schema);

        //Label one as the prediction or whatever
//        df.select("features").show(1);

        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel model = kmeans.fit(df);

        Dataset<Row> predictions = model.transform(df);

        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);

        Vector[] centers = model.clusterCenters();
        System.out.println("Cluster Centers: ");
        for (Vector center: centers) {
            System.out.println(center);
        }

        //????????
//        System.setProperty("hadoop.home.dir", "C:\\Users\\Black Howler\\Documents\\Nearly $100,000 of Knowledge\\Lecture Notes\\COMP424\\spark-2.4.5-bin-hadoop2.7");

//        Configuration conf = new Configuration();
//
//        Path inputPath = new Path("/user/toddchri1/input");
//        Path outputPath = new Path("/user/toddchri1/output");
//
////        if (args.length > 2) {
////            if (args[2].equals("yes") || args[2].equals("y")) {
////                deleteOutputs(args[1]);
////            } else {
////                System.out.println("Last arg for clearing output directory. Values accepted: [\"yes\", \"y\"]");
////            }
////        }
//
//        Job job = Job.getInstance(conf, "Spark Decision Tree");
//
//        //Yes
//        FileInputFormat.addInputPath(job, inputPath);
//        FileOutputFormat.setOutputPath(job, outputPath);
//
//        //Create the read and write types of the input and output, both text types
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        job.waitForCompletion(true);
    }

    public static class clusterClass {

    }
}
