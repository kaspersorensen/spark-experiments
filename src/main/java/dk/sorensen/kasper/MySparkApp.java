package dk.sorensen.kasper;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * To test _this_ app, use:
 * 
 * bin\spark-submit --master yarn-client --class dk.sorensen.kasper.MySparkApp c:\Users\kaspers\git\spark-experiments\target\spark-experiments.jar hdfs://user/vagrant/big_example_data.csv hdfs://user/vagrant/spark1
 * 
 * To run a sample, use:
 * 
 * bin\spark-submit --class org.apache.spark.examples.SparkPi --master yarn-cluster lib/spark-examples*.jar 10
 */
public class MySparkApp {

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Provide command line args");
        }

        final String path1 = args[0];
        final String path2 = args[0];

        MySparkApp app = new MySparkApp();
        app.run(path1, path2, null);
    }

    public void run(String path1, String path2, String master) {
        System.out.println("Hello " + path1 + "to" + path2 + "!");

        final SparkConf conf = new SparkConf().setAppName("KaspersApp");
        if (master != null) {
            conf.setMaster(master);
        }

        final JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            final JavaRDD<Integer> list = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
            final JavaRDD<String> textFile = sc.textFile(path1);

            final JavaPairRDD<String, Integer> cartesian = textFile.cartesian(list);
            System.out.println("Cartesian count: " + cartesian.count());
            
            if (path2 != null) {
                cartesian.saveAsTextFile(path2);
            }
        } finally {
            sc.close();
        }
    }
}
