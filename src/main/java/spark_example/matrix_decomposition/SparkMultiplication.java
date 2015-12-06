package spark_example.matrix_decomposition;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import scala.Tuple2;

import java.util.List;

public class SparkMultiplication {
	public static void main(String[] args) {
		CommandLineParser parser = new GnuParser();

        int n = 10;
        long uid = 0;

        try {
            CommandLine cmd = parser.parse(getOptions(), args);

            if(cmd.hasOption("user")) {
                uid = Integer.parseInt(cmd.getOptionValue("user"));
            } else {
                System.out.println("User id not provided!");
                return;
            }

            if(cmd.hasOption("n")) {
                n = Integer.parseInt(cmd.getOptionValue("n"));
            }
        } catch (java.lang.NumberFormatException e) {
            System.out.println("Invalid number format!");
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }

		SparkConf sparkConf = new SparkConf().setAppName("App");
	    JavaSparkContext sc = new JavaSparkContext(sparkConf);
	    
	    LocalitySensitiveHash lsh = new LocalitySensitiveHash(1.0, 4);
	    System.out.println("Num partitions -> " + lsh.getNumPartitions());
	    
	    JavaRDD<String> usersFile = sc.textFile("/res/users.txt");
	    
	    JavaRDD<IndexedRow> usersRDD = usersFile.map(new Function<String, IndexedRow>(){

			@Override
			public IndexedRow call(String v1) throws Exception {
				String[] arr = v1.split(",");
				long key = Long.parseLong(arr[0]);
				int len = arr.length;
				double[] vec = new double[len-1];
				for(int i=0; i < len-1; i++){
					vec[i] = Double.parseDouble(arr[i+1]);
				}
				Vector v = new DenseVector(vec);
				
				return new IndexedRow(key, v);
			}
	    });
	    
	    
	    JavaRDD<String> moviesFile = sc.textFile("/res/items.txt");
	    
	    JavaRDD<IndexedRow> moviesRDD = moviesFile.map(new Function<String, IndexedRow>(){

			@Override
			public IndexedRow call(String v1) throws Exception {
				String[] arr = v1.split(",");
				long key = Long.parseLong(arr[0]);
				int len = arr.length;
				double[] vec = new double[len-1];
				for(int i=0; i < len-1; i++){
					vec[i] = Double.parseDouble(arr[i+1]);
				}
				Vector v = new DenseVector(vec);
				
				return new IndexedRow(key, v);
			}
	    });

        final long userId = uid;
	    JavaRDD<IndexedRow> a = usersRDD.filter(new Function<IndexedRow, Boolean>() {

			@Override
			public Boolean call(IndexedRow v1) throws Exception {
				
				return v1.index() == userId;
			}
		});
	    
	    
	    final double[] user = a.first().vector().toArray();
	    printDoubleArr("user", user);
	    
	    JavaRDD<Tuple2<Long, Double>> result = moviesRDD.map(new Function<IndexedRow, Tuple2<Long, Double>>() {

			@Override
			public Tuple2<Long, Double> call(IndexedRow v1) throws Exception {
				double[] movie = v1.vector().toArray();
				
				long key = v1.index();
				printDoubleArr("movie"+key , movie);
				Double val = VectorMath.dot(user, movie);
				return new Tuple2<Long, Double>(key, val);
			}
		});
	    result.saveAsTextFile("res/output");
	    List<Tuple2<Long, Double>> ret = result.collect();

        for (int i = 0; i < n; i++) {
            System.out.println(ret.get(i)._1 + " " + ret.get(i)._2);
        }
	    
	    sc.stop();
	}
	
	public static void printDoubleArr(String name, double[] arr){
		System.out.println(name);
	    for(int i=0; i < arr.length; i++){
	    	System.out.print(arr[i] + " ");
	    }
	    System.out.println();
	}

	private static Options getOptions() {
		Options options = new Options();
		options.addOption(new Option("user", true, "The user id to whom we are recommending"));
		options.addOption(new Option("n", true, "Number of recommendations"));
		return options;
	}
}
