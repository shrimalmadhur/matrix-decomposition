package spark_example.matrix_decomposition;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.Vector;
import scala.collection.immutable.VectorBuilder;


public final class MatrixMultiplication {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("local[2]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//    JavaRDD<String> lines = ctx.textFile(args[0], 1);
    int numberOfPartitions = 2;
    int rows = 4;
    int cols = 4; 
    int rowKey = rows/numberOfPartitions;
    int colKey = cols/numberOfPartitions;
    
//    Matrix user = Matrices.dense(rows, cols, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 1.2, 2.3, 1.3, 3.4, 5.6, 2.3, 4.5, 6.7, 1.2, 2.3});
//    Matrix movies = Matrices.dense(rows, cols, new double[] {2.0, 4.0, 6.0, 8.0, 10.0, 6.0, 1.2, 2.3, 1.3, 3.4, 5.6, 2.3, 4.5, 6.7, 1.2, 2.3});
    
    List<Vector<Integer>> user = new ArrayList<Vector<Integer>>();
    
    for(int i=0; i < rows; i++){
    	VectorBuilder<Integer> vb = new VectorBuilder<Integer>();
    	for(int j=0; j < cols; j++){
    		vb.$plus$eq(Integer.valueOf(i+j));
    	}
    	Vector<Integer> vec = vb.result();
    	System.out.println(vec.size());
    	user.add(vec);
    }
    
    List<Vector<Integer>> movies = new ArrayList<Vector<Integer>>();
    
    for(int i=0; i < rows; i++){
    	VectorBuilder<Integer> vb = new VectorBuilder<Integer>();
    	for(int j=0; j < cols; j++){
    		vb.$plus$eq(Integer.valueOf(i+j));
    	}
    	Vector<Integer> vec = vb.result();
    	System.out.println(vec.size());
    	movies.add(vec);
    }
    
    JavaRDD<Vector<Integer>> userRdd = ctx.parallelize(user, numberOfPartitions);
    JavaRDD<Vector<Integer>> movieRdd = ctx.parallelize(movies, numberOfPartitions);
    JavaPairRDD<Vector<Integer>, Vector<Integer>> a = userRdd.cartesian(movieRdd);
    List<Tuple2<Vector<Integer>,Vector<Integer>>> arr = a.collect();
    JavaRDD<Vector<Integer>> vec =  a.values();
    java.util.Iterator<Tuple2<Vector<Integer>, Vector<Integer>>> it = arr.iterator();
    while(it.hasNext()){
    	Tuple2<Vector<Integer>, Vector<Integer>> temp = it.next();
    	scala.collection.immutable.List<Integer> t1 = temp._1.toList();
    	scala.collection.immutable.List<Integer> t2 = temp._2.toList();
//    	for(int i=0 ; i < t1)
    }
//    List<Tuple2<Vector<Integer>,Vector<Integer>>> arr = a.toArray();

    List<Vector<Integer>> col = vec.collect();
    System.out.println("Size: " + col.size());
    for(Vector<Integer> temp: col){
    	Iterator<Integer> itr = temp.toIterator();
    	while(itr.hasNext()){
    		System.out.print(itr.next());
    	}
    	System.out.println();
    }
    System.out.println(a.toString());
    
   
    
    ctx.stop();
  }
}

