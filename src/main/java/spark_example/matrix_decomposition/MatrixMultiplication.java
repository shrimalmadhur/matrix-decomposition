package spark_example.matrix_decomposition;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.rdd.RDD;

public final class MatrixMultiplication {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

//    if (args.length < 1) {
//      System.err.println("Usage: JavaWordCount <file>");
//      System.exit(1);
//    }

    SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("local[2]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//    JavaRDD<String> lines = ctx.textFile(args[0], 1);
    int numberOfPartitions = 2;
    int rows = 4;
    int cols = 4; 
    
    Matrix user = Matrices.dense(rows, cols, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0, 1.2, 2.3, 1.3, 3.4, 5.6, 2.3, 4.5, 6.7, 1.2, 2.3});
    Matrix movies = Matrices.dense(rows, cols, new double[] {2.0, 4.0, 6.0, 8.0, 10.0, 6.0, 1.2, 2.3, 1.3, 3.4, 5.6, 2.3, 4.5, 6.7, 1.2, 2.3});
    
//    System.out.println(user.index(3, 2)/4);
    for(int i=0; i < rows; i++){
    	for(int j=0; j < cols; j++){
    		System.out.print(user.apply(i, j) + " ");
    	}
    	System.out.println();
    }
    
//    ctx.
    int rowKey = rows/numberOfPartitions;
    int colKey = cols/numberOfPartitions;
    
    RDD<String> rdd = user
    
    
    
    
//    user.
    
//    JavaRDD<IndexedRow> rows =  
//    IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());
    
    ctx.stop();
  }
  public static JavaRDD<Vector> toRDD(Matrix m){
	  m.toArray()
  }
}

