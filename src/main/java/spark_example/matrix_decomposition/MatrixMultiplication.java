package spark_example.matrix_decomposition;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.Function1;


public final class MatrixMultiplication {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("App").setMaster("local[3]");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    int numberOfPartitions = 2;
    int rows = 4;
    int cols = 4; 
    int rowKey = rows/numberOfPartitions;
    int colKey = cols/numberOfPartitions;


    JavaRDD<String> distFile = sc.textFile("res/matrix");
    
    JavaRDD<org.apache.spark.mllib.linalg.Vector> movRDD = distFile.map(new Function<String, org.apache.spark.mllib.linalg.Vector>() {
      public org.apache.spark.mllib.linalg.Vector call(String s) throws Exception {
        String[] sa = s.split(",");
        double[] da = new double[sa.length];
        for(int i = 0; i < sa.length; i++) {
          da[i] = Double.valueOf(sa[i]);
        }
        return Vectors.dense(da);
      }
    });
//    int count = 0;
    JavaRDD<IndexedRow> indexedRDD = distFile.map(new Function<String, IndexedRow>() {
      public IndexedRow call(String s) throws Exception {
        String[] sa = s.split(",");
        double[] da = new double[sa.length];
        for(int i = 0; i < sa.length; i++) {
          da[i] = Double.valueOf(sa[i]);
        }
        return new IndexedRow((long)da[0], Vectors.dense(da));
      } 
    });
    
    JavaRDD<String> distFile1 = sc.textFile("res/new");
    JavaRDD<IndexedRow> newindexedRDD = distFile1.map(new Function<String, IndexedRow>() {
        public IndexedRow call(String s) throws Exception {
          String[] sa = s.split(",");
          double[] da = new double[sa.length];
          for(int i = 0; i < sa.length; i++) {
            da[i] = Double.valueOf(sa[i]);
          }
          return new IndexedRow((long)da[0], Vectors.dense(da));
        } 
      });
    
    indexedRDD.union(newindexedRDD);
    
    JavaRDD<IndexedRow> i = indexedRDD.filter(new Function<IndexedRow, Boolean>() {

		@Override
		public Boolean call(IndexedRow v1) throws Exception {
			// TODO Auto-generated method stub
			
			return v1.index() == 2;
		}
    	
    });
    
    List<IndexedRow> list = i.collect();
    org.apache.spark.mllib.linalg.Vector v = list.get(0).vector();
    double[] vec = v.toArray();
    System.out.println("Vector");
    for(int j=0; j < vec.length; j++){
    	System.out.println(vec[j]);
    }
    IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(indexedRDD.rdd());
    
    
    
    RowMatrix movieMatrix = new RowMatrix(movRDD.rdd());
    
    System.out.println("movieMatrix: " + movieMatrix.numRows() + " rows, " + movieMatrix.numCols() + " cols.");

    Matrix userMatrix = new DenseMatrix(5, 2, new double[] {1, 1, 1, 1, 1, 2, 2, 2, 2, 2});
    System.out.println("serMatrix: " + userMatrix.numRows() + " rows, " + userMatrix.numCols() + " cols.");

    RowMatrix result = movieMatrix.multiply(userMatrix);
    System.out.println("resultMatrix: " + result.numRows() + " rows, " + result.numCols() + " cols.");

    result.rows().saveAsTextFile("res/result");

    sc.stop();
  }
}

