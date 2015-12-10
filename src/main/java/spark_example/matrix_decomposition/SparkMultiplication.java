package spark_example.matrix_decomposition;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.typesafe.config.Config;

import scala.Tuple2;
import spark.jobserver.JavaSparkJob;

public class SparkMultiplication extends JavaSparkJob{
	private static JavaRDD<IndexedRow> usersRDD;
	private static JavaRDD<IndexedRow> moviesRDD;
	
	@Override
	public Object runJob(JavaSparkContext jsc, Config jobConfig) {
		// TODO Auto-generated method stub
		System.out.println("JavaSparkContext");
		System.out.println(jsc);
		Long userid = Long.parseLong(jobConfig.getString("user"));
		String outputFolder = jobConfig.getString("output");
		int n = Integer.parseInt(jobConfig.getString("n"));
		
		return getReco(jsc, userid, outputFolder, n);
	}
	public static Object getReco(JavaSparkContext sc, Long uid , String outputFolder, int n) {


        if(outputFolder == null){
        	outputFolder = "/res/output";
        }
	    

	    PriorityQueue<Tuple2<Long, Double>> pq = new PriorityQueue<Tuple2<Long, Double>>(n, new Comparator<Tuple2<Long, Double>>() {

			@Override
			public int compare(Tuple2<Long, Double> o1, Tuple2<Long, Double> o2) {
				return o1._2.compareTo(o2._2);
			}
		});
	    
	    long startUserTime = System.currentTimeMillis();
	    JavaRDD<String> usersFile = sc.textFile("/res/users.txt");
	    
	    if(usersRDD == null){
	    	usersRDD = usersFile.map(new Function<String, IndexedRow>(){

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
		    usersRDD.count();
	    }
	    
	    
	    long endUserTime = System.currentTimeMillis();
	    
	    System.out.println("Time for user matrix: "  + (endUserTime - startUserTime) + "ms");
	    
	    long startMovieTime = System.currentTimeMillis();
	    JavaRDD<String> moviesFile = sc.textFile("/res/items.txt");
	    
	    if(moviesRDD == null){
	    	moviesRDD = moviesFile.map(new Function<String, IndexedRow>(){

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
		    moviesRDD.count();
	    }
	    long endMovieTime = System.currentTimeMillis();
	    System.out.println("Time for movie matrix: "  + (endMovieTime - startMovieTime) + "ms");
        final long userId = uid;
        
        long startRecommendTime = System.currentTimeMillis();
	    JavaRDD<IndexedRow> a = usersRDD.filter(new Function<IndexedRow, Boolean>() {

			@Override
			public Boolean call(IndexedRow v1) throws Exception {
				
				return v1.index() == userId;
			}
		});
	    
	    
	    final double[] user = a.first().vector().toArray();
//	    printDoubleArr("user", user);
	    
	    JavaRDD<Tuple2<Long, Double>> result = moviesRDD.map(new Function<IndexedRow, Tuple2<Long, Double>>() {

			@Override
			public Tuple2<Long, Double> call(IndexedRow v1) throws Exception {
				double[] movie = v1.vector().toArray();
				
				long key = v1.index();
//				printDoubleArr("movie"+key , movie);
				Double val = VectorMath.dot(user, movie);
				return new Tuple2<Long, Double>(key, val);
			}
		});
	    
	    
	    
	    List<Tuple2<Long, Double>> ret = result.collect();
	    long endRecommendTime = System.currentTimeMillis();
	    System.out.println("Time for Recommend matrix: "  + (endRecommendTime - startRecommendTime) + "ms");
	    
	    
	    long startPQTime = System.currentTimeMillis();
	    
	    // ----- Read exisiting Data from local file system ----- / 
	    List<Long> currUserList = new ArrayList<Long>();
	    try {
			BufferedReader in = new BufferedReader(new FileReader(new File("/root/data.csv")));
			String line;
			while((line = in.readLine()) != null){
				String[] arr = line.split(",");
				Long currUserId = Long.parseLong(arr[0]);
				Long currMovieId = Long.parseLong(arr[1]);
				if(currUserId == userId){
					currUserList.add(currMovieId);
				}
			}
			in.close();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    // -------------- &&&&& -------------------------------//
	    
        for(Tuple2<Long, Double> each: ret){
        	if(currUserList.contains(each._1))
        		continue;
        	if(pq.size() < n){
        		pq.offer(each);
        	}else{
        		Tuple2<Long, Double> curr = pq.peek();
        		if(curr._2 < each._2){
        			pq.poll();
        			pq.offer(each);
        		}
        	}
        }
        List<Tuple2<Long, Double>> newResult = new ArrayList<Tuple2<Long, Double>>();
        JSONArray out = new JSONArray();
        int size = n;
        int count = 0;
        while(pq.size() > 0){
        	Tuple2<Long, Double> curr = pq.poll();
        	JSONObject obj = new JSONObject();
        	try {
				obj.put("ID", curr._1);
				obj.put("score", curr._2);
				out.put(size - count - 1, obj);
				count++;
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        	newResult.add(curr);
        }
        
        long endPQTime = System.currentTimeMillis();
        System.out.println("Time for Recommend matrix: "  + (endPQTime - startPQTime) + "ms");
        JavaRDD<Tuple2<Long, Double>> resultRDD = sc.parallelize(newResult);
        
        resultRDD.saveAsTextFile(outputFolder);
       
	    
	    return out.toString();
	}
	
	public static void printDoubleArr(String name, double[] arr){
		System.out.println(name);
	    for(int i=0; i < arr.length; i++){
	    	System.out.print(arr[i] + " ");
	    }
	    System.out.println();
	}
}
