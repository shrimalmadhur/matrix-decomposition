# matrix-decomposition
Matrix Decomposition

In Memory Distributed Matrix Multiplication using Spark.

Implementation is to be added in [Cloudera Oryx](http://oryx.io) Serving Layer


	Clone the repository 
	git clone https://github.com/shrimalmadhur/matrix-decomposition.git
	cd matrix-decomposition
	
Build the jar using

	mvn clean package
	
The jar will be located inside target folder

Setup a Spark Cluster using spark-ec2 API [here](http://spark.apache.org/docs/latest/ec2-scripts.html)

Transfer the file to the master cluster using scp to root folder.
<br>
Run the spark job using the below command

	spark/bin/spark-submit --class spark_example.matrix_decomposition.SparkMultiplication --master yarn-client matrix-decomposition-0.0.1-SNAPSHOT.jar -user 4 -n 25 -output "/res/output3/"
	
	Parameters
	user - user ID
	n - number of recommendation
	output - output folder
