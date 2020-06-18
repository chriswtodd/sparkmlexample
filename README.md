# sparkmlexample

The program when run will output a file with the accuracy of ten runs of the Sparks DecisionTree on the kdd.data 
dataset.

## Setup and Create Files

Using the hadoop cluster you must create a new directory for your input data to be read from.

The input/output directories used in this readme are: 
 - _user/toddchri1/input/_
    - This must contain your input data.
 - _user/toddchri1/output/_
    - Optional?

## Install and Run

We assume that running on a hadoop cluster.

#### Hadoop Cluster

Setup classpath environment vars:

  - `setenv HADOOP_VERSION 2.8.0`
  - `setenv HADOOP_PREFIX /local/Hadoop/hadoop-$HADOOP_VERSION`
  - `setenv SPARK_HOME /local/scratch/spark-2.3.0-bin-hadoop2.7/spark-2.4.5-bin-hadoop2.7/`
    - Tell environment where to find spark distribution. In my case, I have unpacked the _spark-2.3.0-bin-hadoop2.7.tgz_
     into `local/scratch/spark-2.3.0-bin-hadoop` 
  - `setenv PATH ${PATH}:$HADOOP_PREFIX/bin:$SPARK_HOME/bin`
  - `setenv HADOOP_CONF_DIR $HADOOP_PREFIX/etc/hadoop`
  - `setenv YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop`
  - `setenv LD_LIBRARY_PATH $HADOOP_PREFIX/lib/native:$JAVA_HOME/jre/lib/amd64/server`
  - `need java8`
  
Compile the Java Program:

 - `javac -cp "spark-2.4.5-bin-hadoop2.7/jars/*" -d test COMP424/Ass3/src/Group/DT/DecisionTree.java`
    - Compile with extra classpaths to include all spark jars. I also have a spark jars directory with all the necessary 
    Sparkml jars for the program to be run. `-d` for output directory. Final arg is the input source java file.

Create `.jar` file from compiled classes:
 
 - `jar cvf DecisionTree.jar -C test/ .`
    - `-C` sets the directory for the compiled java classes to be used. `.` says all the classes/files in the directory
     should be used.
     
Run the program:

To run the program on a deployed cluster (has no console output, so no results printable): 
 
 - `spark-2.4.5-bin-hadoop2.7/bin/spark-submit --class "Group.DT.DecisionTree" --master yarn --deploy-mode cluster 
 DecisionTree.jar /user/toddchri1/input/kdd.data /user/toddchri1/output/f1`
 
To run the program locally (has a console output, results printed): 
 
 - `spark-2.4.5-bin-hadoop2.7/bin/spark-submit --class "Group.DT.DecisionTree" --master local DecisionTree.jar 
 /user/toddchri1/input/kdd.data output1.txt`
  - The first command needs to point to the directory with **all** of the spark package inside, when calling 
  `spark-submit`.
  - The class command does not need the directory infront of it, just the classpath following from the top directory 
  specified in the `-d` and `-C` above.
  - Following the DecisionsTree.jar, the first arg specifies the input directory that stores the data. The second gives
  the output file a name. If writing to a file not in the current directory (where your shell is), the directory must 
  exist.
  
