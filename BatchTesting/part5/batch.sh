hdfs dfsadmin -safemode leave;

hadoop fs -mkdir -p projectCS522/input/
hadoop fs -mkdir -p projectCS522/output/

hdfs dfs -rm -r /user/cloudera/projectCS522/input/*;
hdfs dfs -rm -r /user/cloudera/projectCS522/output/*;

hadoop fs -copyFromLocal input/* /user/cloudera/projectCS522/input/;

hadoop jar ../ProjectMapReduce.jar cs522.part5.KnnNeighbour /user/cloudera/projectCS522/input/input.csv /user/cloudera/projectCS522/output /user/cloudera/projectCS522/input/knn_test.txt;

hadoop fs -cat /user/cloudera/projectCS522/output/*;

mkdir -p output;

rm -r output/*;


hadoop fs -get /user/cloudera/projectCS522/output/* output/;