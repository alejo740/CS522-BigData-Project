hdfs dfsadmin -safemode leave;

hadoop fs -mkdir -p wordcount/input/
hadoop fs -mkdir -p wordcount/output/

hdfs dfs -rm -r /user/cloudera/wordcount/input/*;
hdfs dfs -rm -r /user/cloudera/wordcount/output/*;

hadoop fs -copyFromLocal input/* /user/cloudera/wordcount/input/;
hadoop jar ../ProjectMapReduce.jar cs522.part1.pointC.WordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output;

hadoop fs -cat /user/cloudera/wordcount/output/*;

mkdir -p output;

rm -r output/*;


hadoop fs -get /user/cloudera/wordcount/output/* output/;