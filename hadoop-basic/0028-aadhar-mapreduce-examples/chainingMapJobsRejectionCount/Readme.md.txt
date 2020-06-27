First Map-Reduce will dump output to a temp folder. that temp folder will be read by second Map-Reduce pair. 

javac -classpath 'hbase classpath' chainingMapJobsRejectionCount.java

jar -cvf chainingMapJobsRejectionCount.jar *.class

hadoop jar chainingMapJobsRejectionCount.jar chainingMapJobsRejectionCount
