
Custom partitioner.
Use the Web log and count the Requests per IP. Classify for each month
e.g. : 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245




```

hadoop fs -mkdir /tmp/hadoop_jobs/accesslogs
hadoop fs -put webserver-accesslogs.txt /tmp/hadoop_jobs/accesslogs
hadoop fs -ls /tmp/hadoop_jobs/accesslogs

hadoop jar ippartitioner.jar com.mapreduce.LogDriver /tmp/hadoop_jobs/accesslogs/webserver-accesslogs.txt  /tmp/hadoop_jobs/accesslogs/output
or
hadoop jar ippartitioner.jar com.mapreduce.LogDriver -D mapreduce.job.reduces=12 /tmp/hadoop_jobs/accesslogs/webserver-accesslogs.txt  /tmp/hadoop_jobs/accesslogs/output_1




20/06/27 12:52:38 INFO client.RMProxy: Connecting to ResourceManager at quickstart.cloudera/10.0.2.15:8032
20/06/27 12:52:38 INFO input.FileInputFormat: Total input paths to process : 1
20/06/27 12:52:39 INFO mapreduce.JobSubmitter: number of splits:1
20/06/27 12:52:39 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1593276742642_0001
20/06/27 12:52:40 INFO impl.YarnClientImpl: Submitted application application_1593276742642_0001
20/06/27 12:52:40 INFO mapreduce.Job: The url to track the job: http://quickstart.cloudera:8088/proxy/application_1593276742642_0001/
20/06/27 12:52:40 INFO mapreduce.Job: Running job: job_1593276742642_0001
20/06/27 12:52:49 INFO mapreduce.Job: Job job_1593276742642_0001 running in uber mode : false
20/06/27 12:52:49 INFO mapreduce.Job:  map 0% reduce 0%
20/06/27 12:52:55 INFO mapreduce.Job:  map 100% reduce 0%
.
.
.
.
20/06/27 12:55:07 INFO mapreduce.Job:  map 100% reduce 100%
20/06/27 12:55:07 INFO mapreduce.Job: Job job_1593276742642_0002 completed successfully
20/06/27 12:55:07 INFO mapreduce.Job: Counters: 49

.
.
.
.
.
.
.
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=258762
        File Output Format Counters
                Bytes Written=6034


hadoop fs -ls /tmp/hadoop_jobs/accesslogs/output
Found 13 items
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/_SUCCESS
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00000
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00001
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00002
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00003
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00004
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00005
-rw-r--r--   1 cloudera supergroup       3610 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00006
-rw-r--r--   1 cloudera supergroup       2424 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00007
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00008
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00009
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00010
-rw-r--r--   1 cloudera supergroup          0 2020-06-27 12:53 /tmp/hadoop_jobs/accesslogs/output/part-r-00011



hadoop fs -cat  /tmp/hadoop_jobs/accesslogs/output/part-r-00006
129.188.154.200 41
129.193.116.41  5
129.59.205.2    7
129.94.144.152  13
131.128.2.155   1
133.127.203.203 1
134.20.175.12   10
156.151.176.30  4
156.151.176.45  6
```
