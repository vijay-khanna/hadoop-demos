This program calculates the number of email and mobile numbers in the provided list. if email/mobile is provided it is 1 else 0,
this is a map only program

This is tested for cloudera VM, which uses hadoop 2.6.0. use Maven for dependencies.
## to Get larger data file : https://www.kaggle.com/knightking007/aadhar/data

copy the aadhar4.csv to hadoop machine. use SCP
need to remove the header line from sample data.

from local disk, copy to hadoop fs
hadoop fs -put aadhar5.csv /user/root/mapreduce


hadoop jar countMobiles4.jar countMobileAndEmailMembers.CounterDriver /user/root/mapreduce/aadhar5.csv /user/root/mapreduce/output-aadhar5
