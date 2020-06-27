Partitioner program

make month-wise files containing employee names, employee IDs and birthday without the year and the month.

solve the problem by writing a mapreduce program that uses a custom data type containing employee name and ID

implement your own partitioner to ensure that the employee details of the employees with birthdays in the same month go to the same reducer.



hadoop fs -mkdir /user/root/mapreduce/hr

hadoop fs -put salary_grid_product_master.csv /user/root/mapreduce/hr
hadoop fs -put HR_Dataset_transactions.csv /user/root/mapreduce/hr

hadoop fs -put HR_Dataset_transactio* /user/root/mapreduce/hr




hadoop jar hrpartitioner.jar practiceMR.HRPartitionerDriver /user/root/mapreduce/hr/HR_Dataset_transactions.csv /user/root/mapreduce/hr/output_hrpartition



hadoop jar hrpartitioner.jar practiceMR.HRPartitionerDriver /user/root/mapreduce/hr/HR_Dataset_transactions2.csv /user/root/mapreduce/hr/output_hrpartition4