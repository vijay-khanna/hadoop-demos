//  File 1 = transactions.csv. 	: User_ID, Product_ID
//	File 2 = products.csv. 	  	: Product_ID, Product_Name
//  List the user ID, and product name which they purchased using Join,


hadoop jar simplejoin.jar simpleJoin.MrjoinDriver /user/root/mapreduce/simplejoin/transactions.csv /user/root/mapreduce/simplejoin/products.csv /user/root/mapreduce/simplejoin/output


The join operation is used to combine two or more database tables based on foreign keys. In other words, join means to combine rows from two or more tables, based on a related column between them.


There are two types of join operations in MapReduce:

1.MapReduce Join or Reduce Side join: In the mapreduce join, the reducer is responsible for performing the join operation.  The sorting and shuffling phase sends the values having identical keys to the same reducer and therefore, by default, the data is organized for us.

2.Map Side Join: As the name implies, the join operation is performed in the map phase itself. We will see map side join in next segment.

## Approach
for every record in the tables, key = join-attribute, value = remaining attributes in table.

FirstMapper = transactions.csv , need to write the Index 1,
SecondMapper = products.csv, need to write the  Index 0



hadoop fs -mkdir -p /user/root/mapreduce/join
hadoop fs -put product*.csv /user/root/mapreduce/join/
hadoop fs -put transaction*.csv /user/root/mapreduce/join/

hadoop jar join.jar join.MrjoinDriver /user/root/mapreduce/join/transactions.csv /user/root/mapreduce/join/products.csv /user/root/mapreduce/join/output_1

hadoop fs -cat /user/root/mapreduce/join/output_1/*



```
#products.csv

100,"mobile"
101,"tv"
102,"pencil"
103,"laptop"
104,"camera"
105,"shoes"
106,"shirt"
107,"pen"
```

```
#transactions.csv
1,100
2,101
5,103
3,104
1,105
2,106
1,107
```

```
output

1       100     "mobile"
2       101     "tv"
5       103     "laptop"
3       104     "camera"
1       105     "shoes"
2       106     "shirt"
1       107     "pen"


```
