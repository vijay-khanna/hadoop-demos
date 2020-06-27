
  Create custom object. it will be used as key or value in MR program.
  need to implement particular interfaces.
  If it needs to be used as a key, then it needs to definitely implement WritableComparable Interface.
  If we are simply using it as value, then we need not implement WritableComparable, but only implement the Writable interface.
  This for intermediate shuffle and sort. For comparing keys in intermediate key-value pairs.
  It will group them by keys, and send them to relevant reducer.


  Need to tell the framework how to compare the custom objects.


  ##Problem Statement here :   create word co-occurence matrix.  
  this is used in Natural Language processing domain. E.g. When buying a phone, check for screen guard to co-sell

 e.g. // this is a simple program to understand custom object
    Q: count frequency of each pair in the document
   either Make use of immediate pairs, e.g. This is, is a, a sample,
   or set number of neighbours : i.e. This is, This a, This sample.... //the neighbours can be set in program.

   in Mapper phase :  for each pair, emit a constant = 1.
   in Reducer phase : for each pairs, aggregate the values to get frequency.

   ```
hadoop fs -put input.txt /user/root/mapreduce/wordcooccurence_input.txt

hadoop jar wordco2.jar wordCoOccurence.WordcoDriver -D neighbors=3 /user/root/mapreduce/wordcooccurence_input.txt /user/root/mapreduce/output_mapco


for text file content :"this is to compare this is to also see if custom object is working"
output =
[cloudera@quickstart jars]$ hadoop fs -cat /user/root/mapreduce/output_mapco/*
also custom     1
also if 1
also is 1
also see        1
also this       1
also to 1
compare is      2
compare this    2
compare to      2
custom also     1
custom if       1
custom is       1
custom object   1
custom see      1
custom working  1
if also 1
if custom       1
if is   1
if object       1
if see  1
if to   1
is also 1
is compare      2
is custom       1
is if   1
is object       1
is see  1
is this 3
is to   3
is working      1
object custom   1
object if       1
object is       1
object see      1
object working  1
see also        1
see custom      1
see if  1
see is  1
see object      1
see to  1
this also       1
this compare    2
this is 3
this to 3
to also 1
to compare      2
to if   1
to is   3
to see  1
to this 3
working custom  1
working is      1
working object  1



   ```
