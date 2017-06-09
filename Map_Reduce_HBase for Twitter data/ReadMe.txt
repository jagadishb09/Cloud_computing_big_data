Hadoop version - 2.2
HBASE version  -0.98
java 8

Files-
MainProgram.java
Map.java
Reducer1.java

Arguments for the code to run are 

1st argument-dummy input text file.
2nd argument-directory where map reduce program output has to be stored.
3rd argument-'part-00000' file which is output file of map reduce program.

I have created a table 'twitter' with columnfamily 'text' and another table 'result' with columnfamily 'frequency'.

Code:-

In 'MainProgram.java' I have written code to read the data from input csv files and inserted into table 'twitter'
in 'text' column of 'text' column family.
Then I have written code to Map reduce. 
In Map.java, I am reading each row from table 'Twitter' and doing Map reduce which gives 
the output file 'part-00000' which is present in 'output.txt'
While Mapping in 'Map.java' I have written code to exclude words which are present in 'stop-word-list.csv'.
Then Reducer1.java is used to implement Reduce function.

Then in 'MainProgram.java', I am using 'part-00000' file to evaluate top 20 most frequent words.
