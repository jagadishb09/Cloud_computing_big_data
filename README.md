# Cloud_computing_big_data
Cloud computing and big data projects


Map Reduce : Implementation of a Mapper/Reducer using Hadoop.


Map_Reduce_HBase	: Stored the results from Map Reduce into “HBase”, then implemented  a simple query wizard on a webpage. For example, if I query “sherlock”, 
program shows all words appearing in the same line with “sherlock” as well as the number of appearances. If I query “Sherlock” and “holmes”, program shows how many 
times both words appear in the same line.

Contains following:
1) code to support store data into HBase and query it
2) a webpage to support query.


Map_Reduce_HBase for Twitter data :

1) Uploaded Twitter data into HBase database. Created an HTable called "twitter" and stored all tweets into
   the HTable. Each Tweet is stored in one row. RowID is the Tweet ID, and value is the Tweet text
2) Wrote a MapReduce program to analyze Twitter data from HBase, and find the top 20 most frequent words in all Tweets. 
   Extended Map class from TableMappe and Reduce class from TableReducer
3) Placed the top 20 most frequent words into a HTable called "result", which has a column family "frequency"



Page Rank :	

1) Implemented PageRank and executed it on a large corpus of data
3) Examined the output from running PageRank on Wikipedia to measure the relative importance of pages in the corpus
