# relation_join
Join of two relations R(a,b) and S(a,c) with 'a' as a connection field

Different ways to implement the join of two relations with Scala:
1-RDD
2-Dataset
3-Dataframe
4-RDD with manual join
5-RDD with sorted tuples and manual join

Results: Calculating the time, the conclusion about the first 3 ways is that the fastest ways are the Dataset and Dataframe. RDD is quite slow since it is an older data structure of Spark. The 4th way though, is faster than the previous ones and the 5th even faster, since the tuples are sorted as well.
