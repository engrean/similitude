# similitude
Find similar sets of things, using LSH. This algorithm is currently in production to block over six billion sets.

This project provides a library that can be used by itself and it also contains a Spark job

#  To build:
./gradlew

This builds all jars including uber jars for the projects that need them.

# To generate a testing and truth data set:
 
Download the top 333k search terms or create a file for the random words you want to use in the following format:
the	23135851162
of	13151942776
and	12997637966
to	12136980858
a	9081174698
in	8469404971
for	5933321709
is	4705743816
on	3750423199
that	3400031103

I used the following list: https://github.com/first20hours/google-10000-english

java -jar lsh/build/libs/lsh-1.0.jar gen-sentences data/30K_top_words.txt 50 100000 5 20 data/sentences.txt

where 30K_top_words.txt is used to seed the sentences with word
50 is the number of top words to use
100,000 is the number of sentences to generate
5 is the minimum number of words in a sentence
20 is the max number of words in a sentence
data/sentences.txt is the file you want to write the sentences to

The data looks like this:
0,my all free but to more you that new if to of was new one or on for
1,are with free have an if you one
2,other be is this an for be we i not you a home on us this page
3,not can was are have it home information of not from be at but was page

where the first column is the sentence key and the second column. I used comma-delimited to make it easier to load this data into Kafka so I can LSH stuff using Kafka Streams

# To generate a truth set
java -jar lsh/build/libs/lsh-1.0.jar gen-truth data/sentences.txt data/truth.txt 0.4

data/sentences.txt is the source to use for the truth set
data/truth.txt is where the truth will be written to
0.4 is the minimum jaccard similarity score the sentence pair must have to make it into the truth set

The data looks like:
0	336	0.4	10/23
0	682	0.4	8/20
0	1255	0.4	8/20
0	2577	0.5	10/20
0	3152	0.4	9/21
where the 1st column is a doc id (smaller ids should always be first)
the 2nd column is the other doc id
the 3rd column is the Jaccard score
the 4th colums is the the number of common set fields/the total number of unique set fields

This command is a brute force compare. It takes a while.

# Testing things out
Install Spark 2.0 either locally or on a cluster. Installing it locally, means you just unzip/untar it in a directory and add SPARK_INSTALL/bin to your PATH.
spark-submit --master local[4] spark/build/libs/spark-1.0-all.jar data/sentences.txt data/truth.txt 40 5 true false

4 is the number of workers you want to concurrently run when in local spark mode
data/sentences.txt is the file that contains the sentences you want to generate LSH keys for
data/truth.txt is the truth file
40 is the number of hash functions to use for Minhash
5 is the number of rows (minhash values) you want in per hash key
true/false is if you want to use a new shifting key feature idea I came up with. If you know what an ngram is, then think of ngraming the minhash key
true/false is if you want to compress the LSH key down to a 23 character string. The more rows per band, the more savings this gives you. However, if you want smaller bands then this might actually increase the size of your key

The output looks like:
0.2     0.06    225107/4073981
0.3	0.15	195752/1347864
0.4	0.36	73983/203744
0.5	0.68	11228/16403
0.6	0.91	1282/1404
0.7	0.99	154/155
0.8	1.0	25/25
0.9	1.0	2/2
Total Precision: 0.6371728189077999
                 507533/507533 + 289006
Total Recall:    0.08993106855260971
                 507533/5643578
                 
1st column is the jaccard score to the nearest tenth
2nd column is the recall score or the number of pairs found from being in the same LSH blocks over the total number of truth pairs
3rd column is simply the hard numbers for the recall score

The Total Precision is defined as the total pairs found from LSH blocks over the sum of the pairs found and the pairs in the blocks that ended in no truth.
The Total Recall: is the total pairs found from LSH over the total truth pairs

