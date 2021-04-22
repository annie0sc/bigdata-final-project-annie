# PySpark - Big Data Final Project

Hello! I am Annie and this is a final project for demonstrating the skills we have learnt in Big Data course. 

## Text Data Input

I have used a text file as input for this project. For this, I fetched the data via URL. The data source is: [Spirits in Bondage by C.S. Lewis](https://www.gutenberg.org/files/2003/2003.txt).

## Tools & Languages Used

* Language: Python Programming Language
* Tools: Spark Processing Engine, PySpark API
* Environment: Databricks Cloud Environment

## Data Processing

1. Ingest Data into DBFS
2. Create an initial RDD using Spark Context
3. Flatmap to word tokens
4. Map to intermediate key, value pairs
5. Filter out stopwords
6. Reduce by Key to get your counts
7. Collect back into native Python for charting

## Code

1. Retrieving data from plain text url website and storing the data into nod-new.txt file.
```
import urllib.request

urllib.request.urlretrieve("https://www.gutenberg.org/files/2003/2003.txt", "/tmp/nod-new.txt")
dbutils.fs.mv("file:/tmp/nod-new.txt","dbfs:/data/nod-new.txt")
```

2. Creating initial RDD using Spark. 
```
nServers=6
nodRDD=sc.textFile("dbfs:/data/nod-new.txt", nServers)
```
3. Flatmapping the data to word tokens.
```
# flatmap() eaxh line to words
wordsRDD = nodRDD.flatMap(lambda line: line.strip().split(" "))
```

4. Cleaning and preprocessing the data. Removing special characters and spaces and punctuations.
```
import re
from pyspark.ml.feature import StopWordsRemover

cleanRDD = wordsRDD.map(lambda w1: re.sub(r'[^A-Za-z]', '', w1))
remover = StopWordsRemover()
stopwords = remover.getStopWords()
newRDD = cleanRDD.filter(lambda word: word not in stopwords)
```
Final RDD:
```
 finalRDD = newRDD.filter(lambda x: x != "")
 ```
 
5. Mapping the values to Intermediate Key-value pairs.
```
# map() to intermediate key-value pairs (word, 1) 

IKVPairsRDD = finalRDD.map(lambda word: (word,1))
```
6. Reducing keys to get word-count and displaying all the values.
```
# reduceByKey() to eord, count (word, sum)
resultsRDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
print("Reduced key values: ", resultsRDD)
# collect() back into Python DS
results =  resultsRDD.collect()
print("Collections: ", results)
```
Displaying only 10 values.
```
# displaying only 10 word count

results10 = resultsRDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)
print(results10)
```
7. Charting the values using pandas, mathplotlib and seaborn for color.
```
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

source = 'Project Gutenberg: Spirits in Bondage, by (AKA Clive Hamilton) C. S. Lewis'

title = 'Top Words in ' + source
xlabel = 'Words'
ylabel = 'Count'

df = pd.DataFrame.from_records(results10, columns =[xlabel, ylabel]) 
plt.figure(figsize=(20,4))
sns.barplot(xlabel, ylabel, data=df, palette="YlOrBr").set_title(title)
```

## Chart for 10 values
![]()

# References

1. [Dr. Case's Spark Project](https://github.com/denisecase/starting-spark)
1. [Vishal's Final Project](https://github.com/Vishalreddy114/bigdata_finalproject)
2. [Seaborn Color Palettes](https://seaborn.pydata.org/tutorial/color_palettes.html)
3. [Project Gutenberg's Spirits in Bondage, by (AKA Clive Hamilton) C. S. Lewis](https://www.gutenberg.org/files/2003/2003.txt)
4. [Project Gutenberg](https://www.gutenberg.org/) - Plain text books.
