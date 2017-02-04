# hbaseApp: Storing tweets into HBase providing a set of queries for data analysis.

The goal of this project is to develop Java application that stores trending topics from Twitter into HBase and provides users with a set of queries for data analysis.	The application is able to run in a distributed environment.

The trending topics to load in HBase are stored	into text files	with the following format:

* 1 file per language.
* Each line of the file in CSV format ````“timestamp_ms, lang, tophashtag1, frequencyhashtag1, tophashtag2, frequencyhashtag2, tophashtag3, frequencyhashtag3”````.

The output of the application is a set of files, one for each query, with the most used hashtags (top-N), depending on the filters of the query.	

Installation
----------- 
Dependencies:

* Oracle Java 7
* Hadoop 2.5.0
* HBase 0.98.6


Instructions on how to execute
----------- 
The application has been configured and compiled with the ````appassembler```` maven plugin. According to the ````mode`````parameter, the script will be used with the following parameters:

````
$ mvn clean install
$ target/appassembler/bin/hbaseApp.sh mode zkHost dataFolder	# Load
$ target/appassembler/bin/hbaseApp.sh mode zkHost startTS endTS N language outputFolder	# Query1
$ target/appassembler/bin/hbaseApp.sh mode zkHost startTS endTS N language outputFolder	# Query2
$ target/appassembler/bin/hbaseApp.sh mode zkHost startTS endTS N outputFolder	# Query3
````

where:

* ````mode````: 1 means read first query, 2 read second query, 3 read third query and 4 create table and load data files. 
* ````zkHost````: string with the format IP:PORT
* ````starTS````: timestamp in milliseconds to be used as start timestamp.
* ````endTS````: timestamp in milliseconds to be used as end timestamp.
* ````N````: size of the ranking for the top-N
* ````language````: a csv list of languages.
* ````dataFolder````: path to the folder containing the files with the trending topics.
* ````outputFolder````: path to the folder where the files with the query results are stored.

Description of the queries
----------- 

1. Given a language, find the Top-N most used words for the given language in a time interval defined with a start and end timestamp. 
2. Find the list of Top-N most used words for each language in a time interval defined with a start and end timestamp. 
3. Find the Top-N most used words and the frequency of each word regardless the language in a time interval defined with a start and end timestamp. 
