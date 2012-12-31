Hadoop Data Filter
==================

This is a small hadoop job for filtering data from a lage data set.

The code uses Maven, and the job can be build using maven package.

I will add commands and the like for running the job later.

There is a vagrant box available which has been set up as a 
psuedo cluster for running the hadoop jobs.

Compilation
===========

	- Install Eclipse
	- Install m2e
	- Get the repo
	- Import the project
	- Convert to a Maven project
	- Run as maven project (type in Package as Goal)
	- copy to your vagrant directory
	- replace the hadoop-filter.jar jar

Usage
=====

First log into the vagrant box

Next become hduser 

	- sudo -s
	- su - hduser

got to the hadoop directory

	- cd /opt/hadoop
	
start hadoop

	- ./hadoop/bin/start-all.sh

run the hadoop job

	- ./hadoop/bin/hadoop jar hadoop-filter.jar FilterData /home/hduser/dataset/ /home/hduser/data-output 
	
A bit about hadoop

Hadoop makes use of HDFS, the hadoop distributed File System. 
This means that hadoop cannot read and write to the local filesystem, but only to 
it's own HDFS.

Not really a problem though as we can interact with the filesystem using hadoop fs or 
hadoop dfs. Basically use standard bash style commands to interact:

	-	./hadoop/bin/hadoop fs -ls /home/hduser
		Will give you an ls for the hdfs://home/hduser directory
	-	./hadoop/bin/hadoop fs -cat /home/hduser/blah.txt
		Will cat the /home/hduser/blah.txt file on the hdfs cluster
		
Obviously you will need to push some actual data for the job to run on:

	-	./hadoop/bin/hadoop dfs -put <local> <hdfs>
	
On occasion, you may want to set a file as part of the distributed cache, in this case, 
you would put the file on the 

