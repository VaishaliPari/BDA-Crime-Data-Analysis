# BDA Crime Data Analysis


# Requirements
* Hadoop setup in Dumbo Cluster
* Spark setup in Dumbo Cluster

# Installation Instructions
* Log into the main HPC node. To do this,
  - On MacOS, open the terminal and type `ssh your_netid@hpc.nyu.edu`
  - On Windows, open PuTTY.exe. In the “Host Name” field, type `your_netid@hpc.nyu.edu`, and then click “Open” at the bottom.
* Enter your password when prompted.
* From the HPC node, log into the Hadoop cluster. To do this, type ssh dumbo. Enter password again (if prompted).
* Upload the file using `scp` from local system to dumbo
* Download Crime Dataset from ![here](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)
* If you	didn’t	before,	put	the	data	file	on	HDFS: `hadoop fs -copyFromLocal NYPD_Complaint_Data_Historic.csv`
* Run	the	Data Cleaner Python	program	using	Spark: `spark-submit cleandata_script.py NYPD_Complaint_Data_Historic.csv`
* Output can be found in cleandata.csv, get in dumbo using: `hadoop fs -getmerge cleandata.csv cleandata.csv`
