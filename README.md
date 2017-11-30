# BAMSI (BAM Search Infrastructure) #

BAMSI is a cloud-based framework for filtering large genomic datasets consisting of BAM/SAM files.
It supports using compute resources in multiple clouds, and the possibility to access the data from multiple mirrors.

BAMSI streams the data, filters it according to a custom criteria, and saves the resulting subset of the data to storage.
This removes the need to store the entire data set on local storage; a small subset of interest can be extracted and further analyzed locally.

BAMSI is currently implemented for filtering the set of BAM files from the 1000 Genomes Project phase 3 dataset (http://www.1000genomes.org),
but could be modified to work with any set of BAM/SAM files.

Follow the setup instructions here to set up BAMSI on your own set of compute resources.

A freely available deployment of BAMSI is also available at (http://bamsi.research.it.uu.se/), hosted by SNIC Science Cloud.


### Requirements ###

The following setup is a prerequisite to launch a deployment of BAMSI:

1. Compute resources: at least one virtual machine.
2. Data source: One or more mirrors hosting the data from where the compute resources can access it. The data may be accessed via HTTP, FTP or as a mounted volume on the compute resource.
3. Storage repository: a resource where the filtered-out data may be stored. The current implementation uses HDFS (Hadoop Distributed File System),
   but the code can be modified to use any storage repository that supports a REST interface.

Let

MASTER_NODE and MASTER_IP denote the hostname and IP-address of the resource hosting the BAMSI server

STORAGE_NODE and STORAGE_IP denote the hostname and IP-address of the resource hosting the interface of the storage repository (e.g. the Namenode in HDFS-case).

WORKER_NODES denote the hostnames of the machines running filtering workers

It is assumed the resources are configured so that the WORKER_NODES can can resolve the hostname of MASTER_NODE and STORAGE_NODE and reach them via a network.

BAMSI should work on any cloud provider supporting Ubuntu VMs. You will need Python, and the following packages:

* java
* git
* pip
* rabbitmq-server(only on MASTER_NODE)
* SAMtools



```
sudo apt-get update
sudo apt-get install default-jre
sudo apt-get install git
sudo apt-get install python-pip
sudo apt-get install python-dev
sudo apt-get install zlib1g-dev
sudo apt-get install samtools
```




### Setup and configuration ####

1. Clone the BAMSI git repository
     ```
    $ git clone ...
    ```
2. Change the directory, and install python packages with pip
    ```
    $ cd BAMSI
    $ sudo apt-get install python-pip
    $ sudo pip install -r requirements.txt
    ```
3. Start the Celery broker - RabbitMQ (Only on MASTER_NODE)

    https://www.rabbitmq.com/install-debian.html
    

4. On the MASTER_NODE and all the WORKER_NODES: modify the configuration file dfaas.cfg according to your setup:

 Example configuration file:

```
[dfaas]
CELERY_BROKER_URL = amqp://celery:stalk@MASTER_NODE/celery-host
CELERY_RESULT_BACKEND = amqp
DATA_PATH=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/{individual}/alignment/{filename}
MASTER_IP=MASTER_NODE
MASTER_PORT=8888
WEBHDFS_IP=STORAGE_NODE
WEBHDFS_PORT=50070
WEBHDFS_PUBLIC_IP=STORAGE_NODE
WEBHDFS_PUBLIC_PORT=14000
WEBHDFS_USER=ubuntu
RESULTS_PATH=/filtered/
```

The first five settings are mandatory. They define the location and protocol of the celery host, and define the location from which
 each WORKER_NODE streams the data. To use the public Amazon S3 mirror, use:

DATA_PATH=http://<span></span>s3.amazonaws.com/1000genomes/phase3/data/{individual}/alignment/{filename}





The other settings relate to the storage repository and can be changed accordingly when adding support for other storage.

For the current HDFS implementation it is assumed that the namenode is listening on STORAGE_NODE:50070 and
that a HttpFS server is listening on STORAGE_NODE:14000. These may have to be modified depending on your setup of HDFS.
(E.g. if the WORKER_NODE is on the same network as the HDFS datanodes, then they can push results to the storage via the namenode.
Otherwise, they should interact with HDFS via the HttpFS server, and WEBHDFS_PORT should also be 14000.)


### Starting the application ####

1. Start the BAMSI server on MASTER_NODE.

    ```
    $ nohup python tapp.py > server.out 2 >&1&
    ```

2. On the WORKER_NODES (could include MASTER_NODE as well), start the celery workers with the settings of your choice, e.g.

    ```
    $ nohup celery -A tapp.celery worker -Q default --loglevel=INFO --prefetch-multiplier=1 --concurrency=5 -n worker1.%h > celery.out 2>&1&
    ```

3. On the MASTER_NODE, verify that the app is running by checking the below URL.

    http://localhost:8888/

4. Setup the database used for monitoring jobs.

    http://localhost:8888/populate

    Uses SQLite database to house the metadata, and job statistics.


5. Define and launch a filter job, either via the launch page of the web interface.

6. Monitor the progress of the query via the dashboard page of the web interface.

There is also a Python API that is possible to use for interaction with the BAMSI server instead of the web interface.


