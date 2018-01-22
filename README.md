# BAMSI - The BAM Search Infrastructure #

BAMSI is a cloud-based framework for filtering large genomic datasets consisting of BAM/SAM files.
It supports using compute resources in multiple clouds, and the possibility to access the data from multiple mirrors.

BAMSI streams the data, filters it according to a custom criteria, and saves the resulting subset of the data to storage.
This removes the need to store the entire data set on local storage; a small subset of interest can be extracted and further analyzed locally.

BAMSI is currently implemented for filtering the set of BAM files from the 1000 Genomes Project phase 3 low coverage dataset (http://www.1000genomes.org),
but could be modified to work with any set of BAM/SAM files.

Follow the instructions here to launch BAMSI on your own set of compute resources.

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

BAMSI should work on any cloud provider supporting Ubuntu VMs.

### Setup and configuration ####

Examples will be shown for Ubuntu using apt.

1. Initial requirements:

* python 2
* java
* git
* pip

     ```
    $ sudo apt-get update
    $ sudo apt-get install default-jre
    $ sudo apt-get install git
    $ sudo apt-get install python-pip
    $ sudo apt-get install python-dev
    $ sudo apt-get install zlib1g-dev
    ```


2. Clone the BAMSI git repository

     ```
    $ git clone https://github.com/NGDSG/BAMSI.git
    ```

3. Change the directory, and install python packages with pip

    ```
    $ cd BAMSI
    $ sudo pip install -r requirements.txt
    ```


4. On all WORKER_NODES: install SAMtools version 1.x

    See http://www.htslib.org/download/ for download instructions.


5. Only on MASTER_NODE: install and start the Celery broker - RabbitMQ

   (See https://www.rabbitmq.com/download.html for instructions)

     ```
    $ sudo apt-get install rabbitmq-server
     ```


    Create virtual host and user: (replace *celery-host*, *celery*, and *stalk* with your own virtual hostname, username and password).


     ```
    $ sudo rabbitmqctl add_vhost celery-host
    $ sudo rabbitmqctl add_user celery stalk
    $ sudo rabbitmqctl set_permissions -p celery-host celery ".*" ".*" ".*"
    ```



6. On the MASTER_NODE and all the WORKER_NODES: modify the configuration file config.cfg according to your setup:

    Example configuration file:

    ```
    [bamsi]
    CELERY_BROKER_URL = amqp://celery:stalk@MASTER_NODE/celery-host
    CELERY_RESULT_BACKEND = amqp
    DATA_PATH=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/{individual}/alignment/{filename}
    MASTER_IP=MASTER_NODE
    MASTER_PORT=8888

    [storage]
    WEBHDFS_IP=STORAGE_NODE
    WEBHDFS_PORT=50070
    WEBHDFS_PUBLIC_IP=STORAGE_NODE
    WEBHDFS_PUBLIC_PORT=14000
    WEBHDFS_USER=ubuntu
    RESULTS_PATH=/filtered/
    ```

    The settings in the section 'bamsi' are mandatory. They define the location and protocol of the celery host, and define the location from which
     each WORKER_NODE streams the data. To use the public Amazon S3 mirror, use:

    ```
    DATA_PATH=http://s3.amazonaws.com/1000genomes/phase3/data/{individual}/alignment/{filename}
    ```



    The settings in the section 'storage' relate to the storage repository and can be changed accordingly when adding support for other systems.
    Adding a different type of storage repository requires writing a class that implements the interface defined by the class StorageRepositoryBase in tapp.py.
    If no other functionality is required, creating such a class and changing the line

        StorageRepository = HDFS()

    in tapp.py to initialize your new class instead, and modifying the config file accordingly, should suffice.



    For the current HDFS implementation it is assumed that the namenode is listening on STORAGE_NODE:50070 and
    that a HttpFS server is listening on STORAGE_NODE:14000. These may have to be modified depending on your setup of HDFS.
    (E.g. if the WORKER_NODE is on the same network as the HDFS datanodes, then they can push results to the storage via the namenode on port 50070.
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

    Uses a SQLite database to house the metadata and job statistics.

5. Define, launch and monitor the progress of filter jobs via the launch and dashboard pages of the web interface.

6. Depending on your setup and preferences: download the results or access them directly from the storage repository for further analysis.

There is also a Python API (https://github.com/NGDSG/BAMSI-API) that is possible to use for interaction with the BAMSI server instead of the web interface.



contact: kristiina.ausmees@it.uu.se
