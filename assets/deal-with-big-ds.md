# Processing Out-Of-Memory Datasets: Pandas vs DuckDB vs Spark

In the data engineering world, you will frequently found datasets that exceed your available memory. On this tutorial I will show you some examples of how to deal with this kind of datasets.

To simulate an AWS cloud architecture, I will use Virtual Machines (VMs). To accurately measure how different technologies handle resource constraints when processing large files, I have deliberately limited the VM resources to 2 CPUs and 3GB of RAM.

## Scenario and Dataset Architecture

To closely mimic a real world scenario, our files are hosted on a RUSTFS application, an object storage service similar to  AWS S3. This storage app is accessed via our resource-constrained VM.

For this tutorial, we have two datasets `users.csv` and `transactions_logs.json` tha we need to join and process to calculate the total sales amount by city.

- **users.csv (1.1 GB)**, Contains users metadata, including the city they belong to.
- **transactions_logs.json (4.5 GB)**, Contains the historical transactions made by users, including the transaction amount. Note that this file should have the extension JSONL cause each line contains a JSON object.


## 1. Pandas: Loading the Dataset in Memory (The Naive Approach)

For our first attempt, we use a standard Pandas script on our 3GB RAM machine. The script attempts to load the entirely of both files into RAM to merge them and perform the aggregation.

What the script actually does behind the scene is:

1. Downloads and reads the entire CSV and JSON files into the VM's memory.
2. Keeps both DataFrames in memory simultaneously.
3. Allocates additional memory to create a new DataFrame for the JOIN operation.
4. Creates further intermediate data structures during the GROUP BY aggregation.

Because the VM only has 3GB of memory, the system quickly exhausts the available RAM and begins consuming SWAP space. SWAP memory also has a strict limit; once exhausted, the operating system kill the process (OOM Killer)

The goal of this script is to demonstrate the standard, yet highly inefficient, way of managing big files in Pandas when resouces are limited.

## 2. Pandas: Loading the Dataset in Chunks

In this improved version, we avoid loading the files entirely into memory at once. Instead, we load and process the files in sequential chunks.

The main difference from the previous example is how we handle `transactions_logs.json`. We load it using a specific `chunksize` (e.g. 100000 rows at a time). For each chunk, we perform a partial aggregation and then combine the results at the very end. While this script successfully processes the files without chashing the VM, the tradeoff is execution speed, it takes a considerable amount of time to finish.

## 3. DuckDB: Optimized Analytical Querying

Using DuckDB is fundamentally different from writting Pandas code. Instead of procedural steps, we write a SQL query to get, process and aggregate the data. This makes the code significantly cleaner and reduces execution time. DuckDB is an analytical SQL engine, meaning it can query files directly from disk/storage without loading the entire dataset into RAM.

For this script, the first step is establishing a DuckDB connection. Before running queries, we must configure DuckDB to access our S3-compatible storage by:

1. Installing and loading the `httpfs` extension.
2. Setting the S3 endpoint.
3. Configuring our storage credentials.

To query the files directly, we use DuckDB's `read_json_auto` and `read_csv_auto` functions. By providing the file paths, DuckDB creates virtual tables. Inside a single SQL statement, we can define the JOIN, handle data type CASTing, and execute the GROUP BY aggregation efficiently.

## 4. Spark: Processing on a Single Node

Next, we evaluate Apache Spark running locally on a single node (our VM) using the `pyspark` library.

Frist, we initialize the Spark application to access the DataFrame API. We configure the Spark drive to use a maximun of 2GB of memory. We also explicitly set a custom local directory; this is critical because Spark need sufficient disk space to spill data when it runs out of RAM. To read data from our S3-compatible object store, Spark utilizes the Hadoop S3A filesystem implementation.

We then create a Dataframe from the `users.csv` (inferring headers) and another from `transaction_logs.json`. A crucial step here is casting the `user_id` field to ensure both DataFrames have matching data types, preventing errors during the JOIN operation. After definig the JOIN, GROUP BY, and ORDER BY clauses, we call the `show()` function. Because Spark uses lazy evaluation, no actual data processing occurs until this final action is triggered.


## 5. Spark: Distributed Processing on a Cluster

Finally, we scale out PySpark script acroos a distributed cluster consisting of one Master node and three Worker nodes (four VMs in total).

To execute the code in this architecture, we must specify the IP address of the Master node. The Master node does not process the data otherwise it is in charge of:
* Keeps track of available Worker nodes.
* Accepts incoming Spark applications.
* Allocates cluster resources
* Monitors Worker health and progress.

When the job starts, Spark creates a physical execution plan and partitions the  `users.csv` and `transactions_logs.json` files. These partitions are distributed across the three Worker nodes, allowing them to process chunks of the data in parallel, reducing the total execution time compared to the single-node setup.


## Results

| Technology | Time (s) |
| ------------ | --------- |
| Pandas Load in Memory | OOM |
| Pandas Chunk | 39091.00 |
| DuckDB | 87.28 |
| Spark Single Node | 282.94 |
| Spark Workers | 185.80 |

## Conclusions

After all executions I can conclude that:

While Pandas is the industry standard for in-memory data manipulation, it fails completely when datasets exceed available RAM. Attempting to bypass this using the chunksize parameter successfully prevented an OOM crash, but the script took nearly 11 hours (39,091 seconds).

At just 87.11 seconds, DuckDB processed the data over 500 times faster than chunked Pandas and significantly faster than Spark. Because it is a purpose-built, DuckDB effortlessly handles datasets larger than RAM by efficiently spilling to disk.

Running Spark on a single node took 282 seconds—slower than DuckDB due to the overhead of the Java Virtual Machine (JVM) and Spark's internal architecture. Moving to a cluster of workers improved the time to 185 seconds. However, it still didn't beat DuckDB. 

The Final Verdict:

Use Pandas only when you are absolutely certain your dataset will comfortably fit in your machine's RAM (ideally using less than 50% of it to leave room for intermediate operations).

Use DuckDB for gigabyte-scale data when you are working on a single machine (VM or laptop) and the data exceeds your RAM. It is currently the most efficient way to run analytical queries on local hardware.

Use Spark when your data hits the terabyte scale and exceeds the disk storage or CPU capabilities of a single machine. Spark's 149-second execution time on a cluster might look "slow" here compared to DuckDB, but unlike DuckDB, Spark can scale horizontally to thousands of nodes to process petabytes of data.