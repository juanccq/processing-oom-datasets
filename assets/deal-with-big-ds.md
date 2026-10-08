# Processing Out-Of-Memory Datasets: Pandas vs DuckDB vs Spark

On the data engineering world sometimes you will deal with datasets that exceed the size of the memory available. For the current example we have two datasets `users.csv` and `transactions.json` which need to be processed in order to get the total sales amount by city.

## Dataset description

- **users.csv**, this file contains data from users including the city that they belong. The filesize is 1.1 GB
- **transactions.json**, this file contains the transactions made by users including the amount.

To be closed to a real world scenario these files are loaded in RUSTFS app which has similarities to the Amazon S3 service. This app is running on a Virtual Machine (VM).

## Pandas loading the dataset in memory

For this case we have a VM with 2 CPUs and 3GB of RAM and pandas installed. The script will load the 

## Pandas loading the dataset in chunks

## DuckDB

## Spark with a single node

## Spark with a cluster of nodes

## Results

## Conclusions