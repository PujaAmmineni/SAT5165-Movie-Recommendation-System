
# Movie Recommendation System using PySpark

## Project Overview

This project is focused on building a movie recommendation system using PySpark to process the `movies_metadata.csv` dataset in a distributed environment. The environment consists of 10 virtual machines (VMs), where one VM serves as the master node and the other 9 as slave nodes. Spark and Hadoop have been configured across these VMs to enable distributed data processing.

## Project Setup

### Software and Environment

- **Operating System**: Fedora 37
- **Hadoop Version**: 3.3.6
- **Spark Version**: 3.5.0
- **Java Version**: 21.0.1
- **Python Version**: 3.11.0
- **Cluster Configuration**: 
  - Master Node: hadoop1 (192.168.13.143)
  - Slave Nodes: hadoop2 to hadoop10 (192.168.13.144 - 192.168.13.171)
  
### Directory Structure

The following directory structure is used for the setup:

```
/opt
  ├── hadoop-3.3.6/
  ├── spark-3.5.0/
  ├── java-21.0.1/
  └── python-3.11.0/
```

### Key Configuration

- **Passwordless SSH** is set up between the master node and all slave nodes to enable communication.
- **Hadoop and Spark configuration files** are updated to run Spark in cluster mode across the nodes.

### Dataset

The dataset used is `movies_metadata.csv`, which includes various metadata related to movies such as:
- Revenue
- Runtime
- Vote count
- Vote average
- Other relevant attributes

### Python Script

The `movie_recommend_pyspark.py` script implements the logic for processing the dataset and generating movie recommendations. This script is distributed across the Hadoop-Spark cluster, leveraging the power of PySpark for large-scale data processing.

## How to Run

1. **Copy the Python Script**: Copy the `movie_recommend_pyspark.py` file from the local machine to the master node (`hadoop1`).

2. **Run the PySpark Job**:
   - SSH into the master node:
     ```
     ssh sat3812@192.168.13.143
     ```
   - Submit the PySpark job:
     ```
     spark-submit --master yarn --deploy-mode cluster /opt/movie_recommend_pyspark.py
     ```
   This command will distribute the job across the cluster.

3. **View Results**: Once the job completes, the output will be available in the logs of the master node, and the recommendations will be printed in the terminal.


## Goals and Deliverables

- **Setup a distributed environment** for Hadoop and Spark.
- **Run distributed data processing jobs** using PySpark.
- **Generate movie recommendations** from the movies_metadata.csv dataset.
