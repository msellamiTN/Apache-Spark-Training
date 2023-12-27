
# SparkMapRProject

## Project Description

**SparkMapRProject** is a Scala-based Apache Spark application designed for ETL (Extract, Transform, Load) operations. The application integrates with Hive and MapR-DB, reading data from Hive tables, transforming it, and then storing the results in MapR-DB JSON tables. It is ideal for environments using MapR data platforms along with Apache Hive.

### Key Features

- **Hive Integration**: Seamlessly reads data from Hive tables.
- **Data Transformation**: Employs Spark's DataFrame API for complex data transformations.
- **MapR-DB Storage**: Efficiently stores the transformed data in MapR-DB JSON tables.
- **Scalable and Fault-Tolerant**: Inherits Apache Spark's scalability and fault tolerance, suitable for large datasets.

## Building and Deploying

### Building with SBT

#### Prerequisites

- Scala (version 2.11.x or higher)
- SBT (Scala Build Tool)
- Access to a Hadoop cluster with MapR and Hive

#### Compile and Package

1. **Clone the repository** (if applicable):
   ```bash
   git clone [repository_url]
   cd SparkMapRProject
   ```

2. **Compile the project**:
   ```bash
   sbt compile
   ```

3. **Package the application**:
   ```bash
   sbt package
   ```
   The generated JAR file will be located in `target/scala-<scala_version>/`.

### Deploying on a Spark Cluster

#### Spark Job Submission

1. **Submit the Spark job**:
   ```bash
   spark-submit      --class RetailDataETL      --master [master-url]      --deploy-mode cluster      --executor-memory 4G      --num-executors 4      /path/to/SparkMapRProject_2.11-0.1.0-SNAPSHOT.jar
   ```
   Replace `[master-url]` with your Spark cluster's master URL. Adjust memory and executors as needed.

2. **Monitor the job** using Spark's web UI.

3. **Verify the output** in MapR-DB after the job completion.

#### Notes

- Ensure proper configuration of Spark and Hadoop environment variables on the cluster.
- The application may require specific configurations (e.g., Hive settings, MapR-DB paths) based on your environment.
