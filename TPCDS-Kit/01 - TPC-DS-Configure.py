# Databricks notebook source
# MAGIC %md
# MAGIC ## Configure the TPC-DS data generator
# MAGIC 
# MAGIC ###Overview
# MAGIC Generating TPC-DS datasets with spark-sql-perf involves the following steps.<br><br>
# MAGIC 
# MAGIC 1. Add the spark-sql-perf library jar to your Databricks cluster.
# MAGIC 2. Mount storage.
# MAGIC 3. Install the Databricks <a href="https://github.com/databricks/tpcds-kit" target="_blank">TPC-DS benchmark kit</a>.
# MAGIC 4. Restart the cluster.
# MAGIC 4. Run the TPC-DS-Generate notebook to generate the data and setup the database.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Add the spark-sql-perf library jar to your Databricks cluster
# MAGIC 
# MAGIC Install the jar file from the <a href="https://github.com/BlueGranite/tpc-ds-dataset-generator/tree/master/lib" target="_blank">BlueGranite tpc-ds-dataset-generator repo</a> using the Databricks cluster Libraries menu. Use the jar file that matches the Scala version of the cluster. **When downloading the file, make sure you click through to the jar file page in the repo and use the Download button to get the actual file**.
# MAGIC 
# MAGIC Alternately you can build the spark-sql-perf library jar yourself using <a href="https://www.scala-sbt.org/1.x/docs/index.html" target="_blank">sbt</a>:<br><br>
# MAGIC 
# MAGIC 1. Install sbt on your local machine using the instructions <a href="https://docs.scala-lang.org/getting-started/sbt-track/getting-started-with-scala-and-sbt-on-the-command-line.html" target="_blank">here</a>.
# MAGIC 2. Clone the <a href="https://github.com/databricks/spark-sql-perf.git" target="_blank">spark-sql-perf repository</a> and navigate to the new directory.<br><br>
# MAGIC `git clone https://github.com/databricks/spark-sql-perf.git`<br><br>
# MAGIC 3. Run `sbt package` to build for scala 2.11. Run `sbt +package` to build for scala 2.11 and 2.12.
# MAGIC 4. The jar file can then be found in the /spark-sql-perf/target/scala-2.1x directory. Install the library using the Databricks cluster Libraries menu.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Mount ADLS and use it for the dataset download

# COMMAND ----------

#dbutils.fs.unmount("/mnt/tpcds")
STORAGE_ACCOUNT = ""
CONTAINER = ""
MOUNT_POINT = "/mnt/<>"

source_str = "abfss://{container}@{storage_acct}.dfs.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "",
           "fs.azure.account.oauth2.client.secret": "",
           "fs.azure.account.oauth2.client.endpoint": ""}
dbutils.fs.mount(
  source = source_str,
  mount_point = MOUNT_POINT,
  extra_configs = configs
)


# COMMAND ----------

# MAGIC %fs ls /mnt/deepaksekaradls

# COMMAND ----------

# MAGIC %fs mkdirs /mnt/deepaksekaradls/bootcamp2/data/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Install the Databricks TPC-DS benchmark kit
# MAGIC 
# MAGIC A <a href="https://docs.azuredatabricks.net/user-guide/clusters/init-scripts.html#example-cluster-scoped-init-script" target="_blank">cluster-scoped init script</a> is used to install the Databricks TPC-DS benchmark kit on all cluster nodes.
# MAGIC 
# MAGIC Create a directory for the init script.

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/databricks/scripts")

# COMMAND ----------

# MAGIC %md
# MAGIC Create the BASH init script that will install the <a href="https://github.com/databricks/tpcds-kit" target="_blank">Databricks TPC-DS benchmark kit and prequisites.

# COMMAND ----------

dbutils.fs.put("/databricks/scripts/tpcds-install.sh","""
#!/bin/bash
sudo apt-get --assume-yes install gcc make flex bison byacc git

cd /usr/local/bin
git clone https://github.com/databricks/tpcds-kit.git
cd tpcds-kit/tools
make OS=LINUX""", True)

# COMMAND ----------

# MAGIC %sh ls /usr/local/bin/tpcds-kit/

# COMMAND ----------

# MAGIC %md
# MAGIC Add the init script to your cluster using the steps below:<br><br>
# MAGIC 
# MAGIC 1. On the cluster configuration page, click **Advanced Options**.
# MAGIC 2. At the bottom of the page, click **Init Scripts**.<br><br>
# MAGIC ![Databricks Init Script menu](https://github.com/BlueGranite/tpc-ds-dataset-generator/blob/master/img/databricks_init_script_menu.png?raw=true "Databricks Init Script menu")<br><br> 
# MAGIC 3. In the Destination drop-down, select a destination type.
# MAGIC 4. Specify a path to the init script.
# MAGIC 5. Click Add.
# MAGIC 6. Upload your script to the specified location.
# MAGIC 7. Restart the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC After the cluster restarts, you can run the [data generator notebook]($./TPC-DS-GenerateData).

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks/scripts/tpcds-install.sh"))

# COMMAND ----------


