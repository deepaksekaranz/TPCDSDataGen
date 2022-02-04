# Databricks notebook source
# MAGIC %md
# MAGIC ## Generate TPC-DS data
# MAGIC 
# MAGIC Generating data at larger scales can take hours to run, and you may want to run the notebook as a job.
# MAGIC 
# MAGIC The cell below generates the data. Read the code carefully, as it contains many parameters to control the process. See the <a href="https://github.com/databricks/spark-sql-perf" target="_blank">Databricks spark-sql-perf repository README</a> for more information.

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.databricks.spark.sql.perf.tpcds.TPCDSTables
# MAGIC 
# MAGIC // Set:
# MAGIC val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
# MAGIC val scaleFactoryInt = scaleFactor.toInt
# MAGIC 
# MAGIC val scaleName = if(scaleFactoryInt < 1000){
# MAGIC     f"${scaleFactoryInt}%03d" + "GB"
# MAGIC   } else {
# MAGIC     f"${scaleFactoryInt / 1000}%03d" + "TB"
# MAGIC   }
# MAGIC 
# MAGIC val fileFormat = "parquet" // valid spark file format like parquet, csv, json.
# MAGIC val rootDir = s"/mnt/deepaksekaradls/bootcamp2/data/SourceFiles${scaleName}_${fileFormat}"
# MAGIC val databaseName = "deepaksekartpcds" + scaleName // name of database to create.
# MAGIC 
# MAGIC // Run:
# MAGIC val tables = new TPCDSTables(sqlContext,
# MAGIC     dsdgenDir = "/usr/local/bin/tpcds-kit/tools", // location of dsdgen
# MAGIC     scaleFactor = scaleFactor,
# MAGIC     useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
# MAGIC     useStringForDate = false) // true to replace DateType with StringType
# MAGIC 
# MAGIC tables.genData(
# MAGIC     location = rootDir,
# MAGIC     format = fileFormat,
# MAGIC     overwrite = true, // overwrite the data that is already there
# MAGIC     partitionTables = false, // create the partitioned fact tables 
# MAGIC     clusterByPartitionColumns = false, // shuffle to get partitions coalesced into single files. 
# MAGIC     filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
# MAGIC     tableFilter = "", // "" means generate all tables
# MAGIC     numPartitions = 10) // how many dsdgen partitions to run - number of input tasks.
# MAGIC 
# MAGIC // Create the specified database
# MAGIC //sql(s"create database $databaseName")
# MAGIC sql(s"drop database if exists $databaseName cascade")
# MAGIC sql(s"create database if not exists $databaseName")
# MAGIC // Create the specified database
# MAGIC //sql(s"create database $databaseName")
# MAGIC //sql(s"create database if not exists $databaseName")
# MAGIC 
# MAGIC // Create metastore tables in a specified database for your data.
# MAGIC // Once tables are created, the current database will be switched to the specified database.
# MAGIC tables.createExternalTables(rootDir, fileFormat, databaseName, overwrite = true, discoverPartitions = true)
# MAGIC 
# MAGIC // Or, if you want to create temporary tables
# MAGIC // tables.createTemporaryTables(location, fileFormat)
# MAGIC 
# MAGIC // For CBO only, gather statistics on all columns:
# MAGIC tables.analyzeTables(databaseName, analyzeColumns = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Parquet TPC-DS data to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_sales` --PARTITIONED BY (`cs_sold_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.catalog_sales;
# MAGIC CREATE TABLE deepaksekartpcds001GB.catalog_sales USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_sales';
# MAGIC 
# MAGIC --CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_returns` --PARTITIONED BY (`cr_returned_date_sk` INT);
# MAGIC --drop table if exists deepaksekartpcds001GB.catalog_returns;
# MAGIC --CREATE TABLE deepaksekartpcds001GB.catalog_returns USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_returns';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/inventory` --PARTITIONED BY (`inv_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.inventory;
# MAGIC CREATE TABLE deepaksekartpcds001GB.inventory USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/inventory';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store_sales` --PARTITIONED BY (`ss_sold_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.store_sales;
# MAGIC CREATE TABLE deepaksekartpcds001GB.store_sales USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store_sales';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store_returns` --PARTITIONED BY (`sr_returned_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.store_returns;
# MAGIC CREATE TABLE deepaksekartpcds001GB.store_returns USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store_returns';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_sales` --PARTITIONED BY (`ws_sold_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.web_sales;
# MAGIC CREATE TABLE deepaksekartpcds001GB.web_sales USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_sales';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_returns` --PARTITIONED BY (`wr_returned_date_sk` INT);
# MAGIC drop table if exists deepaksekartpcds001GB.web_returns;
# MAGIC CREATE TABLE deepaksekartpcds001GB.web_returns USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_returns';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/call_center`;
# MAGIC drop table if exists deepaksekartpcds001GB.call_center;
# MAGIC CREATE TABLE deepaksekartpcds001GB.call_center USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/call_center';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_page`;
# MAGIC drop table if exists deepaksekartpcds001GB.catalog_page;
# MAGIC CREATE TABLE deepaksekartpcds001GB.catalog_page USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/catalog_page';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer`;
# MAGIC drop table if exists deepaksekartpcds001GB.customer;
# MAGIC CREATE TABLE deepaksekartpcds001GB.customer USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer_address`;
# MAGIC drop table if exists deepaksekartpcds001GB.customer_address;
# MAGIC CREATE TABLE deepaksekartpcds001GB.customer_address USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer_address';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer_demographics`;
# MAGIC drop table if exists deepaksekartpcds001GB.customer_demographics;
# MAGIC CREATE TABLE deepaksekartpcds001GB.customer_demographics USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/customer_demographics';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/date_dim`;
# MAGIC drop table if exists deepaksekartpcds001GB.date_dim;
# MAGIC CREATE TABLE deepaksekartpcds001GB.date_dim USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/date_dim';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/household_demographics`;
# MAGIC drop table if exists deepaksekartpcds001GB.household_demographics;
# MAGIC CREATE TABLE deepaksekartpcds001GB.household_demographics USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/household_demographics';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/income_band`;
# MAGIC drop table if exists deepaksekartpcds001GB.income_band;
# MAGIC CREATE TABLE deepaksekartpcds001GB.income_band USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/income_band';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/item`;
# MAGIC drop table if exists deepaksekartpcds001GB.item;
# MAGIC CREATE TABLE deepaksekartpcds001GB.item USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/item';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/promotion`;
# MAGIC drop table if exists deepaksekartpcds001GB.promotion;
# MAGIC CREATE TABLE deepaksekartpcds001GB.promotion USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/promotion';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/reason`;
# MAGIC drop table if exists deepaksekartpcds001GB.reason;
# MAGIC CREATE TABLE deepaksekartpcds001GB.reason USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/reason';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/ship_mode`;
# MAGIC drop table if exists deepaksekartpcds001GB.ship_mode;
# MAGIC CREATE TABLE deepaksekartpcds001GB.ship_mode USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/ship_mode';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store`;
# MAGIC drop table if exists deepaksekartpcds001GB.store;
# MAGIC CREATE TABLE deepaksekartpcds001GB.store USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/store';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/time_dim`;
# MAGIC drop table if exists deepaksekartpcds001GB.time_dim;
# MAGIC CREATE TABLE deepaksekartpcds001GB.time_dim USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/time_dim';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/warehouse`;
# MAGIC drop table if exists deepaksekartpcds001GB.warehouse;
# MAGIC CREATE TABLE deepaksekartpcds001GB.warehouse USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/warehouse';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_page`;
# MAGIC drop table if exists deepaksekartpcds001GB.web_page;
# MAGIC CREATE TABLE deepaksekartpcds001GB.web_page USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_page';
# MAGIC 
# MAGIC CONVERT TO DELTA parquet.`/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_site`;
# MAGIC drop table if exists deepaksekartpcds001GB.web_site;
# MAGIC CREATE TABLE deepaksekartpcds001GB.web_site USING DELTA LOCATION '/mnt/deepaksekaradls/bootcamp2/data/SourceFiles001GB_parquet/web_site';

# COMMAND ----------

# MAGIC %md
# MAGIC ###Entity Relationship

# COMMAND ----------

# MAGIC %md
# MAGIC ####Fact
# MAGIC The schema includes seven fact tables:
# MAGIC 
# MAGIC A pair of fact tables focused on the product sales and returns for each of the three channels. ie:  
# MAGIC Store Sales and returns  
# MAGIC Catalog Sales and returns  
# MAGIC Web Sales and returns  
# MAGIC   
# MAGIC   
# MAGIC A single fact table that models inventory for the catalog and internet sales channels.  
# MAGIC Inventory

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dimensions
# MAGIC In addition, the schema includes 17 dimension tables that are associated with all sales channels
# MAGIC 
# MAGIC Store  
# MAGIC Call Center  
# MAGIC Catalog Page  
# MAGIC Web Site  
# MAGIC Web Page  
# MAGIC Warehouse  
# MAGIC Customer  
# MAGIC Customer Address  
# MAGIC Customer Demographics  
# MAGIC Date Dim  
# MAGIC Household Demographics  
# MAGIC Item  
# MAGIC Income Band  
# MAGIC Promotion  
# MAGIC Reason  
# MAGIC Ship Mode  
# MAGIC Time Dim  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store
# MAGIC 
# MAGIC #### Store Sales
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_sales_er_diagram.jpg?tseed=1530804597" width=1000/>
# MAGIC <br>
# MAGIC <br>
# MAGIC #### Store Returns
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_store_er_diagram.jpg?tseed=1530804688" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Web
# MAGIC 
# MAGIC #### Web Sales
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_web_sales_er_diagram.jpg?tseed=1530804992" width=1000/>
# MAGIC <br>
# MAGIC <br>
# MAGIC #### Web Returns
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_web_return_er_diagram.jpg?tseed=1530866445" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog
# MAGIC 
# MAGIC #### Catalog Sales
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_catalog_sales_er_diagram.jpg?tseed=1530804777" width=1000/>
# MAGIC <br>
# MAGIC <br>
# MAGIC #### Catalog Returns
# MAGIC <img src="https://datacadamia.com/_media/data/type/relation/benchmark/tpcds/tpcds_catalog_return_er_diagram.jpg?tseed=1530804876" width=1000/>
