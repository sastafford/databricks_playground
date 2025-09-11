# Databricks notebook source
dbutils.widgets.text("catalog", "default", "Catalog")
dbutils.widgets.text("schema", "public", "Schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS usa.scott_stafford_cases.cases

# COMMAND ----------

# MAGIC %pip install dbldatagen faker

# COMMAND ----------

from dbldatagen import DataGenerator, fakerText
from faker.providers import internet
from pyspark.sql.types import IntegerType

shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = 5000

# partition parameters etc.
spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

my_word_list = [ "HIGH", "MEDIUM", "LOW" ]

fakerDataspec = (DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("case_id", IntegerType(), uniqueValues=5000, random=True)
            .withColumn("case", text=fakerText("company") )
            .withColumn("description", text=fakerText("catch_phrase") )
            .withColumn("email", text=fakerText("ascii_company_email") )
            .withColumn("clearance", text=fakerText("word", ext_word_list=my_word_list))
            .withColumn("compartment", "string", values=['A', 'B', 'C', 'D', 'E'], random=True, percentNulls=0.10)
            )
dfFakerOnly = fakerDataspec.build()
display(dfFakerOnly)

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

cases_path = "{}.{}.cases".format(catalog, schema)
print(cases_path)

# COMMAND ----------

dfFakerOnly.write.format("delta").mode("overwrite").saveAsTable(cases_path)
