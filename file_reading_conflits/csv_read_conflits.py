from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("read_csv_conflits") \
    .getOrCreate()

# When you set ignoreLeadingWhiteSpace to true, it tells Spark to ignore any leading whitespaces while parsing the fields. This is helpful in scenarios where your data might have inconsistencies in formatting, such as leading spaces before values in some columns.
#Ex:- if ur csv data had space in front

# id, name, age
# 1, John, 25
# 2,   Alice, 30

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .load("file_path")

#Similarly if whitespaces at the end of each field

df_1 = spark.read.format("csv") \
    .option("header", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .load("file_path")

#if the Data in CSV is like below
# id,name,age
# 1,John\, Doe,25
# if "\" is found in the data we need to specify that to ignore
df_2 = spark.read.format("csv") \
    .option("header", "true") \
    .option("escape", "|") \
    .load("file_path")

#if ur CSV data have empty space insted of null
#or to consider any perticular value as null
df_3 = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "") \
    .load("file_path")
# specify the string representing null or empty spaces

# if your csv file has data as shown below
##  id, name, age
##  1, "John, Doe", 25
##  2, "Alice
##  Wonderland
##  ",30
# i.e if the previous "string" continues in second line
df_4 = spark.read.format("csv") \
    .option("header", "true") \
    .option("quote", "'") \
    .load("file_path")

# we can also use
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("multiLine", "true") \
    .load("file_path")
# Handles multiline records. If set to True, it treats records
# that span multiple lines as a single record. The default value is False.
"""mode: Specifies the parsing mode. Possible values are "PERMISSIVE", "DROPMALFORMED", and "FAILFAST". 
The default value is "PERMISSIVE".
"PERMISSIVE": Sets other fields to null when it encounters a malformed line and moves on.
"DROPMALFORMED": Drops the row containing malformed data.
"FAILFAST": Throws an exception when it encounters a malformed line."""

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .load("file_path")

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "DROPMALFORMED") \
    .load("file_path")

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "FAILFAST") \
    .load("file_path")

spark.stop()