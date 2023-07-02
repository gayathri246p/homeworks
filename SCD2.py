import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import lead, date_sub, col, coalesce, lit

spark = SparkSession.builder.getOrCreate()

existing_data = [
    (111, 'Product A', 'abc', 10.99, '2021-01-01', '9999-12-31'),
    (222, 'Product B', 'def', 29.99, '2021-05-01', '9999-12-31')

]
schema = ['productid', 'prodname', 'prodesc', 'prodprice', 'startdate', 'enddate']
existing_data_df = spark.createDataFrame(existing_data, schema)

new_data = [
    (111, 'Product A', 'abc', 15.99, '2021-06-01', '9999-12-31')
]
new_data_df = spark.createDataFrame(new_data, schema)


def update_scd2_data(existing_df, new_df):
    combined_df = existing_df.union(new_df)

    # Add a grouping key column to represent productid and sort the data by it and startdate
    grouped_df = combined_df.withColumn('group_key', col('productid')).orderBy('group_key', 'startdate')

    # Create a window specification ordered by the group key
    window_spec = Window.partitionBy('group_key').orderBy('startdate')

    # Update the end date of the current row based on the start date of the next row
    updated_df = grouped_df.withColumn('enddate', date_sub(lead('startdate').over(window_spec), 1))

    # Assign the end date of the last row to '9999-12-31'
    updated_df = updated_df.withColumn('enddate', coalesce(col('enddate'), lit('9999-12-31')))
    updated_df = updated_df.drop(col('group_key'))

    updated_df.show()
    return updated_df

a = update_scd2_data(existing_data_df, new_data_df)


new_data1 = [
    (111, 'Product A', 'abc', 12.99, '2021-07-30', '9999-12-31'),
]
new_data_df1 = spark.createDataFrame(new_data1, schema)

update_scd2_data(a, new_data_df1)
