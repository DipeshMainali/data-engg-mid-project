import sys
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize the GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Reading data from S3 bucket (e.g., CSV files)
source_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://weather-data-ingestion /"]},
    format="csv", 
    format_options={"withHeader": True}
)
# Removing duplicate rows
cleaned_data = source_data.drop_duplicates()

# Handling missing values (example: filling nulls with a default value)
cleaned_data = cleaned_data.fillna({"Temperature": 0, "Humidity": 0})

# Example: Filtering out rows where 'Temperature' is zero
cleaned_data = cleaned_data.filter(lambda x: x["Temperature"] != 0)

# Convert the DynamicFrame to a DataFrame for more advanced transformations
df_cleaned = cleaned_data.toDF()

# Drop rows with null values in any column
df_cleaned = df_cleaned.dropna()

# Convert back to a DynamicFrame
cleaned_dynamic_frame = DynamicFrame.fromDF(df_cleaned, glueContext, "cleaned_data")
# Example: Renaming columns for better readability
transformed_data = cleaned_dynamic_frame.rename_field("Temperature_C", "Temp_Celsius")
transformed_data = transformed_data.rename_field("Humidity_pct", "Humidity_Percent")

# Example: Adding a new column based on existing data
def add_temperature_fahrenheit(record):
    temp_celsius = record["Temp_Celsius"]
    temp_fahrenheit = (temp_celsius * 9/5) + 32
    record["Temp_Fahrenheit"] = temp_fahrenheit
    return record

# Apply transformation to add the Fahrenheit column
transformed_data = transformed_data.map(add_temperature_fahrenheit)

# Example: Converting DynamicFrame to DataFrame for more complex transformations
df_transformed = transformed_data.toDF()
df_transformed = df_transformed.withColumn("Temp_Fahrenheit", df_transformed["Temp_Celsius"] * 9/5 + 32)

# Convert back to DynamicFrame for output
final_dynamic_frame = DynamicFrame.fromDF(df_transformed, glueContext, "final_transformed_data")
# Writing the final transformed data back to S3 as CSV
glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://weather-data-ingestion/Processed_data/"},
    format="csv"
)

