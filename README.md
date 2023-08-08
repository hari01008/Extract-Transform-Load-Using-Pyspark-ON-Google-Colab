Welcome to the Extract-Transform-Load (ETL) Using PySpark program on Google Colab. This repository contains a PySpark program that performs data extraction, transformation, and loading operations using Apache Spark on Google Colab.

## About the Program

The provided code is an ETL (Extract, Transform, Load) program written in Python using Apache Spark and the PySpark library. It reads data from CSV files, applies transformations, and saves the final output in Parquet file format.

## Program Steps

1. **Import Libraries:** Necessary libraries are imported, including `json` for working with JSON files and `pyspark` for Spark operations.

2. **Create SparkSession:** A SparkSession, the entry point for Spark, is created.

3. **Load Configuration:** The program loads configuration from a `config.json` file containing input CSV files, transformations, and output paths.

4. **Check Input:** The program checks if the input CSV file exists and is in the correct format.

5. **Load DataFrames:** DataFrames are loaded from input CSV files using Spark's `read.csv()` function.

6. **Convert to Parquet:** DataFrames are converted to Parquet format using the `write.mode().parquet()` method.

7. **Load Parquet DataFrames:** DataFrames are loaded from Parquet files using Spark's `read.parquet()` function.

8. **Apply Transformations:** Specified transformations are applied to the DataFrame, including date formatting, column operations, and dropping columns.

9. **Join DataFrames:** If applicable, DataFrames are joined based on specified columns.

10. **Save Transformed Data:** The transformed or joined DataFrame is saved to a new Parquet file.

11. **Read Final Data:** The final DataFrame is read from the output Parquet file.

12. **Align 'sno' Values:** A new DataFrame is created with aligned 'sno' values using Spark's window functions.

13. **Display Final DataFrame:** The final DataFrame is displayed using the `show()` method.

## Running the Program on Google Colab

1. **Upload Files:** Upload the `config.json` file and input CSV files to your Google Drive.

2. **Mount Google Drive:** Mount your Google Drive in Colab using the code:
   ```python
   from google.colab import drive
   drive.mount('/content/drive')
