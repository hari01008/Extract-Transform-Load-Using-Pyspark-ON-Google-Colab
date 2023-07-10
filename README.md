# Extract-Transform-Load-Using-Pyspark-ON-Google-Colab
Here we wrote a pyspark program read data from csv file and convert it to .parquet file and applying the transformation and save ant print the final output in parquet file format
The provided code is an ETL (Extract, Transform, Load) program written in Python using Apache Spark and the PySpark library. It performs the following steps:

1. It imports the required libraries, including `json` for working with JSON files, `pyspark` for Spark operations, and other necessary functions and classes.

2. It creates a SparkSession, which is the entry point for working with Spark.

3. It loads the configuration from a `config.json` file. This file contains information about the input CSV files, transformations to apply, and the output Parquet file.

4. It checks if the input CSV file exists and is in the correct format.

5. It loads the DataFrame from the input CSV file using Spark's `read.csv()` function. It also loads another DataFrame from a second input CSV file, if specified in the configuration.

6. It converts the DataFrame to a Parquet file format using the `write.mode().parquet()` method. This step is necessary to perform transformations efficiently.

7. It loads the DataFrames from the Parquet files using Spark's `read.parquet()` function.

8. It applies transformations specified in the configuration to the DataFrame. These transformations can include date formatting, column concatenation, column splitting, and column dropping.

9. If a column named 'Industry' exists in the second input DataFrame and a column named 'sno' exists in the first DataFrame, it joins the two DataFrames based on the 'sno' column and creates a new DataFrame called `joined_df`.

10. It saves the transformed DataFrame or the joined DataFrame (if applicable) to a new Parquet file specified in the configuration.

11. It reads the final transformed DataFrame from the output Parquet file.

12. It creates a new DataFrame called `final_df` with aligned 'sno' values using Spark's `row_number()` function and `Window.orderBy()` method.

13. It displays the final DataFrame with aligned 'sno' values using the `show()` method.

***********************To run this program on Google Colab, you would need to follo w these steps:**********************

1. Upload the `config.json` file and the input CSV files to your Google Drive.

2. Mount your Google Drive in Colab using the following code:
```python
from google.colab import drive
drive.mount('/content/drive')
```

3. Modify the file path in the code to match the location of the `config.json` file in your Google Drive. For example:
```python
with open('/content/drive/MyDrive/project/config.json') as f:
```

4. Modify the file paths for the input CSV files in the configuration to match their locations in your Google Drive.

5. Run the code cell. It will load the configuration, perform the ETL process, and display the final DataFrame with aligned 'sno' values.

Ensure that you have installed the necessary libraries (such as PySpark) in your Colab environment. You can install them using `!pip install library_name` if needed.

Note: Adjust the code as necessary to match your specific file paths, configurations, and transformations.
