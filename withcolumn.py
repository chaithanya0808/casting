from pyspark.sql.functions import col, cast

def convert_to_int(df, cols):
  """
  Converts specified columns to IntegerType in a DataFrame.

  Args:
      df (pyspark.sql.DataFrame): The DataFrame to modify.
      cols (list): A list of column names to convert to IntegerType.

  Returns:
      pyspark.sql.DataFrame: The DataFrame with converted columns.
  """
  for col_name in cols:
    df = df.withColumn(col_name, cast(col(col_name), IntegerType()))
  return df

# List of columns to convert
cols_to_convert = ["EMPLOYEE_COUNT", "EMPLOYEE_DETAILS.EMPLOYEE_AGE", 
                    "EMPLOYEE_DETAILS.NUMBER_OF_CHILDREN", 
                    "RELATIVE_DETAILS.RELATIVE_AGE", "RELATIVE_DETAILS.NUMBER_OF_CHILDREN"]

# Apply the conversion function
df = convert_to_int(df.copy(), cols_to_convert)
