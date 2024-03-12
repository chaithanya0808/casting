from pyspark.sql.functions import col, cast

def convert_to_int(df, cols):
  """
  Converts specified columns to IntegerType in a DataFrame.

  Args:
      df (pyspark.sql.DataFrame): The DataFrame to convert.
      cols (list): A list of column names to convert to IntegerType.

  Returns:
      pyspark.sql.DataFrame: A new DataFrame with converted columns.
  """
  # Select all columns and apply casting for specified columns
  df = df.selectExpr("*", *[f"cast({col_name}) as {col_name}" for col_name in cols])
  return df

# List of columns to convert
cols_to_convert = ["EMPLOYEE_COUNT", "EMPLOYEE_DETAILS.EMPLOYEE_AGE", 
                    "EMPLOYEE_DETAILS.NUMBER_OF_CHILDREN", 
                    "RELATIVE_DETAILS.RELATIVE_AGE", "RELATIVE_DETAILS.NUMBER_OF_CHILDREN"]

# Apply the conversion function
df = convert_to_int(df, cols_to_convert)
