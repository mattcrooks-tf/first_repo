# Databricks notebook source
from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType


DTYPE_DICT = {
  float: FloatType(),
  int: IntegerType(),
  str: StringType()
}

class pandas():
  def __init__(self):
    print("initializing...")
    df = None
    self.schema = None
  
  def read_csv(self, file_location, *args, **kwargs):
    print("reading...")
    self.file_location = file_location
    self.args = args
    self.kwargs = kwargs
    
    file_type = file_location.split('.')[-1]
    filename = file_location.split(f".{file_type}")[0]
    kwargs['file_type'] = file_type
    return self.load_data(file_location, *args, **kwargs)
  
  def display(self):
    display(self.df)

  def load_data(
    self,
    file_location,
    file_type='csv',
    infer_schema=False,
    schema=None,
    header='infer',
    sep=',',
    names=[],
    dtype={}
  ):

    print("loading...")
    if schema is not None:
      print("using schema")
      self.schema = schema
    elif (names != []) & (dtype != {}):
      print("using args and kwargs")
      self.schema = _create_schema(names, dtype)
      infer_schema = False
    
    if self.schema is not None:
      print('using self.schema')
      infer_schema = False
    elif self.schema is None:
      print("inferring schema")
      infer_schema = True
      if header == 'infer':
        spark_header = False
      elif header == 0:
        spark_header = True
  
    if infer_schema:
      return (
        spark.read.format(file_type)
        .option("inferSchema", infer_schema)
        .option("header", spark_header)
        .option("sep", delimiter)
        .load(file_location))
    else:
      return (
        spark.read.format(file_type)
        .schema(self.schema)
        .option("sep", delimiter)
        .load(file_location))
    
  def rename(self, columns = {}, inplace=False):
    names, dtype = names_and_dtype_from_schema(self.df.schema)
    names = [columns.get(name, name) for name in names]
    dtype = {columns.get(key, key): item for key, item in dtype.items()}
    self.schema = _create_schema(names, dtype)
    print(self.args)
    print(self.kwargs)
    if inplace:
      self.df = self.read_csv(self.file_location, *self.args, **self.kwargs)
    else:
      self.read_csv(self.file_location, *self.args, **self.kwargs).display()

def _create_schema(names, dtype):
    schema = StructType()
    for name in names:
      schema = schema.add(name, DTYPE_DICT.get(dtype[name], dtype[name]), True)
    return schema
  
def names_and_dtype_from_schema(schema):
  names = [column.name for column in schema]
  dtype = {column.name: column.dataType for column in schema}
  return names, dtype

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/iris/iris_data.csv"
file_type = "csv"


from pyspark.sql.types import StructType, StructField, FloatType, StringType, IntegerType

names = ["sepal_length", "sepal_width", "petal_length", "petal_width", "species"]
dtype = {
  "sepal_length": float, 
  "sepal_width": float, 
  "petal_length": float, 
  "petal_width": float, 
  "species": str
}
schema = _create_schema(names, dtype)

# CSV options
infer_schema = False
first_row_is_header = False
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type)\
  .schema(schema)\
  .option("sep", delimiter)\
  .load(file_location)

display(df)

# COMMAND ----------

pd = pandas()
pd.df = pd.read_csv(file_location)

# COMMAND ----------

pd.df.display()

# COMMAND ----------

pd.rename(columns = {
  '_c0': 'sepal_length',
  '_c1': 'sepal_width',
  '_c2': 'petal_length',
  '_c3': 'petal_width',
  '_c4': 'species'
})

# COMMAND ----------

pd.df.display()

# COMMAND ----------

pd.rename(
  columns = {
    '_c0': 'sepal_length',
    '_c1': 'sepal_width',
    '_c2': 'petal_length',
    '_c3': 'petal_width',
    '_c4': 'species'
  },
  inplace=True
)

# COMMAND ----------

pd.df.display()
