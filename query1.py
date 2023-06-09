import tuplex

c = tuplex.Context({'tuplex.redirectToPythonLogging':True, 'tuplex.executorMemory':'3G', 'tuplex.driverMemory':'3G'})
ds = c.csv('s3://clustertuplexproofs231/311_subset.csv')
dict(zip(ds.columns, ds.types))
year_to_investigate = 2019

def extract_month(row):
  date = row['Created Date']
  date = date[:date.find(' ')]
  return int(date.split('/')[0])

def extract_year(row):
  date = row['Created Date']
  date = date[:date.find(' ')]
  return int(date.split('/')[-1])

ds2 = ds.withColumn('Month', extract_month) \
  .withColumn('Year', extract_year) \
  .filter(lambda row: 'Mosquito' in row['Complaint Type']) \
  .filter(lambda row: row['Year'] == year_to_investigate) \
  .selectColumns(['Month', 'Year', 'Complaint Type'])


ds2.show(5)