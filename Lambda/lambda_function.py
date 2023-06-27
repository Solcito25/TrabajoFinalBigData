import json
import boto3
import csv 
import io
import tuplex

s3Client = boto3.client('s3')    

def extract_month(row):
  date = row['Created Date']
  date = date[:date.find(' ')]
  return int(date.split('/')[0])
def extract_year(row):
  date = row['Created Date']
  date = date[:date.find(' ')]
  return int(date.split('/')[-1])


def lambda_handler(event, context):
    # Get bucket name and key from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    response = s3Client.get_object(Bucket=bucket, Key=key)
    
    c = tuplex.Context()
    ds = c.csv(reader(io.StringIO(data)))
    ds.show(5)
    dict(zip(ds.columns, ds.types))
    year_to_investigate = 2019

    ds2 = ds.withColumn('Month', extract_month) \
        .withColumn('Year', extract_year) \
        .filter(lambda row: 'Mosquito' in row['Complaint Type']) \
        .filter(lambda row: row['Year'] == year_to_investigate) \
        .selectColumns(['Month', 'Year', 'Complaint Type'])
        
    ds2.show(5)