import boto3

dms_resources = boto3.resource('dms')

for instance in dms_resources:
   print (instance)
