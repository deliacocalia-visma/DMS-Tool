import argparse
import json
import os
import subprocess
import yaml
import boto3

# Define the replication task settings
rep_task_settings = {
    'replication-task-identifier': 'tripletex-to-globaldata',
    'source-endpoint-arn': os.environ['SOURCE_ENDPOINT_ARN'],
    'target-endpoint-arn': os.environ['TARGET_ENDPOINT_ARN'],
    'replication-instance-arn': os.environ['REPLICATION_INSTANCE_ARN'],
    'migration-type': 'full-load-and-cdc',
    'table-mappings': '',
}

# Load the table names from the file
table_names_file = open('table_names', 'r')
table_names=table_names_file.readlines()

# Create the table mappings for each table
client = boto3.client('dms')
for table_name in table_names:
  table_mappings = {
    'rules': []
  }
  table_mapping = {
      'rule-type': 'selection',
      'rule-id': '2',
      'rule-name': '2',
      'object-locator': {
          'schema-name': 'tripletex',
          'table-name': table_name.strip()
      },
      'rule-action': 'include'
  }
  table_mappings['rules'].append(table_mapping)

  # Add the schema renaming rule to the table mappings
  schema_rename_rule = {
      'rule-type': 'transformation',
      'rule-id': '1',
      'rule-name': '1',
      'rule-target': 'schema',
      'object-locator': {
          'schema-name': 'tripletex'
      },
      'rule-action': 'rename',
      'value': 'globaldata',
      'old-value': None
  }
  table_mappings['rules'].insert(0, schema_rename_rule)

  # Add the table mappings to the replication task settings
  rep_task_settings['table-mappings'] = json.dumps(table_mappings)

  task_settings = {
      'FullLoadSettings': {
          'TargetTablePrepMode': 'DO_NOTHING'
      },
      'ValidationSettings': {
          'ValidationPartialLobSize': 0,
          'PartitionSize': 10000,
          'RecordFailureDelayLimitInMinutes': 0,
          'SkipLobColumns': False,
          'FailureMaxCount': 10000,
          'HandleCollationDiff': False,
          'ValidationQueryCdcDelaySeconds': 0,
          'ValidationMode': 'ROW_LEVEL',
          'TableFailureMaxCount': 1000,
          'RecordFailureDelayInMinutes': 5,
          'MaxKeyColumnSize': 8096,
          'EnableValidation': True,
          'ThreadCount': 5,
          'RecordSuspendDelayInMinutes': 30,
          'ValidationOnly': False
      }
  }

  # Create replication task
  try:
    response = client.create_replication_task(
        ReplicationTaskIdentifier=table_name.strip()+'-to-globaldata',
        SourceEndpointArn=rep_task_settings['source-endpoint-arn'],
        TargetEndpointArn=rep_task_settings['target-endpoint-arn'],
        ReplicationInstanceArn=rep_task_settings['replication-instance-arn'],
        MigrationType='full-load-and-cdc',
        TableMappings=rep_task_settings['table-mappings'],
        ReplicationTaskSettings=json.dumps(task_settings)
    )
    print('Created DMS Replication Task for table '+table_name.strip())
  except Exception as e:
    print(f"An error occurred: {e}")

