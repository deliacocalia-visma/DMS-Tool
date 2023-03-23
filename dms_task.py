import boto3
import json
import os
import yaml


def get_tables(filename='table_names.yaml'):
    table_actions = []

    # Load the table names from the file
    with open(filename, "r") as stream:
        try:
            file_contents = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print("error reading yaml-file:")
            print(e)

    for key in file_contents:
        if file_contents[key] == "tripletex_to_globaldata":
            table_actions.append(key)

    return table_actions


def main():
    
    # Define the replication task settings
    rep_task_settings = {
        'replication-task-identifier': 'tripletex-to-globaldata',
        'source-endpoint-arn': os.environ['SOURCE_ENDPOINT_ARN'],
        'target-endpoint-arn': os.environ['TARGET_ENDPOINT_ARN'],
        'replication-instance-arn': os.environ['REPLICATION_INSTANCE_ARN'],
        'migration-type': 'full-load-and-cdc',
        'table-mappings': '',
    }
    
    
    table_actions = get_tables()
    
    # Create the table mappings for each table
    client = boto3.client('dms')
    
    # Get all existing replication tasks
    existing_tasks = client.describe_replication_tasks()['ReplicationTasks']
    
    # Create a list of replication task identifiers from the 'table_names' file
    task_identifiers = [task.strip() for task in table_actions]
    
    # Create a list of replication task ARNs from the existing tasks
    task_arns = [task['ReplicationTaskArn'] for task in existing_tasks]
    
    # Delete any existing replication tasks that are not in the 'table_names' file
    for task in existing_tasks:
        if task['ReplicationTaskIdentifier'] not in task_identifiers:
            try:
                client.delete_replication_task(
                    ReplicationTaskArn=task['ReplicationTaskArn'])
                print(
                    f"Deleted DMS Replication Task {task['ReplicationTaskIdentifier']}")
            except Exception as e:
                print(
                    f"An error occurred while deleting DMS Replication Task {task['ReplicationTaskIdentifier']}: {e}")
    
    # Create new replication tasks for each table in the 'table_names' file
    for table_action in table_actions:
        counter = 1
        pre, direction, table_name = table_action.strip().split('-', 2)
        if direction == 'globaldata':
            schema_name = 'tripletex'
            target_schema_name = 'globaldata'
        elif direction == 'tripletex':
            schema_name = 'globaldata'
            target_schema_name = 'tripletex'
        else:
            print(f"Invalid direction specified for table {table_name}")
            continue
    
        table_mappings = {
            'rules': []
        }
        table_mapping = {
            'rule-type': 'selection',
            'rule-id': str(counter),
            'rule-name': str(counter),
            'object-locator': {
                'schema-name': schema_name,
                'table-name': table_name
            },
            'rule-action': 'include'
        }
        counter += 1
        table_mappings['rules'].append(table_mapping)
    
        # Add the schema renaming rule to the table mappings
        schema_rename_rule = {
            'rule-type': 'transformation',
            'rule-id': str(counter),
            'rule-name': str(counter),
            'rule-target': 'schema',
            'object-locator': {
                'schema-name': schema_name
            },
            'rule-action': 'rename',
            'value': target_schema_name,
            'old-value': None
        }
        counter += 1
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
                ReplicationTaskIdentifier=table_action.strip(),
                SourceEndpointArn=rep_task_settings['source-endpoint-arn'],
                TargetEndpointArn=rep_task_settings['target-endpoint-arn'],
                ReplicationInstanceArn=rep_task_settings['replication-instance-arn'],
                MigrationType='full-load-and-cdc',
                TableMappings=rep_task_settings['table-mappings'],
                ReplicationTaskSettings=json.dumps(task_settings),
                Tags=[{'Key': 'Generated by',            'Value': 'GitHub DMS Tool'},]
            )
    
            print('Created DMS Replication Task for table '+table_name.strip())
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
