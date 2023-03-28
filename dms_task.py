import boto3
import json
import os
import yaml
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_client = WebClient(token=os.environ['SLACK_BOT_TOKEN'])
dmschannel="#novak-test"
botname="DMS-BOT"
icon="https://upcdn.io/W142hJk/image/demo/4mZ3fv3pLT.png"


def get_tables(filename='table_names.yml'):
    table_actions = []

    # Load the table names from the file
    with open(filename, "r") as stream:
        try:
            file_contents = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            print("error reading yaml-file:")
            print(e)

    tables_old = []
    for key in file_contents:
        tables_old.append(key)

        direction = file_contents[key]

        # ensure the migration description is known, or let the user know
        if direction == "to-globaldata":
            schema_name = 'tripletex'
            target_schema_name = 'globaldata'
        elif direction == "to-tripletex":
            schema_name = 'globaldata'
            target_schema_name = 'tripletex'
        else:
            raise Exception(
                f"unknown migration direction specified: '{direction}'")

        table = {'name': key, 'schema_name': schema_name,
                 'target_schema_name': target_schema_name}
        table_actions.append(table)

    tables_new = tables_old.copy()

    return table_actions, tables_old, tables_new


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

    table_actions, tables_old, tables_new = get_tables()

    # Create the table mappings for each table
    client = boto3.client('dms')

    # Get the list of existing replication tasks
    existing_tasks = client.describe_replication_tasks()['ReplicationTasks']

    # Loop through existing tasks and stop tasks that are no longer needed
    for task in existing_tasks:
        task_identifier = task['ReplicationTaskIdentifier']
        tags = client.list_tags_for_resource(
            ResourceArn=task['ReplicationTaskArn'])['TagList']
        if any(tag['Key'] == 'Generated by' and tag['Value'] == 'GitHub DMS Tool' for tag in tags):
            if task_identifier not in [f"{table['name']}-{table['schema_name']}-{table['target_schema_name']}" for table in table_actions]:
                try:
                    client.stop_replication_task(
                        ReplicationTaskArn=task['ReplicationTaskArn'])
                    print(f"Stopped DMS Replication Task {task_identifier}")
                    try:
                        response = slack_client.chat_postMessage(
                            channel=dmschannel,
                            text=f"Stopped DMS Replication Task *{task_identifier}*",
                            username=botname,
                            as_user=False,
                            icon_url=icon
                        )
                        print(
                            f"Sent Slack message for stopped DMS Replication Task {task_identifier}")
                    except SlackApiError as e:
                        print(f"Error sending Slack message: {e}")
                except Exception as e:
                    print(
                        f"An error occurred while stopping DMS Replication Task {task_identifier}: {e}")

    # Create new replication tasks for each table in the 'table_names' file
    for table in table_actions:
        counter = 1
        table_name = table['name']
        schema_name = table['schema_name']
        target_schema_name = table['target_schema_name']

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
            },
            'Logging': {
                'EnableLogging': True
            }
        }

        # Create replication task
        try:
            response = client.create_replication_task(
                ReplicationTaskIdentifier=f"{table_name}-{schema_name}-{target_schema_name}",
                SourceEndpointArn=rep_task_settings['source-endpoint-arn'],
                TargetEndpointArn=rep_task_settings['target-endpoint-arn'],
                ReplicationInstanceArn=rep_task_settings['replication-instance-arn'],
                MigrationType='full-load-and-cdc',
                TableMappings=rep_task_settings['table-mappings'],
                ReplicationTaskSettings=json.dumps(task_settings),
                Tags=[{'Key': 'Generated by',
                       'Value': 'GitHub DMS Tool'},]
            )

        # waits for task to be ready before starting
            waiter = client.get_waiter('replication_task_ready')
            waiter.wait(
                Filters=[
                    {
                        'Name': 'replication-task-arn',
                        'Values': [
                            response['ReplicationTask']['ReplicationTaskArn'],
                        ]
                    },
                ],
                MaxRecords=100,
                WaiterConfig={
                    'Delay': 15,
                    'MaxAttempts': 60
                }
            )
        #starts the task
            client.start_replication_task(
                ReplicationTaskArn=response['ReplicationTask']['ReplicationTaskArn'],
                StartReplicationTaskType='start-replication'
            )

            print('Created DMS Replication Task for table '+table_name.strip())
            try:
                response = slack_client.chat_postMessage(
                    channel=dmschannel,
                    text=f"Created DMS Replication Task for table *" +table_name.strip()+ "* from *" +schema_name + "* to *" +target_schema_name + "*",
                    username=botname,
                    as_user=False,
                    icon_url=icon
                )
                print(
                    f"Sent Slack message for created DMS Replication Task for table "+table_name.strip())
            except SlackApiError as e:
                print(f"Error sending Slack message: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
