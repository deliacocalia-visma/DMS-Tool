import argparse
import json
import os
import subprocess
import yaml


# Define the replication task settings
rep_task_settings = {
    'replication-task-identifier': 'tripletex-to-globaldata',
    'source-endpoint-arn': os.environ['SOURCE_ENDPOINT_ARN'],
    'target-endpoint-arn': os.environ['TARGET_ENDPOINT_ARN'],
    'replication-instance-arn': os.environ['REPLICATION_INSTANCE_ARN'],
    'migration-type': 'full-load-and-cdc',
    'table-mappings': '',
}

# Load the table names from the YAML file
with open('table_names.yml', 'r') as file:
    table_names = yaml.safe_load(file)

# Create the table mappings for each table
table_mappings = {
    'rules': []
}
for table_name in table_names:
    table_mapping = {
        'rule-type': 'selection',
        'rule-id': table_name,
        'rule-name': table_name,
        'object-locator': {
            'schema-name': 'tripletex',
            'table-name': table_name
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

# Create the replication task using AWS CLI
cmd = ['aws', 'dms', 'create-replication-task']
for key, value in rep_task_settings.items():
    cmd.extend(['--'+key, value])


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    argParser.add_argument('--noop', action='store_true')

    args = argParser.parse_args()
    if args.noop:
        print(cmd)
    else:
        subprocess.run(cmd)

