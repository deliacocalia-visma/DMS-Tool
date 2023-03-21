import yaml
import json
import subprocess

# Define the source and target endpoints
source_endpoint = 'arn:aws:dms:eu-west-1:124523898468:endpoint:GWGREXCRDWOPRYDQKRS7A4S2SM7NZADOCMV4VDA'
target_endpoint = 'arn:aws:dms:eu-west-1:124523898468:endpoint:EPBDPPQSIC76SYPPBHFN24XELWY3AX5ESD5J33Y'
replication_instance = 'arn:aws:dms:eu-west-1:124523898468:rep:YH2AHWJ5IAPUWXJ6NEG62B6ITUEC7BCKQ6RZDYY'

# Define the replication task settings
rep_task_settings = {
    'ReplicationTaskIdentifier': 'tripletex-to-globaldata',
    'SourceEndpointArn': source_endpoint,
    'TargetEndpointArn': target_endpoint,
    'ReplicationInstanceArn': replication_instance,
    'MigrationType': 'full-load-and-cdc',
    'TableMappings': ''
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
rep_task_settings['TableMappings'] = json.dumps(table_mappings)

# Create the replication task using AWS CLI
cmd = ['aws', 'dms', 'create-replication-task']
for key, value in rep_task_settings.items():
    cmd.extend(['--'+key, value])
subprocess.run(cmd)
