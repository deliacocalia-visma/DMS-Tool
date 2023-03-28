# DMS Replication Tool

This tool is designed to automate the creation and management of AWS DMS replication tasks. It reads a YAML file containing the names of tables that need to be replicated, and creates DMS replication tasks for each of them.

## Usage

The ``table_names.yml`` file contains the names of the tables that need to be replicated, as well as the direction of replication (either "to-globaldata" or "to-tripletex"). Here's an example:

```
table1: to-globaldata
table2: to-tripletex
table3: to-globaldata
```

In this example, table1 and table3 will be replicated from the source endpoint to the target endpoint, while table2 will be replicated from tripletex to globaldata, while table 2 will be replicated from globaldata to tripletex.

If you remove a table from the table_names.yml file, the corresponding DMS Task will be stopped, and a notification will be sent to inform the teams through Slack.

**! Important Note for Replication in the Other Direction** 

When creating a replication in the other direction, please note that you should not add the table name twice in the ``table_names.yml`` file. Instead, you should only change the direction of replication. It is important to remember that a table should be present only once in the table_names.yml file to avoid any errors during replication.

For example, if you have a table called ``currency`` that is being replicated from the source database ( tripletex ) to the target database ( globaldata ), and you want to replicate it from the target database ( globaldata ) to the source database ( tripletex ), you should update the ``table_names.yml`` file as follows:

```
currency: to-tripletex 
```

***DO NOT ADD** the table ( ex: ``currency`` ) twice in ``table_names.yml`` file.*

## Notifications

Note that the replication tool also comes with a *Slack* notification feature that sends messages when tasks are created or stopped. Additionally, if a table is no longer present in the ``table_names.yml`` file, the tool will send a notification message to alert you after the DMS Task is stopped.