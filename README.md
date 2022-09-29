# python-knowledge-connector

This is a suite of utilities to sync entities between Senzing and Knowledge Graph

## Dependencies
senzing -- Senzing's environment must be sourced.  If senzing is installed into /home/user, the environment can be sourced as follows
```
. /home/user/senzing/setupEnv
```

This utility will automatically use the senzing configuration associated with your installation.

If you don't already have Senzing installed, instructions can be found here [Senzing quickstart guide](https://senzing.zendesk.com/hc/en-us/articles/115002408867-Quickstart-Guide)

pika -- RabbitMQ Client must be installed.  It can be installed with Python pip with the following command.

```
sudo pip install pika
```

## process_entities.py

This utility reads work items from RabbitMQ and syncs entities and relationships in Knowledge Graph based on the queued work items.:w

## data_model_util.py

This is a utility to create, delete, and print out Knowledge "named types" data models.  

Named types are configured in a CSV file.  A sample file can be found in entity_data_model.csv.SAMPLE

## clean_entities.py

This utility will delete all entities of a type.  Used to empty the Knowledge Graph between iterations.

