node-zabbix-mongodb
===================

This application ships mongodb metrics to a local zabbix trapper agent.

## Requirements ##
* nodejs 0.8+
* local zabbix trapper
* enabled REST interface on mongodb nodes (if you want to use HTTP)

## How to use ##
Write the hosts you want to monitor in a local hosts.txt file with one of the following formats:
```
hostname
hostname,name_in_zabbix
```

## How it works ##
The application queries information about the server status and (if applicable) its
replication information from the web interface (if USE_HTTP = true) or with the node-mongodb-native of the given hosts.

