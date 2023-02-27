# Event-Hub-Throughput-Generator

## Description

Event Hub Throughput Generator provides an easy way to send a customized json payload at a specific throughput (events/sec) to an Azure event hub. 

## How to Run

The following commands should be executed from the Azure Cloud Shell at https://shell.azure.com using Bash. This will deploy the full tutorial environment with no additional configuration needed.

```
@Azure:~$ git clone https://github.com/bretamyers/Event-Hub-Throughput-Generator
@Azure:~$ cd Event-Hub-Throughput-Generator
```

Once you clone the repo, you'll need to edit the <i><b>config_user.toml</b></i> file to include the batch account and event hub connection details as well as specifying the desired throughput and run duration.

You can modify the json payload by editing the <i><b>SampleJSON.json</b></i>. Adding or removing any of the json keys. 

After the config_user.toml has been updated and the SampleJSON.json file altered to match the desired payload definition, run the following command to start the data gen process.

```
@Azure:~$ bash RunGen.sh 
```

## Data Generation Steps
1. Profile the SampleJSON.json file to identify the upper limit memory footprint of a single event.
2. Perform calculations to identify the max number of events that can be sent in a single payload/batch.
3. Perform calculations to identify the required number of nodes and number of events the node has to send per payload/batch.  
4. Create an Azure batch pool creating the number of nodes from the output from above and installing all the required modules.
5. Create an Azure batch job with a task per node passing in the payload size and target second that the node will send to event hub.
6. Create an Azure batch job to delete the pool once the job has completed.

