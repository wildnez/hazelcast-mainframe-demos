# Hazelcast for Mainframe
This is to demonstrate deploying and running of Hazelcast Platform on a Mainframe environment, IBM z/OS in this case, and making data available to applications running outside of mainframe ecosystem. The app runs fraud detection process on incoming financial transactions that arrive in the form of String that comprises of csv (comma separated values). 

The deployment itself does not distinguish between the environments as Hazelcast is flexible enough to run as is, on any infrastructure. It does, however, provide required configurable options to deploy on a desired environment, z/OS in this case. 

To read more about the business objectives of this demo, see here: TODO

## Architecture
To meet one of the objectives of making data available to external applications, the deployment uses 2 separate Platform clusters that run in parallel and remain connected with each other through WAN Replication. More on WAN Replication can be found at https://docs.hazelcast.org/docs/4.1.1/manual/html-single/index.html#wan-replication

Cluster 1 runs on z/OS and Cluster 2 on OpenShift Container Platform on IBM Cloud. 

> **_Note: You can choose any infrastructure, be it another cloud provider or on-prem or anything else, for Cluster 2_**

Cluster 1, when starts, reads some data from the underlying RDBMS DB2 and stores in distributed maps. These maps are WAN replicated i.e. as soon as the data gets stored in these maps, it is replicated to Cluster 2. This data is later used for fraud detection runs on incoming transactions.


### Cache Design

Each transaction string contains several fields including a transactionID, an accountID, a merchantID, the transaction amount and several others. Each transaction, after it is processed, is saved against an account ID that it belongs to.

The cluster creates 3 distributed maps:
* Account = this stores all transactions against their account ID
* Merchant = detailed information about several merchants
* RulesResult = result of fraud detection checks against the transaction ID 
> **_Note: All above 3 maps are WAN Replicated_**

Results of fraud detection are also placed in a distributed queue named `rules_result_string_queue` for extended use.


## Configuration
There is `FraudDetection.properties` in resources. All networking, WAN Replication and JDBC configurations go there. 
There are additional configurations for the number of accounts that are to be created, number of transactions per account and merchant count. 


## Launchers
There are multiple shell scripts available, here is the description of each of them:

`generateDatabase.sh` - run this first. This creates table schema and loads the tables with reference and transactional data. 

`startPlatformServer.sh` - starts one instance of Platform cluster and look for an already running cluster on ServerIP and ServerPort configured in properties. To launch multi-node cluster, run this multiple times on different machines/VMs.

## License Key
This application uses Enterprise features of Hazelcast, hence you will need enterprise keys to run this application. To obtain a license key, please visit https://hazelcast.com/get-started/ and place it in https://github.com/hazelcast/hazelcast-mainframe-demos/blob/6103a43bb6269e0ea7796400cd6bc7d6a2e38995/fraud-detection-mainframe-wan/src/main/java/com/hazelcast/certification/util/License.java#L5 before building the project.

