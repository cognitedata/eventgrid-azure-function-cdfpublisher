# eventgrid-azure-function-cdfpublisher
This function is a sample on how to push datapoints to CDF using an Azure Function which is triggered by a message on IOT HUB.

The specific implementation can be used to extend the HOWTO [Ingesting OPC UA data with Azure Digital Twins](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-ingest-opcua-data?tabs=cli) 
to store historic time series data in CDF in addition to the snapshot in Azure Digital Twin.

CDF credentials are fetched from environment variables, make sure to set COGNITE_PROJECT, CLIENT_ID, CLIENT_SECRET, TENANT_ID and BASE_URL when deploying the function
![function-configuration](https://user-images.githubusercontent.com/51971968/158560021-cd051df6-68b7-48bd-9a6d-c7a65ca7dcf8.png)

The function should be triggered by IOT HUB by setting up an [EventGrid trigger](https://docs.microsoft.com/en-us/azure/digital-twins/how-to-ingest-opcua-data?tabs=cli#create-event-subscription)


