#!/bin/bash
# This script sets up the required Azure resources for the Rideau Canal Skateway monitoring system
# Prerequisites: Azure CLI must be installed and you must be logged in (az login)

# Configuration
RESOURCE_GROUP="RideauCanalMonitoring"
LOCATION="canadacentral"  # Using Canada Central as it's closest to Ottawa
IOT_HUB_NAME="rideauCanal-iot-hub"
STORAGE_ACCOUNT_NAME="rideaucanaldata$(date +%s | tail -c 6)"  # Append timestamp to ensure uniqueness
STORAGE_CONTAINER_NAME="rideau-canal-data"
STREAM_ANALYTICS_JOB_NAME="rideau-canal-stream-analytics"

# Colors for better readability
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Creating Resource Group: $RESOURCE_GROUP${NC}"
az group create --name $RESOURCE_GROUP --location $LOCATION

echo -e "${YELLOW}Creating IoT Hub: $IOT_HUB_NAME${NC}"
az iot hub create --name $IOT_HUB_NAME --resource-group $RESOURCE_GROUP --sku S1

echo -e "${YELLOW}Creating IoT Hub devices...${NC}"
az iot hub device-identity create --hub-name $IOT_HUB_NAME --device-id "dows-lake-sensor"
az iot hub device-identity create --hub-name $IOT_HUB_NAME --device-id "fifth-avenue-sensor"
az iot hub device-identity create --hub-name $IOT_HUB_NAME --device-id "nac-sensor"

echo -e "${YELLOW}Creating Storage Account: $STORAGE_ACCOUNT_NAME${NC}"
az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --location $LOCATION --sku Standard_LRS --kind StorageV2

# Get storage account connection string
STORAGE_CONNECTION_STRING=$(az storage account show-connection-string --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --query connectionString -o tsv)

echo -e "${YELLOW}Creating Blob Storage Container: $STORAGE_CONTAINER_NAME${NC}"
az storage container create --name $STORAGE_CONTAINER_NAME --connection-string "$STORAGE_CONNECTION_STRING"

echo -e "${YELLOW}Creating Stream Analytics Job: $STREAM_ANALYTICS_JOB_NAME${NC}"
az stream-analytics job create --resource-group $RESOURCE_GROUP --name $STREAM_ANALYTICS_JOB_NAME --location $LOCATION --output-error-policy "Drop" --events-outoforder-policy "Adjust" --events-outoforder-max-delay 5 --events-late-arrival-max-delay 5

# Get IoT Hub connection string
IOT_HUB_CONNECTION_STRING=$(az iot hub connection-string show --hub-name $IOT_HUB_NAME --resource-group $RESOURCE_GROUP --query connectionString -o tsv)

echo -e "${YELLOW}Creating Stream Analytics Input (from IoT Hub)${NC}"
az stream-analytics input create --resource-group $RESOURCE_GROUP --job-name $STREAM_ANALYTICS_JOB_NAME --name "rideau-canal-input" --type Stream --datasource @- <<EOF
{
  "type": "Microsoft.Devices/IotHubs",
  "properties": {
    "iotHubNamespace": "$IOT_HUB_NAME",
    "sharedAccessPolicyName": "iothubowner",
    "sharedAccessPolicyKey": "$(az iot hub policy show --hub-name $IOT_HUB_NAME --name iothubowner --query primaryKey -o tsv)",
    "consumerGroupName": "\$Default",
    "endpoint": "messages/events"
  }
}
EOF

echo -e "${YELLOW}Creating Stream Analytics Output (to Blob Storage)${NC}"
az stream-analytics output create --resource-group $RESOURCE_GROUP --job-name $STREAM_ANALYTICS_JOB_NAME --name "rideau-canal-output" --datasource @- <<EOF
{
  "type": "Microsoft.Storage/Blob",
  "properties": {
    "storageAccounts": [
      {
        "accountName": "$STORAGE_ACCOUNT_NAME",
        "accountKey": "$(az storage account keys list --account-name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --query [0].value -o tsv)"
      }
    ],
    "container": "$STORAGE_CONTAINER_NAME",
    "pathPattern": "{date}/{time}",
    "dateFormat": "yyyy-MM-dd",
    "timeFormat": "HH-mm-ss",
    "authenticationMode": "ConnectionString",
    "serialization": {
      "type": "Json",
      "properties": {
        "encoding": "UTF8",
        "format": "LineSeparated"
      }
    }
  }
}
EOF

echo -e "${YELLOW}Setting Stream Analytics Query${NC}"
az stream-analytics job update --resource-group $RESOURCE_GROUP --name $STREAM_ANALYTICS_JOB_NAME --query @- <<EOF
-- Aggregate data for each location over a 5-minute tumbling window
SELECT
    location,
    System.Timestamp() AS windowEndTime,
    AVG(iceThickness) AS avgIceThickness,
    MAX(snowAccumulation) AS maxSnowAccumulation,
    AVG(surfaceTemperature) AS avgSurfaceTemperature,
    AVG(externalTemperature) AS avgExternalTemperature,
    COUNT(*) AS numberOfReadings
INTO
    [rideau-canal-output]
FROM
    [rideau-canal-input]
GROUP BY
    location,
    TumblingWindow(minute, 5)
EOF

echo -e "${YELLOW}Starting Stream Analytics Job${NC}"
az stream-analytics job start --resource-group $RESOURCE_GROUP --name $STREAM_ANALYTICS_JOB_NAME --output-start-mode JobStartTime

# Print device connection strings for use in the sensor simulator
echo -e "${GREEN}======= DEVICE CONNECTION STRINGS =======${NC}"
echo -e "${GREEN}Dow's Lake Sensor:${NC}"
az iot hub device-identity connection-string show --hub-name $IOT_HUB_NAME --device-id "dows-lake-sensor" --query connectionString -o tsv

echo -e "${GREEN}Fifth Avenue Sensor:${NC}"
az iot hub device-identity connection-string show --hub-name $IOT_HUB_NAME --device-id "fifth-avenue-sensor" --query connectionString -o tsv

echo -e "${GREEN}NAC Sensor:${NC}"
az iot hub device-identity connection-string show --hub-name $IOT_HUB_NAME --device-id "nac-sensor" --query connectionString -o tsv

echo -e "${GREEN}Setup completed successfully!${NC}"
echo -e "${YELLOW}Please copy the device connection strings above and update them in your sensor_simulator.py script.${NC}"
