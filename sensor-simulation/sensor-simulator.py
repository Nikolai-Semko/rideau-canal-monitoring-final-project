import asyncio
import json
import random
import time
from datetime import datetime, timezone
from azure.iot.device.aio import IoTHubDeviceClient

# Azure IoT Hub connection strings for each device
# Replace these with your actual connection strings from Azure IoT Hub
DEVICE_CONNECTION_STRINGS = {
    "Dow's Lake": "HostName=rideauCanal-iot-hub.azure-devices.net;DeviceId=dows-lake-sensor;SharedAccessKey=7bN2bKvpS9FCmXIrgtIHCIuFVH6DoivamnGl3GyeOa0=",
    "Fifth Avenue": "HostName=rideauCanal-iot-hub.azure-devices.net;DeviceId=fifth-avenue-sensor;SharedAccessKey=Q5mAeIaugeByGUoe44Su4IQPLbDDCyGcJJE0zJIMXGo=",
    "NAC": "HostName=rideauCanal-iot-hub.azure-devices.net;DeviceId=nac-sensor;SharedAccessKey=N8RugUF01HCuXyGoStb3mE3pRT4J5X8LPSqqcTRHzE8="
}

# Sensor parameter ranges for realistic simulation
PARAMETER_RANGES = {
    "iceThickness": (15, 40),        # Ice thickness in cm (15cm to 40cm)
    "surfaceTemperature": (-15, 0),  # Surface temperature in °C (-15°C to 0°C)
    "snowAccumulation": (0, 15),     # Snow accumulation in cm (0cm to 15cm)
    "externalTemperature": (-20, 5)  # External temperature in °C (-20°C to 5°C)
}

async def simulate_sensor(location, connection_string):
    """Simulate a sensor at a specific location and send data to IoT Hub"""
    # Create an IoT Hub client for this device
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    await device_client.connect()
    
    print(f"Connected: {location} sensor")
    
    try:
        while True:
            # Generate random sensor values within realistic ranges
            ice_thickness = round(random.uniform(*PARAMETER_RANGES["iceThickness"]), 1)
            surface_temp = round(random.uniform(*PARAMETER_RANGES["surfaceTemperature"]), 1)
            snow_accum = round(random.uniform(*PARAMETER_RANGES["snowAccumulation"]), 1)
            external_temp = round(random.uniform(*PARAMETER_RANGES["externalTemperature"]), 1)
            
            # Create a UTC timestamp
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Create the telemetry message
            message = {
                "location": location,
                "iceThickness": ice_thickness,
                "surfaceTemperature": surface_temp,
                "snowAccumulation": snow_accum,
                "externalTemperature": external_temp,
                "timestamp": timestamp
            }
            
            # Convert the message to JSON
            message_json = json.dumps(message)
            
            # Send the message to IoT Hub
            await device_client.send_message(message_json)
            print(f"Sent from {location}: {message_json}")
            
            # Wait 10 seconds before sending the next reading
            await asyncio.sleep(10)
    
    except Exception as e:
        print(f"Error in {location} sensor: {e}")
    finally:
        # Disconnect the client
        await device_client.disconnect()
        print(f"Disconnected: {location} sensor")

async def main():
    """Start simulating all sensors concurrently"""
    print("Starting Rideau Canal Skateway sensor simulation...")
    
    # Create a list of tasks for each sensor location
    sensor_tasks = []
    for location, connection_string in DEVICE_CONNECTION_STRINGS.items():
        sensor_tasks.append(simulate_sensor(location, connection_string))
    
    # Run all sensor simulations concurrently
    await asyncio.gather(*sensor_tasks)

if __name__ == "__main__":
    # Run the main coroutine
    asyncio.run(main())
