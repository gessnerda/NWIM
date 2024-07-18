# National Wildfire Incident Manager (NWIM)

## Description

The National Wildfire Incident Manager (NWIM) is a Python-based system designed to manage and process wildfire incident data. NWIM fetches and processes incident data from multiple centers, assigns unique unit IDs to each incident, and ensures that unit IDs are reused efficiently. The system manages the lifecycle of units based on incident status, logs activities, structures output data into organized directories, and sends processed data to a designated API endpoint.

## Features

- Fetches incident data from specified centers
- Validates latitude and longitude of incidents
- Determines incident status and assigns unique unit IDs
- Reuses unit IDs once an incident is cleared
- Logs activities and errors
- Structures output data into organized directories
- Sends processed data to a designated API endpoint
