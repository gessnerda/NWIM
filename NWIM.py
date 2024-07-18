import requests
import csv
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

log_file = "logger.log"
unit_mapping_file = "unit_mapping.json"

def load_config(config_file="config.json"):
    if not os.path.exists(config_file):
        log_message(f"Config file {config_file} does not exist.")
        return None
    with open(config_file, "r") as file:
        return json.load(file)

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a") as file:
        file.write(f"{timestamp} - {message}\n")

def fetch_data(url, headers):
    try:
        log_message(f"Fetching data from URL: {url}")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log_message(f"Request failed for URL: {url}. Error: {e}")
        return None

def is_valid_lat_long(incident):
    try:
        float(incident.get('latitude'))
        float(incident.get('longitude'))
        return True
    except (TypeError, ValueError):
        log_message(f"No valid lat long for incident: {incident.get('uuid')} Name: {incident.get('name')}")
        return False

def is_recent_incident(incident_date, age_limit_months):
    try:
        try:
            incident_datetime = datetime.strptime(incident_date, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            incident_datetime = datetime.strptime(incident_date, "%Y-%m-%dT%H:%M:%S")
        
        now = datetime.now()
        age_limit = now - relativedelta(months=age_limit_months)
        return incident_datetime > age_limit
    except ValueError:
        return False

def get_status_code(fire_status):
    fire_out = fire_status.get('out')
    if fire_out:
        return "Avail"
    else:
        return "OnScene"

def process_incident(incident, center_code, agency, age_limit_months):
    incident_id = incident.get('uuid')
    if not incident_id:
        return None

    if not is_recent_incident(incident.get('date'), age_limit_months):
        return None

    fiscal_data = incident.get('fiscal_data', {})
    if isinstance(fiscal_data, str):
        try:
            fiscal_data = json.loads(fiscal_data)
        except json.JSONDecodeError:
            fiscal_data = {}
            log_message(f"Failed to decode fiscal_data JSON for incident id {incident_id}")

    fire_status = incident.get('fire_status', {})
    if isinstance(fire_status, str):
        try:
            fire_status = json.loads(fire_status)
        except json.JSONDecodeError:
            fire_status = {}
            log_message(f"Failed to decode fire_status JSON for incident id {incident_id}")

    # Add negative sign to longitude if not already there
    longitude = incident.get('longitude', '')
    if longitude and not longitude.startswith('-'):
        longitude = '-' + longitude

    # Remove returns from webComment and fiscal_comments fields
    web_comment = incident.get('webComment', '') or ''
    web_comment = web_comment.replace('\n', ' ').replace('\r', ' ')
    
    fiscal_comments = fiscal_data.get('fiscal_comments', '') or ''
    fiscal_comments = fiscal_comments.replace('\n', ' ').replace('\r', ' ')

    # Determine the new incidentTypeDescription based on fire status
    fire_contain = fire_status.get('contain')
    fire_control = fire_status.get('control')
    fire_out = fire_status.get('out')

    if fire_out is not None:
        clear_datetime = fire_out
    else:
        clear_datetime = None

    if fire_contain and not fire_control and not fire_out:
        incident_type_description = "Wildfire Contained"
    elif fire_contain and fire_control and not fire_out:
        incident_type_description = "Wildfire Controlled"
    else:
        incident_type_description = incident.get('type', '')

    processed_incident = {
        "agency": agency,
        "jurisdiction": center_code,
        "incidentId": incident.get('uuid'),
        "alternateId": incident.get('inc_num'),
        "incidentTypeDescription": incident_type_description,
        "latitude": incident.get('latitude'),
        "longitude": longitude,
        "statusUpdatedDatetime": incident.get('date', 'Unknown'),
        "clearDatetime": clear_datetime,
        "narrative": web_comment,
        "name": incident.get('name'),
        "ic": incident.get('ic'),
        "acres": incident.get('acres'),
        "fuels": incident.get('fuels'),
        "fire_out": fire_status.get('out'),
        "fire_contain": fire_status.get('contain'),
        "fire_control": fire_status.get('control'),
        "fire_code": fiscal_data.get('fire_code'),
        "wfdssunit": fiscal_data.get('wfdssunit'),
        "fs_job_code": fiscal_data.get('fs_job_code'),
        "fs_override": fiscal_data.get('fs_override'),
        "fiscal_comments": fiscal_comments,
        "state_fiscal_code": fiscal_data.get('state_fiscal_code'),
        "fire_status": fire_status  
    }

    return processed_incident

def is_old_prescribed_fire(incident):
    if incident.get('type') == "Prescribed Fire":
        try:
            incident_datetime = datetime.strptime(incident.get('date'), "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            incident_datetime = datetime.strptime(incident.get('date'), "%Y-%m-%dT%H:%M:%S")
        
        now = datetime.now()
        ninety_days_ago = now - relativedelta(days=90)
        return incident_datetime < ninety_days_ago
    return False

def get_next_unit_id(available_units, unit_mapping):
    if available_units:
        return available_units.pop(0)
    used_ids = set(unit_mapping.values())
    i = 1
    while f"FixedUnit{i}" in used_ids:
        i += 1
    return f"FixedUnit{i}"

def process_center_data(center_code, center_response, all_center_data, agency, age_limit_months, unit_mapping, available_units):
    current_uuids = set()
    incident_dict = {}
    if isinstance(center_response, list):
        for center_data in center_response:
            if 'data' not in center_data or not isinstance(center_data['data'], list):
                continue

            filtered_incidents = [
                incident for incident in center_data['data']
                if incident['type'] not in {"Miscellaneous", "Resource Order", "Aircraft", "False Alarm", "Classroom Training", "Preparedness/Preposition", "N/A", "Resource Program (internal)", "Emergency Stabilization","Nonstatistical Fire"}
                and is_valid_lat_long(incident)
                and not is_old_prescribed_fire(incident)
            ]

            if center_code not in all_center_data:
                all_center_data[center_code] = []

            for incident in filtered_incidents:
                processed_incident = process_incident(incident, center_code, agency, age_limit_months)
                if processed_incident:
                    current_uuids.add(processed_incident["incidentId"])
                    all_center_data[center_code].append(processed_incident)
                    incident_dict[processed_incident["incidentId"]] = processed_incident

                    # Update or create unit mapping
                    if processed_incident["incidentId"] not in unit_mapping:
                        unit_mapping[processed_incident["incidentId"]] = get_next_unit_id(available_units, unit_mapping)
                    else:
                        # If the incident has clearDatetime, release the unit for reuse
                        if processed_incident["clearDatetime"] is not None:
                            available_units.append(unit_mapping[processed_incident["incidentId"]])
                            del unit_mapping[processed_incident["incidentId"]]

    return current_uuids, incident_dict

def generate_unit_data(agency, unit_id, incident_id, status_code, latitude, longitude, status_updated_datetime, gpsFixDatetime):
    return {
        "agency": agency,
        "unitId": unit_id,
        "incidentId": incident_id,
        "statusCode": status_code,
        "latitude": latitude,
        "longitude": longitude,
        "statusUpdatedDatetime": status_updated_datetime,
        "gpsFixDatetime": gpsFixDatetime
    }

def save_to_txt(center_code, data, file_prefix):
    if not data:
        log_message(f"No data to save for {center_code}.")
        return None
    
    keys = data[0].keys()
    directory = os.path.join("DC", center_code)
    os.makedirs(directory, exist_ok=True)
    file_name = os.path.join(directory, f"{file_prefix}_{center_code}.txt")
    
    try:
        with open(file_name, "w", newline='') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter='\t')
            dict_writer.writeheader()
            dict_writer.writerows(data)
        log_message(f"Data saved to {file_name}")
    except IOError as e:
        log_message(f"Error: Could not write to {file_name}. Error: {e}")
        return None
    return file_name

def save_to_json(data, file_name):
    try:
        directory = "fetched"
        os.makedirs(directory, exist_ok=True)
        file_name = os.path.join(directory, file_name)
        
        with open(file_name, "w") as json_file:
            json.dump(data, json_file, indent=4)
        log_message(f"Fetched data saved to {file_name}")
    except IOError as e:
        log_message(f"Error: Could not write to {file_name}. Error: {e}")

def load_unit_mapping():
    if os.path.exists(unit_mapping_file):
        with open(unit_mapping_file, "r") as file:
            return json.load(file)
    return {}

def save_unit_mapping(unit_mapping):
    with open(unit_mapping_file, "w") as file:
        json.dump(unit_mapping, file, indent=4)

def send_to_dc_api(incident_file, unit_file, dc_api_url, dc_api_key, dc_api_secret):
    if not incident_file or not unit_file:
        log_message("No files to send to DC API.")
        return
    
    files = {
        'incidents': open(incident_file, 'rb'),
        'units': open(unit_file, 'rb')
    }
    data = {'type': 'sitstat', 'agency': 'CoStateTest'}
    
    try:
        response = requests.post(dc_api_url, auth=(dc_api_key, dc_api_secret), files=files, data=data)
        if response.status_code == 200:
            log_message('Files sent to DC API successfully')
        else:
            log_message(f'Something happened. Check out the problem: {response.status_code}')
    except Exception as e:
        log_message(f"Error while sending files to DC API: {e}")

def main():
    config = load_config()
    if not config:
        return

    center_codes = config.get("center_codes", [])
    api_link = config.get("api_link")
    headers = config.get("headers")
    agency = config.get("agency")
    dc_api_key = config.get("dc_api_key")
    dc_api_link = config.get("dc_api_link")
    dc_api_secret = config.get("dc_api_secret")
    age_limit_months = config.get("incident_age_limit_months", 2)
    
    if not center_codes or not api_link or not headers or not agency:
        log_message("Required configuration not found in config file.")
        return

    all_center_data = {}
    change_log = {}

    # Load the unit mapping and initialize available units
    unit_mapping = load_unit_mapping()
    available_units = []

    for center_code in center_codes:
        request_url = api_link.replace("{center_code}", center_code)
        center_response = fetch_data(request_url, headers)
        if not center_response:
            log_message(f"No data received for center code {center_code}. Moving to next center code.")
            continue
        
        log_message(f"Fetched data: {json.dumps(center_response)[:1000]}...")  # Log first 1000 characters of the response
        save_to_json(center_response, f"{center_code}_fetched_data.json")
        current_uuids, incident_dict = process_center_data(center_code, center_response, all_center_data, agency, age_limit_months, unit_mapping, available_units)
        total = len(current_uuids)
        change_log[center_code] = {'total': total}

        unit_data = []

        # Generate unit data for each incident
        for incident_id in current_uuids:
            unit_id = unit_mapping.get(incident_id)
            if not unit_id:
                unit_id = get_next_unit_id(available_units, unit_mapping)
                unit_mapping[incident_id] = unit_id
            
            status_code = get_status_code(incident_dict[incident_id].get("fire_status", {}))
            latitude = incident_dict[incident_id].get("latitude")
            longitude = incident_dict[incident_id].get("longitude")
            status_updated_datetime = incident_dict[incident_id].get("statusUpdatedDatetime")
            gpsFixDatetime = incident_dict[incident_id].get("statusUpdatedDatetime")
            unit = generate_unit_data(agency, unit_id, incident_id, status_code, latitude, longitude, status_updated_datetime, gpsFixDatetime)
            unit_data.append(unit)

        # Save the center's data to a TXT file
        if center_code in all_center_data and all_center_data[center_code]:
            save_to_txt(center_code, all_center_data[center_code], "Incidents")

        # Save unit data to a TXT file for each center code
        if unit_data:
            save_to_txt(center_code, unit_data, "Units")

        # Send the files to the DC API
        incident_file = f"DC/{center_code}/Incidents_{center_code}.txt"
        unit_file = f"DC/{center_code}/Units_{center_code}.txt"
        if os.path.exists(incident_file) and os.path.exists(unit_file):
            send_to_dc_api(incident_file, unit_file, dc_api_link, dc_api_key, dc_api_secret)

    # Save the updated unit mapping
    save_unit_mapping(unit_mapping)

    log_message(f"Changes log: {change_log}")

if __name__ == "__main__":
    main()