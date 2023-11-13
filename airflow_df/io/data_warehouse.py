from collections import namedtuple
import pandas as pd
import numpy as np
from types import GeneratorType
from pymongo import MongoClient
import requests
import json

class DataWarehouse:
    """
    Reads data lake files. 
    """
    __file = namedtuple("File", "title tpl_df genkey tpl_serialized meta")

    def __init__(self, dw_user: str, dw_password:str, host: str = '3.139.233.232', port:int = 5432, db_name:str = 'warehouse', engine:str = 'postgresql'):
        self.__dw_user = dw_user
        self.__dw_password = dw_password
        self.__host = host
        self.__port = port
        self.__db_name = db_name
        self.__engine = engine

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'engine': engine,
            'host': host,
            'port': port,
            'user': dw_user,
            'password': dw_password,
            'db_name': db_name,
        }
        try:
            response = requests.post(f'http://{host}:5050/api/server/connect', headers=headers, json=json_data, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            # Handle connection errors, timeouts, or other request-related exceptions
            print(f"Request failed: {e}")
            raise e
        
        except Exception as e:
            # Handle any other unexpected exceptions
            print(f"An error occurred: {e}")
            raise e

    def save_timestamp(self, timestamp):
        response = self.__check_and_create_timestamp(timestamp)
        return response
    
    def save_and_create_case(self, leak_size, leak_location, fluid, stroke, operation_condition, failure_type, line, terminal, title):

        self.__check_and_create_leak_size(leak_size)
        self.__check_and_create_leak_location(leak_location)
        self.__check_and_create_fluid(fluid)
        self.__check_and_create_stroke(stroke)
        self.__check_and_create_operation_condition(operation_condition)
        self.__check_and_create_failure_type(failure_type)
        self.__check_and_create_terminal(terminal)
        self.__check_and_create_line(line, terminal)

        self.__create_cases(leak_size, leak_location, operation_condition, fluid, stroke, failure_type, line, terminal, title)

    def save_tags_and_send_blobs(self, tpl_df: pd.DataFrame, title: str):
        
        timestamp_list = []
        for i in pd.to_datetime(tpl_df['TIME_SERIES_S'], unit='s').items():
            timestamp_list.append(i[1].strftime('%Y/%m/%d %H:%M:%S.%f'))

        total_value_tags = []
        for i in tpl_df.columns:

            tag_values = {}            
            if('Pressure_PA' in i):
                tag_values['tag'] = {}
                tag_values['tag']['name'] = i
                tag_values['tag']['unit']= 'Pa'
                position = int(i.split('@')[-1].split('M')[0])
                tag_values['tag']['description']= f'Pressure in Pascal at position {position}'

            
            elif('Total_mass_flow_KG/S' in i):
                tag_values['tag'] = {}
                tag_values['tag']['name'] = i
                tag_values['tag']['unit']= 'kg/s'
                position = int(i.split('@')[-1].split('M')[0])
                tag_values['tag']['description']= f'Mass flow in kilogram second at position {position}'
            
            elif('Fluid_temperature_C' in i):
                tag_values['tag'] = {}
                tag_values['tag']['name'] = i
                tag_values['tag']['unit']= 'C'
                position = int(i.split('@')[-1].split('M')[0])
                tag_values['tag']['description']= f'Temperature in Celsius Degrees at position {position}'
                
            elif('Oil_density_KG/M3' in i):
                tag_values['tag'] = {}
                tag_values['tag']['name'] = i
                tag_values['tag']['unit']= 'kg/m3'
                position = int(i.split('@')[-1].split('M')[0])
                tag_values['tag']['description']= f'Mass desnity in kilogram cubic meter at position {position}'
            
            if('tag' in tag_values):
                tag_values['tag']['data_type']= 'float'
                tag_values['tag']['display_name']= i

                tag_values['values'] = []
                
                for j in range(len(timestamp_list)):
                    tag_values['values'].append({'timestamp':timestamp_list[j], 'tag': i, 'case': title, 'value': tpl_df[i][j]})

                total_value_tags.append(tag_values)
        
        self.send_bulk_tag_values_blob(total_value_tags)

    def __check_and_create_timestamp(self, timestamp:str):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'timestamp': timestamp,
        }

        response = requests.post('http://localhost:5050/api/simulation-timestamp/find-by-timestamp', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:
                    
                    response = requests.post('http://localhost:5050/api/simulation-timestamp/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()

                    print(f'Timestamp {timestamp} created successfully')
                    return response.json()['data']
                except Exception as e:
                    raise e
        else:
            print(f'Timestamp {timestamp} already exists')
            return response.json()

    def __check_and_create_leak_size(self, leak_size: float):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'value': leak_size,
        }

        response = requests.post('http://localhost:5050/api/leak_sizes/find-by-value', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/leak_sizes/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Leak Size {leak_size} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for leak size '{leak_size}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for leak size '{leak_size}': {e}")
                    raise e

        else:
            print(f'Leak Size {leak_size} already exists')
            return response.json()

    def __check_and_create_leak_location(self, leak_location: float):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'value': leak_location,
        }


        response = requests.post('http://localhost:5050/api/leak_locations/find-by-value', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/leak_locations/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Leak location {leak_location} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for leak location '{leak_location}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for leak location '{leak_location}': {e}")
                    raise e

        else:
            print(f'Leak location {leak_location} already exists')
            return response.json()

    def __check_and_create_fluid(self, fluid: str):
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'name': fluid,
        }


        response = requests.post('http://localhost:5050/api/fluids/find-by-name', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/fluids/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Fluid {fluid} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for fluid '{fluid}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for fluid '{fluid}': {e}")
                    raise e

        else:
            print(f'Fluid {fluid} already exists')
            return response.json()

    def __check_and_create_stroke(self, stroke:float):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'value': stroke
        }


        response = requests.post('http://localhost:5050/api/strokes/find-by-value', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:
                    json_data['unit'] = 's'
                    response = requests.post('http://localhost:5050/api/strokes/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Stroke {stroke} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for stroke '{stroke}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for stroke '{stroke}': {e}")
                    raise e

        else:
            print(f'Stroke {stroke} already exists')
            return response.json()

    def __check_and_create_operation_condition(self, operation_condition: str):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'name': operation_condition,
        }


        response = requests.post('http://localhost:5050/api/operation-conditions/find-by-name', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/operation-conditions/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Operation condition {operation_condition} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for operation condition '{operation_condition}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for operation condition '{operation_condition}': {e}")
                    raise e

        else:
            print(f'Operation condition {operation_condition} already exists')
            return response.json()

    def __check_and_create_line(self, line: str, terminal: str):
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'name': line,
            'terminal_abbreviation': terminal
        }


        response = requests.post('http://localhost:5050/api/lines/find-by-name-and-terminal-abbreviation', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/lines/create-by-terminal-abbreviation', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Line {line} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for line '{line}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for line '{line}': {e}")
                    raise e

        else:
            print(f'Line {line} already exists')
            return response.json()

    def __check_and_create_terminal(self, terminal:str):
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'abbreviation': terminal
        }

        response = requests.post('http://localhost:5050/api/terminals/find-by-abbreviation', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:
                    json_data['abbreviation'] = terminal[:2]
                    response = requests.post('http://localhost:5050/api/terminals/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Terminal {terminal} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for terminal '{terminal}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for terminal '{terminal}': {e}")
                    raise e

        else:
            print(f'Terminal {terminal} already exists')
            return response.json()

    def __check_and_create_failure_type(self, failure_type:str):

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'name': failure_type,
        }


        response = requests.post('http://localhost:5050/api/failure-types/find-by-name', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/failure-types/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Failure type {failure_type} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for failure type '{failure_type}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for failure type '{failure_type}': {e}")
                    raise e

        else:
            print(f'Failure type {failure_type} already exists')
            return response.json()

    def __create_cases(
            self,
            leak_size: float, 
            leak_location: float, 
            operation_condition: str, 
            fluid: str, 
            stroke: float, 
            failure_type: str, 
            line: str,
            terminal: str,
            title: str
        ):
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            "name": title
        }

        response = requests.post('http://localhost:5050/api/cases/find-by-name', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:
                    
                    json_data = {
                        "name": title,
                        "leak_size": leak_size,
                        "leak_location": leak_location,
                        "operation_condition": operation_condition,
                        "fluid": fluid,
                        "stroke": stroke,
                        "failure_type": failure_type,
                        "line": line,
                        "terminal_abbreviation": terminal
                    }
                    
                    response = requests.post('http://localhost:5050/api/cases/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Case {title} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for case '{title}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for case '{title}': {e}")
                    raise e

        else:
            print(f'Case {title} already exists')
            return response.json()

    def send_bulk_tag_values_blob(self, blob: list):
        url = 'http://localhost:5050/api/simulation-tag-values/blob-multiple-tags-and-values'  # Replace with the appropriate URL
        
        blob = json.dumps(blob)	
        blob = blob.encode('utf-8')
        
        try:
            headers = {'Content-Type': 'application/octet-stream'}
            response = requests.post(url, headers=headers, data=blob, timeout=100)

            if response.status_code == 200:
                print('Blob sent successfully.')
            else:
                print('Failed to send blob. Status Code:', response.status_code)
        except requests.exceptions.RequestException as e:
            
            raise Exception('Error sending tag values blob:', str(e))

    def send_bulk_simulation_timestamps_blob(self, blob:list):

        url = 'http://localhost:5050/api/simulation-timestamp/blob'  # Replace with the appropriate URL
        
        blob = json.dumps(blob)	
        blob = blob.encode('utf-8')

        try:
            headers = {'Content-Type': 'application/octet-stream'}
            response = requests.post(url, headers=headers, data=blob, timeout=100)

            if response.status_code == 200:
                print('Blob sent successfully.')
            else:
                print('Failed to send blob. Status Code:', response.status_code)
        except requests.exceptions.RequestException as e:

            raise Exception('Error sending simulation timestamps blob:', str(e))