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

        leak_size = self.__check_and_create_leak_size(leak_size)
        leak_location = self.__check_and_create_leak_location(leak_location)
        fluid = self.__check_and_create_fluid(fluid)
        stroke = self.__check_and_create_stroke(stroke)
        operation_condition = self.__check_and_create_operation_condition(operation_condition)
        failure_type = self.__check_and_create_failure_type(failure_type)
        line = self.__check_and_create_line(line, terminal)
        terminal = self.__check_and_create_terminal(terminal)

        self.__create_cases(leak_location, operation_condition, fluid, stroke, failure_type, line, terminal, title)

    def save_tags(self, tpl_columns: list) -> list:
        pressure_tags = self.__get_pressure_tag(tpl_columns)
        flow_tags = self.__get_flow_tag(tpl_columns)
        temperature_tags = self.__get_temperature_tag(tpl_columns)
        density_tags = self.__get_density_tag(tpl_columns)    

        total_tags = pressure_tags + flow_tags + temperature_tags + density_tags
        return total_tags

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
            'terminal': terminal
        }


        response = requests.post('http://localhost:5050/api/lines/find-by-name-and-terminal', headers=headers, json=json_data, timeout=30)
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:

                    response = requests.post('http://localhost:5050/api/lines/create', headers=headers, json=json_data, timeout=30)
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
            'name': terminal
        }

        response = requests.post('http://localhost:5050/api/terminals/find-by-name', headers=headers, json=json_data, timeout=30)
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

    def __check_and_create_tag(self, tag_name:str):
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
        }

        json_data = {
            'name': tag_name,
        }

        response = requests.post('http://localhost:5050/api/tags/find-by-name', headers=headers, json=json_data)
        
        position = int(tag_name.split('@')[-1].split('M')[0])
        if('message' in response.json()):
            if(response.json()['message']=='Not found'):
                try:
                    if('Pressure_PA' in tag_name):
                        json_data['unit']= 'Pa'
                        json_data['description']= f'Pressure in Pascal at position {position}'

                    elif('Fluid_temperature_C' in tag_name):
                        json_data['unit']= 'C'
                        json_data['description']= f'Temperature in Celsius Degrees at position {position}'

                    elif('Total_mass_flow_KG/S' in tag_name):
                        json_data['unit']= 'kg/s'
                        json_data['description']= f'Mass flow in kilogram second at position {position}'

                    elif('Oil_density_KG/M3' in tag_name):
                        json_data['unit']= 'kg/m3'
                        json_data['description']= f'Mass desnity in kilogram cubic meter at position {position}'

                    json_data['data_type']= 'float'
                    json_data['display_name']= tag_name


                    response = requests.post('http://localhost:5050/api/tags/create', headers=headers, json=json_data, timeout=30)
                    response.raise_for_status()  # Raise an exception if the request was unsuccessful (status code >= 400)

                    print(f'Tag {tag_name} created successfully')
                    return response.json()['data']

                except requests.exceptions.RequestException as e:
                    # Handle connection errors, timeouts, or other request-related exceptions
                    print(f"Request failed for tag '{tag_name}': {e}")
                    raise e
                except Exception as e:
                    # Handle any other unexpected exceptions
                    print(f"An error occurred for tag '{tag_name}': {e}")
                    raise e

        else:
            print(f'Tag {tag_name} already exists')
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
                        "terminal": terminal
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
        url = 'http://localhost:5050/api/simulation-tag-values/blob'  # Replace with the appropriate URL
        
        blob = json.dumps(blob)	
        blob.encode('utf-8')
        
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
        blob.encode('utf-8')

        try:
            headers = {'Content-Type': 'application/octet-stream'}
            response = requests.post(url, headers=headers, data=blob, timeout=100)

            if response.status_code == 200:
                print('Blob sent successfully.')
            else:
                print('Failed to send blob. Status Code:', response.status_code)
        except requests.exceptions.RequestException as e:

            raise Exception('Error sending simulation timestamps blob:', str(e))

    def __get_pressure_tag(self, columns:list)->list:
        pressure_tag = []
        for i in columns:
            if('Pressure_PA' in i):
                self.__check_and_create_tag(i)
                pressure_tag.append(i)
        return pressure_tag

    def __get_flow_tag(self, columns:list)->list:
        flow_tag = []
        for i in columns:
            if('Total_mass_flow_KG/S' in i):
                self.__check_and_create_tag(i)
                flow_tag.append(i)
        return flow_tag

    def __get_temperature_tag(self, columns:list)->list:
        temperature_tag = []
        for i in columns:
            if('Fluid_temperature_C' in i):
                self.__check_and_create_tag(i)
                temperature_tag.append(i)
        return temperature_tag

    def __get_density_tag(self, columns:list)->list:
        density_tag = []
        for i in columns:
            if('Oil_density_KG/M3' in i):
                self.__check_and_create_tag(i)
                density_tag.append(i)
        return density_tag
    

