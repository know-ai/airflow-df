from collections import namedtuple
import pandas as pd
import numpy as np
from types import GeneratorType
from pymongo import MongoClient

class DataLake:
    """
    Reads data lake files. 
    """
    __file = namedtuple("File", "title tpl_df genkey tpl_serialized meta")

    def __init__(self, mongo_user: str, mongo_password: str, host: str, port:int = 27017):
        self.__mongo_user = mongo_user
        self.__mongo_password = mongo_password
        self.__host = host
        self.__port = port
        self.__query = {}
        MONGO_URI = f"mongodb://{self.__mongo_user}:{self.__mongo_password}@{self.__host}:{self.__port}/"

        client = MongoClient(MONGO_URI, maxPoolSize = 5)
        db = client['iDetectFugas']
        self.info_case = db['info_case']
    
    def group_olga_files(self, title, tpl_df, genkey: dict, tpl_serialized:dict, meta: dict)->namedtuple:

            file = self.__file(title, tpl_df, genkey, tpl_serialized, meta)
            return file
    
    def read_file(self, case):
        
        if('title' not in case):
            raise ValueError(f'There is a case without title.')

        elif('tpl' not in case):
            raise ValueError(f'There is no TPL in case {case["title"]}.')

        elif('genkey' not in case):
            raise ValueError(f'There is no genkey in case {case["title"]}.')


        title = case['title']
        tpl_df = pd.DataFrame(case['tpl']['data'])
        tpl_serialized = case['tpl']
        genkey = case['genkey']

        del case['tpl']
        del case['genkey']
        if 'date_added' in case:
            del case['date_added']
        if 'date_updated' in case:
            del case['date_updated']
        if '_id' in case:
            del case['_id']

        meta = case
        if('info' in case['tpl']):
            if('date' in case['tpl']['info']):
                meta['date'] = case['tpl']['info']['date']

        return self.group_olga_files(title, tpl_df, genkey, tpl_serialized, meta)

    def read(
            self,
            **kwargs
            )->GeneratorType:

        self.query_maker(**kwargs)
                  
        are_there_cases = True
        counter = 0
        while are_there_cases:

            case_found = self.info_case.find(self.__query).sort('_id').skip(counter).limit(1)
            case_found = list(case_found)
            counter += 1

            if case_found is None or len(case_found) == 0:
                are_there_cases = False

            if are_there_cases:
                for case in case_found:
                    yield self.read_file(case)

    def query_maker(
            self,
            project=None, 
            terminal=None, 
            line=None, 
            fluid=None, 
            failure=None, 
            operation_state=None, 
            control_vout_sp=None,
            control_vin_sp=None,
            tkin_temperature=None,
            control_tkin_sp=None,
            control_tkout_sp=None,
            leak_size=None,
            leak_location=None,
            stroke=None,
            title=None
            ):
        
        query = {}

        if project:
            query['project'] = project
 
        if title:
            query['title'] = title
 
        if control_vin_sp:
            query['control_vin_sp'] = control_vin_sp
 
        if control_vout_sp:
            query['control_vout_sp'] = control_vout_sp

        if tkin_temperature:
            query['tkin_temperature'] = tkin_temperature

        if control_tkin_sp:
            query['control_tkin_sp'] = control_tkin_sp

        if control_tkout_sp:
            query['control_tkout_sp'] = control_tkout_sp

        if leak_size:
            query['leak_size'] = leak_size

        if leak_location:
            query['leak_location'] = leak_location

        if stroke:
            query['stroke'] = stroke

        if terminal:
            query['terminal'] = terminal

        if line:
            query['line'] = line

        if fluid:
            query['fluid'] = fluid

        if failure:
            query['failure'] = failure

        if operation_state:
            query['operation_state'] = operation_state

        self.__query = query



