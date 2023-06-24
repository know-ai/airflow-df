import os
import re


class Genkey(dict):
    """This class takes the genkey file and saves it into a Python dictionary.
    """

    def __init__(self, *args, **kwargs):
        self.__previous_line = None
        self.__previous_item = None
        self._keys = list()
        super().__init__(*args, **kwargs)

    def set_previous_item(self, item: str):

        self.__previous_item = item

    def get_previous_item(self) -> str:

        return self.__previous_item

    def set_previous_line(self, line: str):

        self.__previous_line = line

    def get_previous_line(self):

        return self.__previous_line

    def __append_key(self, key: str):

        if key not in self.__get_keys():

            self._keys.append(key)

    def __clean_keys(self):

        self._keys = list()

    def __clean_last_key(self):

        self._keys.pop(-1)

    def __get_keys(self):

        return self._keys

    @staticmethod
    def __clean_lines(lines: str) -> list:
        """Documentation here
        """
        # Append lines when it has \\
        _el = ''
        broken_lines = []
        for el in lines.split('\n'):
            if re.search('\\\\', el):
                if not _el:
                    _el = el
                    continue
                _el += el
                continue

            if el.find('\\\\') == -1 and _el and bool(el.strip()):
                _el += el
                _el = ' '.join([e.strip() for e in _el.split('\\')])
                broken_lines.append(_el.strip())
                _el = ''
                continue

            if bool(el.strip()):
                broken_lines.append(el.strip())

        # Append lines when it starts with third level key
        _el = ''
        fixed_lines = []
        second_key_pattern = re.compile(r'^[a-zA-Z]+\s\w+')
        third_key_pattern = re.compile(r'^[a-zA-Z]+\=|^[a-zA-Z]+\s\=')

        for line in broken_lines:
            if second_key_pattern.search(line):
                _el = line
                fixed_lines.append(line)
                continue

            if third_key_pattern.search(line):
                line = ' ' + line
                _el += line
                fixed_lines.append(_el)
                continue

        return fixed_lines

    @staticmethod
    def __split_elements(line: str) -> list:
        """Documentation here
        """
        _info = ''
        _el = ''
        clean_line = []
        flag = False
        second_key_pattern = re.compile(r'^[A-Z]+\s')
        opening_third_key_pattern_1 = re.compile(
            r'^[A-Z]+\=\(|^[A-Z]+\s\=\s\(')
        opening_third_key_pattern_2 = re.compile(r'^[A-Z]+\=\(')
        third_key_pattern = re.compile(r'^[A-Z]+\=')
        closing_third_key_pattern = re.compile(r'\)$|\)\s.+$')

        for n, el in enumerate(line.split(', ')):
            if second_key_pattern.search(el):
                splited_line = el.split(' ')
                clean_line.append(splited_line[0])
                second_key = ' '.join([e for e in splited_line[1:]])

                if opening_third_key_pattern_1.search(second_key):
                    _el = second_key
                    flag = True
                    continue
                el = second_key
                clean_line.append(el)
                continue

            if opening_third_key_pattern_2.search(el):
                _el = el
                flag = True
                continue

            if third_key_pattern.search(el):
                if n + 1 == len(line.split(', ')):
                    clean_line.append(el)
                    continue

                if re.search(r'^INFO', el):
                    _info = el
                    continue

                clean_line.append(el)
                continue

            if re.search(r'^INFO', _info):
                _info = _info + ', ' + el
                clean_line.append(_info)
                _info = ''
                continue

            if flag:
                el = ', ' + el
                _el += el

                if closing_third_key_pattern.search(el):
                    clean_line.append(_el)
                    _el = ''
                    flag = False

        return clean_line

    @staticmethod
    def __group_key_and_vals(keys: list, vals: list) -> list:
        return list(zip(keys, vals))

    def list_of_strings_2_dict(self, key_values: list):
        """Converts a list of strings into a dictionary.

        It takes a list of strings as follows:
            ['Key1=value', 'key2=value2', 'key3=value3']
        And returns a dictionary as follows:
            {'Key1':'value', 'key2':'value2', 'key3':'value3'}

    **Parameters**

        **key_values:** (list) List of strings. Each string represents a key-value pair separated by an equal sign.
        """
        # breakpoint()
        keys = [element.split('=')[0].strip() for element in key_values]
        vals = [element.split('=')[1].strip() for element in key_values]

        key_vals_list = self.__group_key_and_vals(keys=keys, vals=vals)
        self.__build_dictionary(key_vals_list=key_vals_list)

        THIRD_LEVEL_KEY_INFO_PATTERN = re.compile(r'INFO')
        THIRD_LEVEL_KEY_TERMINALS_PATTERN = re.compile(r'TERMINALS')
        THIRD_LEVEL_KEY_PVTFILE_PATTERN = re.compile(r'PVTFILE')

        THIRD_LEVEL_TUPLE_OF_STRINGS_VALUE_PATTERN = re.compile(
            r'\(\"\.\./|\(\"\w+')
        THIRD_LEVEL_STRING_TUPLE_VALUE_PATTERN = re.compile(r'\(\w+')
        # THIRD_LEVEL_NUMBER_PLUS_CHARACTER_VALUE_PATTERN = re.compile(
        #     r'\d+\ \W+')
        THIRD_LEVEL_NUMBERS_AND_NUMERIC_STRING_TUPLE_VALUE_PATTERN = re.compile(
            r'\d+\.\d+|^[0-9]*$|\(\d+')
        THIRD_LEVEL_NUMBER_PLUS_PHYSICS_UNIT = re.compile(
            r'\d\s\w|\d\)\s\w+|\d\)\s\%|\d\s\%|\(\"\w+|\d+\ \W+')

        for key, val in self.items():

            if THIRD_LEVEL_TUPLE_OF_STRINGS_VALUE_PATTERN.search(val):
                val = [e.replace('"', '').replace('(', '').replace(')', '').strip()
                       for e in val.split(',')]
                val = tuple(val)
                self[key] = val
                continue

            if THIRD_LEVEL_KEY_INFO_PATTERN.search(key):
                val = val.replace('"', '')
                self[key] = val
                continue

            if THIRD_LEVEL_KEY_PVTFILE_PATTERN.search(key):
                self[key] = val.replace('"', '')
                continue

            if THIRD_LEVEL_NUMBER_PLUS_PHYSICS_UNIT.search(val):
                if THIRD_LEVEL_KEY_TERMINALS_PATTERN.search(key):
                    val = val.replace('(', '').replace(')',
                                                       '').replace(',', '')
                    val = [e.strip() for e in val.split(' ')]
                    _val = []
                    _el = ''
                    n = 0
                    for el in val:
                        n += 1
                        if n == 1:
                            _el = el
                            continue

                        if n == 2:
                            el = ' ' + el
                            _el += el
                            _val.append(_el)
                            n = 0
                            continue
                    VALUE = tuple(_val)
                    self[key] = VALUE
                    continue

                val = val.strip().split(' ')
                VALUE = ' '.join([el for el in val[:-1]])
                UNIT = val[-1]
                plural = False
                VALUE = eval(VALUE)

                if isinstance(VALUE, tuple):
                    plural = True

                self[key] = {
                    f'VALUE{"S" if plural else ""}': VALUE,
                    'UNIT': UNIT.strip(',')
                }
                continue

            if THIRD_LEVEL_NUMBERS_AND_NUMERIC_STRING_TUPLE_VALUE_PATTERN.search(val):
                self[key] = eval(val)
                continue

            if THIRD_LEVEL_STRING_TUPLE_VALUE_PATTERN.search(val):
                val = val.strip('(').strip(')')
                val = [el.strip() for el in val.split(',') if el]
                self[key] = tuple(val)
                continue

            # if THIRD_LEVEL_NUMBER_PLUS_CHARACTER_VALUE_PATTERN.search(val):
            #     val = val.strip().split()
            #     VALUE = eval(val[0])
            #     UNIT = val[-1]
            #     self[key] = {
            #         'VALUE': VALUE,
            #         'UNIT': UNIT
            #     }
            #     continue

            self[key] = val.replace('"', '')

        return self.copy()

    def __build_dictionary(self, key_vals_list: list) -> dict:
        """Builds a dictionary from a list containing tuples with keys and values. Returns a dictionary.

    **Parameters**

        **key_vals_list:** (list) List of tuples. The first element is the key and the second one its value.
        """
        self.clear()

        # Creating list of second level keys for duplicated first level keys
        for key in key_vals_list:
            self.setdefault(key[0], []).append(key[1])

        # Extracting second level keys from list if first level key is not duplicated.
        for key, val in self.items():
            if len(val) == 1:
                self[key] = self.get(key)[0]

        for key, val in self.items():
            if val == {}:
                self[key] = None

        return self

    def __get_second_level_dictionary(self, dict_elements: tuple) -> dict:
        """Builds a dictionary from a list containing tuples with keys and values. Returns a dictionary.

    **Parameters**

        **dict_elements:** (list) List of tuples. The first element is the key and the second one its value.
        """
        # Convert list of strings into a list of dictionaries
        values = list(map(self.list_of_strings_2_dict,
                          dict_elements[1]))

        key_vals_list = self.__group_key_and_vals(
            keys=dict_elements[0], vals=values)

        return self.__build_dictionary(key_vals_list=key_vals_list)

    def __get_second_level_key_val_lists(self, lines: list) -> tuple:
        """Splits a list of lines of genkey text and saves them into two lists. 
        Returns a tuple with each value are the lists.
        The first one is the genkey's second-level keys, and the second one is its values.

    **Parameters**

        **lines:** (list) List of lines of the text block belonging to the genkey's first-level key. 
        """
        elements = list(map(self.__split_elements, lines))
        second_level_keys = [el[0] for el in elements]
        second_level_values = [el[1:] for el in elements]

        return second_level_keys, second_level_values

    @staticmethod
    def __clean_empty_spaces(string: str, join_by: str = '') -> str:

        return join_by.join([character.strip() for character
                            in string.split(join_by)])

    @staticmethod
    def __read_file(filepath: str) -> str:

        with open(filepath, 'r') as file:
            genkey_file = file.read()

        return genkey_file

    def read(self, filepath: str):
        """Reads and process a genkey file. Retunrs a Genkey object.

    **Parameters**

        **filepath:** (str) Path to the file.
        """
        assert isinstance(
            filepath, str), f'filepath must be a string! Not {type(filepath)}'

        file = self.__read_file(filepath=filepath)

        GENKEY_PRINCIPAL_ELEMENT_PATTERN = re.compile(r'\s\n')
        GENKEY_FIRST_LEVEL_KEY_PATTERN = re.compile(r'!\s\w+.+')

        # Splitting Genkey in principal elements
        genkey_elements = []
        for element in GENKEY_PRINCIPAL_ELEMENT_PATTERN.split(file):
            genkey_elements.append(element)

        first_level_keys = []
        second_level_keys_and_vals = []

        for element in genkey_elements:

            # Finding first-level keys in the genkey file's splitted line
            genkey_element = self.__clean_empty_spaces(
                string=element, join_by=' ')
            first_level_key = GENKEY_FIRST_LEVEL_KEY_PATTERN.search(
                genkey_element)

            if first_level_key:

                # Remove '!' from the begining of first-level key string.
                # That's because it is distinctive of them.
                first_level_key = first_level_key.group().replace('!', '').strip()
                first_level_keys.append(first_level_key)

                lines = self.__clean_lines(element)

                # Convert each line into a dictionary
                second_level_elements = self.__get_second_level_key_val_lists(
                    lines=lines)
                second_level_dict = self.__get_second_level_dictionary(
                    dict_elements=second_level_elements)

                second_level_keys_and_vals.append(second_level_dict.copy())

        # Putting together first and second level keys
        genkey_keys = self.__group_key_and_vals(
            keys=first_level_keys, vals=second_level_keys_and_vals)

        return self.__build_dictionary(key_vals_list=genkey_keys)
