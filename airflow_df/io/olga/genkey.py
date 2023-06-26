import os
import re
import ast
import numpy as np


class GenkeyRegex:
    """This class store regex needed to process the genkey file
    """
    GENKEY_PRINCIPAL_ELEMENT_PATTERN = re.compile(r'\s\n')
    GENKEY_LINE_CONTINUATION = re.compile('\\\\')

    GENKEY_FIRST_LEVEL_KEY_PATTERN = re.compile(r'!\s\w+.+')
    GENKEY_SECOND_LEVEL_KEY_PATTERN = re.compile(r'^[a-zA-Z]+\s\w+')
    GENKEY_THIRD_LEVEL_KEY_PATTERN = re.compile(r'^[a-zA-Z]+\=|^[a-zA-Z]+\s\=')

    THIRD_LEVEL_PARENTHESES_OPENING_VALUE_PATTERN = re.compile(
        r'^[A-Z]+\=\(|^[A-Z]+\s\=\s\(')
    THIRD_LEVEL_PARENTHESES_CLOSING_VALUE_PATTERN = re.compile(r'\)$|\)\s.+$')
    THIRD_LEVEL_KEY_INFO_PATTERN = re.compile(r'INFO')
    THIRD_LEVEL_KEY_TERMINALS_PATTERN = re.compile(r'TERMINALS')
    THIRD_LEVEL_KEY_PVTFILE_PATTERN = re.compile(r'PVTFILE')
    THIRD_LEVEL_TUPLE_OF_STRINGS_VALUE_PATTERN = re.compile(
        r'\(\"\.\./|\(\"\w+')
    THIRD_LEVEL_STRING_TUPLE_VALUE_PATTERN = re.compile(r'\(\w+')
    THIRD_LEVEL_NUMBERS_AND_NUMERIC_STRING_TUPLE_VALUE_PATTERN = re.compile(
        r'\d+\.\d+|^[0-9]*$|\(\d+')
    THIRD_LEVEL_NUMBER_PLUS_PHYSICS_UNIT = re.compile(
        r'\d\s\w|\d\)\s\w+|\d\)\s\%|\d\s\%|\(\"\w+|\d+\ \W+')


class Genkey(dict):
    """This class takes the genkey file and saves it into a Python dictionary.
    """

    def __init__(self, *args, **kwargs):
        self.regex = GenkeyRegex()
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

    def __clean_lines(self, lines: str) -> list:
        """This method converts a principal genkey-text block into a list of lines (strings) containing second and third-level keys and values.
        It removes the first level key.

    **Parameters**

        **lines:** (str) Principal genkey-text block.
        """
        # Append lines when it has \\
        lines = lines.split('\n')
        previous_element = ''
        complete_lines = []

        for element in lines:
            element = element.strip()
            if self.regex.GENKEY_LINE_CONTINUATION.search(element):
                if not previous_element:
                    previous_element = element
                    continue
                previous_element += element
                continue

            if previous_element and element:
                previous_element += element
                previous_element = self.clean_empty_spaces(
                    string=previous_element, join_by=' ', split_by='\\')
                complete_lines.append(previous_element)
                previous_element = ''
                continue

            if element:
                complete_lines.append(element)

        del lines

        third_level_key_elements = list(
            filter(self.regex.GENKEY_THIRD_LEVEL_KEY_PATTERN.match, complete_lines))

        if any(third_level_key_elements):
            complete_lines = self.__fix_lines_when_starts_with_3_lvl_key(
                lines=complete_lines)

            return complete_lines

        complete_lines = list(
            filter(self.regex.GENKEY_SECOND_LEVEL_KEY_PATTERN.match, complete_lines))

        return complete_lines

    def __fix_lines_when_starts_with_3_lvl_key(self, lines: list) -> list:
        previous_line = ''
        fixed_lines = []

        for line in lines:
            if self.regex.GENKEY_SECOND_LEVEL_KEY_PATTERN.search(line):
                previous_line = line
                fixed_lines.append(line)
                continue

            if self.regex.GENKEY_THIRD_LEVEL_KEY_PATTERN.search(line):
                line = ' ' + line
                previous_line += line
                fixed_lines.append(previous_line)
                continue

        return fixed_lines

    @staticmethod
    def check_if_last_element(element_position: int, element_list: list) -> bool:
        return element_position + 1 == len(element_list)

    def __split_line_elements(self, line: str) -> list:
        """This method split a genkey file line into its different elements.
        Returns a list containing a second-level key and third-level keys and values elements.

    **Parameters**

        **line:** (str) Line belonging to a principal genkey-text block. 
        """
        def separate_second_level_key(line_element: str, split_by=' ', join_by=' ') -> tuple:

            splitted_line = line_element.split(split_by)
            second_level_key = splitted_line[0]
            third_level_key_val = join_by.join(list(splitted_line[1:]))

            return second_level_key, third_level_key_val

        _info = ''
        previous_element = ''
        splitted_line = []
        flag = False

        line = line.split(', ')

        for element_position, line_element in enumerate(line):
            if self.regex.GENKEY_SECOND_LEVEL_KEY_PATTERN.search(line_element):
                second_level_key, third_level_key_val = separate_second_level_key(
                    line_element=line_element)
                splitted_line.append(second_level_key)

                if self.regex.THIRD_LEVEL_PARENTHESES_OPENING_VALUE_PATTERN.search(third_level_key_val):
                    previous_element = third_level_key_val
                    flag = True
                    continue

                line_element = third_level_key_val
                splitted_line.append(line_element)
                continue

            if self.regex.THIRD_LEVEL_PARENTHESES_OPENING_VALUE_PATTERN.search(line_element):
                previous_element = line_element
                flag = True
                continue

            if self.regex.GENKEY_THIRD_LEVEL_KEY_PATTERN.search(line_element):
                if self.check_if_last_element(element_position, line):
                    splitted_line.append(line_element)
                    continue

                if self.regex.THIRD_LEVEL_KEY_INFO_PATTERN.search(line_element):
                    _info = line_element
                    continue

                splitted_line.append(line_element)
                continue

            # TODO: Handle when key INFO has a string with ',' for example:
            # INFO="Modelo de parada, a partir del minuto 9 el sistema queda estable"
            # This block of code is currently not working.
            # Fix it or ask to not write the value of INFO in that fashion.
            if self.regex.THIRD_LEVEL_KEY_INFO_PATTERN.search(_info):
                _info = _info + ', ' + line_element
                splitted_line.append(_info)
                _info = ''
                continue

            if flag:
                line_element = ', ' + line_element
                previous_element += line_element

                if self.regex.THIRD_LEVEL_PARENTHESES_CLOSING_VALUE_PATTERN.search(line_element):
                    splitted_line.append(previous_element)
                    previous_element = ''
                    flag = False

        return splitted_line

    @staticmethod
    def remove_string_quotes(string: str) -> str:
        """Removes quotes from string.

    **Parameters**

        **string:** (str) String with quotes.
        """
        return string.replace('"', '')

    @staticmethod
    def group_key_and_vals(keys: list, vals: list) -> list:
        """Zips together dictionary keys and their values.
        Returns a list of tuples where the first element in each tuple is the key
        and the second one is its value.

    **Parameters**

        **keys:** (list) List of keys.
        **vals:** (list) List of values.
        """
        return list(zip(keys, vals))

    def __list_of_strings_2_dict(self, key_values: list):
        """Converts a list of strings into a dictionary.

        It takes a list of strings as follows:
            ['Key1=value', 'key2=value2', 'key3=value3']
        And returns a dictionary as follows:
            {'Key1':'value', 'key2':'value2', 'key3':'value3'}

    **Parameters**

        **key_values:** (list) List of strings. Each string represents a key-value pair separated by an equal sign.
        """
        keys = [element.split('=')[0].strip() for element in key_values]
        vals = [element.split('=')[1].strip() for element in key_values]

        key_vals_list = self.group_key_and_vals(keys=keys, vals=vals)
        third_level_dict = self.__build_dictionary(key_vals_list=key_vals_list)
        del key_vals_list, keys, vals

        def remove_parentheses_and_split(value: str, split_by: str = '') -> list:

            value = self.remove_string_quotes(value)

            return [element.replace('(', '').replace(')', '').strip()
                    for element in value.split(split_by)]

        def join_terminals_value_elements(value: list, sections_by_element: int = 2, join_by=' ') -> list:

            elements_number = int(len(value)/sections_by_element)
            value = np.array_split(value, elements_number)

            return list(map(join_by.join, value))

        def split_number_and_unit(value: str) -> tuple:

            val = value.strip().split(' ')
            value = ' '.join(list(val[:-1]))
            value = ast.literal_eval(value)
            unit = val[-1]

            return value, unit

        for key, val in third_level_dict.items():

            if self.regex.THIRD_LEVEL_TUPLE_OF_STRINGS_VALUE_PATTERN.search(val):
                val = remove_parentheses_and_split(value=val, split_by=',')
                val = tuple(val)
                third_level_dict[key] = val
                continue

            if self.regex.THIRD_LEVEL_KEY_INFO_PATTERN.search(key) or \
                    self.regex.THIRD_LEVEL_KEY_PVTFILE_PATTERN.search(key):
                val = self.remove_string_quotes(string=val)
                third_level_dict[key] = val
                continue

            if self.regex.THIRD_LEVEL_NUMBER_PLUS_PHYSICS_UNIT.search(val):
                if self.regex.THIRD_LEVEL_KEY_TERMINALS_PATTERN.search(key):
                    val = remove_parentheses_and_split(
                        value=val, split_by=' ')
                    val = list(
                        map(lambda element: element.replace(',', ''), val))

                    val = join_terminals_value_elements(value=val)
                    third_level_dict[key] = tuple(val)
                    continue

                value, unit = split_number_and_unit(value=val)
                plural = False

                if isinstance(value, tuple):
                    plural = True

                third_level_dict[key] = {
                    f'VALUE{"S" if plural else ""}': value,
                    'UNIT': unit.strip(',')
                }
                continue

            if self.regex.THIRD_LEVEL_NUMBERS_AND_NUMERIC_STRING_TUPLE_VALUE_PATTERN.search(val):
                third_level_dict[key] = ast.literal_eval(val)
                continue

            if self.regex.THIRD_LEVEL_STRING_TUPLE_VALUE_PATTERN.search(val):
                val = remove_parentheses_and_split(value=val, split_by=',')
                third_level_dict[key] = tuple(val)
                continue

            third_level_dict[key] = self.remove_string_quotes(string=val)

        return third_level_dict

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

        return self.copy()

    def __get_second_level_dictionary(self, dict_elements: tuple) -> dict:
        """Builds a dictionary from a list containing tuples with keys and values. Returns a dictionary.

    **Parameters**

        **dict_elements:** (list) List of tuples. The first element is the key and the second one its value.
        """
        # Convert list of strings into a list of dictionaries
        values = list(map(self.__list_of_strings_2_dict,
                          dict_elements[1]))

        key_vals_list = self.group_key_and_vals(
            keys=dict_elements[0], vals=values)

        return self.__build_dictionary(key_vals_list=key_vals_list)

    def __get_second_level_key_val_lists(self, lines: list) -> tuple:
        """Splits a list of lines of genkey text and saves them into two lists. 
        Returns a tuple with each value are the lists.
        The first one is the genkey's second-level keys, and the second one is its values.

    **Parameters**

        **lines:** (list) List of lines of the text block belonging to the genkey's first-level key. 
        """
        elements = list(map(self.__split_line_elements, lines))
        second_level_keys = [el[0] for el in elements]
        second_level_values = [el[1:] for el in elements]

        return second_level_keys, second_level_values

    @staticmethod
    def clean_empty_spaces(string: str, join_by: str = '', split_by: str = '') -> str:
        """Cleans the empty spaces in a string. Returns a string without empty spaces.

    **Parameters**

        **string:** (str) String with empty spaces.
        **join_by:** (str) String to join the stripped string.
        """

        return join_by.join([character.strip() for character
                            in string.split(split_by)])

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

        # Splitting Genkey in principal elements
        genkey_elements = []
        for element in self.regex.GENKEY_PRINCIPAL_ELEMENT_PATTERN.split(file):
            genkey_elements.append(element)

        first_level_keys = []
        second_level_keys_and_vals = []

        for element in genkey_elements:

            # Finding first-level keys in the genkey file's splitted line
            genkey_element = self.clean_empty_spaces(
                string=element, join_by=' ', split_by=' ')
            first_level_key = self.regex.GENKEY_FIRST_LEVEL_KEY_PATTERN.search(
                genkey_element)

            if first_level_key:

                # Remove '!' from the begining of first-level key string.
                # That's because it is distinctive of them.
                first_level_key = first_level_key.group().replace('!', '').strip()
                first_level_keys.append(first_level_key)

                element = self.__clean_lines(element)

                # Convert each line into a dictionary
                second_level_elements = self.__get_second_level_key_val_lists(
                    lines=element)
                second_level_dict = self.__get_second_level_dictionary(
                    dict_elements=second_level_elements)

                second_level_keys_and_vals.append(second_level_dict)

        # Putting together first and second level keys
        genkey_keys = self.group_key_and_vals(
            keys=first_level_keys, vals=second_level_keys_and_vals)

        return self.__build_dictionary(key_vals_list=genkey_keys)
