import os
import re


class Genkey(dict):

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

    def clean_lines(self, lines: str):
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

    def split_values(self, line):
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

    def get_dict_values(self, values: list):
        '''
        Documentation here
        '''
        k = [el.split('=')[0].strip() for el in values]
        v = [el.split('=')[1].strip() for el in values]
        k_v = dict(zip(k, v))

        pattern = re.compile(r'\d\s\w|\d\)\s\w+|\d\)\s\%|\d\s\%|\(\"\w+')
        for key, val in k_v.items():
            if re.search(r'\(\"\.\./|\(\"\w+', val):
                val = [e.replace('"', '').replace('(', '').replace(')', '').strip()
                       for e in val.split(',')]
                val = tuple(val)
                k_v[key] = val
                continue

            if re.search(r'^INFO', key):
                val = val.replace('"', '')
                k_v[key] = val
                continue

            if re.search(r'PVTFILE', key) and not re.search(r'\(\"\.\./|\(\"\w+', val):
                k_v[key] = val.replace('"', '')
                continue

            if pattern.search(val):
                if re.search(r'TERMINALS', key):
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
                    k_v[key] = VALUE
                    continue
                else:
                    val = val.split(' ')
                    VALUE = ' '.join([el for el in val[:-1]])
                    UNIT = val[-1]
                    plural = False
                    VALUE = eval(VALUE)

                    if isinstance(VALUE, tuple):
                        plural = True
                  
                    k_v[key] = {
                        # f'VALUE{"S" if plural else ""}': VALUE,
                        'VALUE': tuple(VALUE) if plural else tuple([VALUE]),
                        'UNIT': UNIT.strip(',')
                    }
                    continue

            if re.search(r'\d+\.\d+|^[0-9]*$|\(\d+', val):
                k_v[key] = eval(val)
                continue

            if re.search(r'\(\w+', val):
                val = val.strip('(').strip(')')
                val = [el.strip() for el in val.split(',') if el]
                k_v[key] = tuple(val)
                continue

            if re.search(r'\d+\ \W+', val):
                plural = False
                val = val.strip().split()
                VALUE = eval(val[0])
                UNIT = val[-1]

                if isinstance(VALUE, tuple):
                    plural = True
                  
                k_v[key] = {
                    # 'VALUE': VALUE,
                    'VALUE': tuple(VALUE) if plural else tuple([VALUE]),
                    'UNIT': UNIT
                }
                continue

            k_v[key] = val.replace('"', '')

        return k_v

    def read(self, filepath: str):
        """Documentstion here
        """
        assert isinstance(
            filepath, str), f'filepath must be a string! Not {type(filepath)}'

        try:
            with open(filepath, 'r') as f:
                file = f.read()
        except FileNotFoundError:
            # Modify the filepath by joining the parent directory path
            parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(filepath)))
            modified_filepath = os.path.join(parent_directory, os.path.basename(filepath))
            
            try:
                with open(modified_filepath, 'r') as f:
                    file = f.read()
            except FileNotFoundError:
                print("File not found in both locations.")


        # Splitting Genkey in principal elements
        split_genkey_elements_pattern = re.compile('\s\n')
        genkey_elements = []

        for element in split_genkey_elements_pattern.split(file):
            genkey_elements.append(element)

        # Getting first level and second level Genkey keys
        first_level_key_pattern = re.compile('!\s\w+.+')
        first_level_keys = []
        second_level_keys = []

        for el in genkey_elements:
            genkey_element = ' '.join([c.strip() for c in el.split(' ')])
            _first_level_key = first_level_key_pattern.search(genkey_element)

            if _first_level_key:
                first_level_key = _first_level_key.group().replace('!', '').strip()
                first_level_keys.append(first_level_key)

                lines = self.clean_lines(el)
                elements = list(map(self.split_values, lines))
                second_keys = [el[0] for el in elements]
                list_values = [el[1:] for el in elements]
                values = list(map(self.get_dict_values, list_values))
                key_vals_list = list(zip(second_keys, values))

                key_vals_dict = {}
                for key in key_vals_list:
                    key_vals_dict.setdefault(key[0], []).append(key[1])

                for key, val in key_vals_dict.items():
                    if len(val) == 1:
                        key_vals_dict[key] = key_vals_dict.get(key)[0]

                second_level_keys.append(key_vals_dict)

        # Putting together first and second level keys
        genkey_keys = list(zip(first_level_keys, second_level_keys))

        # Creating list of second level keys for duplicated first level keys
        for key in genkey_keys:
            self.setdefault(key[0], []).append(key[1])

        # Extracting second level keys from list if first level key is not duplicated.
        for key, val in self.items():
            if len(val) == 1:
                self[key] = self.get(key)[0]

        for key, val in self.items():
            if val == {}:
                self[key] = None

        return self
