import os
import unittest
from ..io.olga.tpl import TPL

class TestTPL(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()
    
    def test_read_raw_file(self):
        filepath = os.path.join("data", "olga", "1.tpl")
        tpl = TPL()

        expected = """'OLGA 2017.2.0.107'
TIME PLOT
INPUT FILE
'1.genkey'
PVT FILE
'../../../00 Nuevos fluidos/Diesel_1.tab' 
DATE
'23-06-08 08:39:01'
PROJECT
'Supe'
TITLE
''
AUTHOR
'Jesus E Varajas'
NETWORK
1
GEOMETRY' (M)  '
BRANCH
'PIPELINE'
18
0.0000000000000000e+000 1.5000000000000000e+000 3.0000000000000000e+000 9.8499999999999996e+000 1.6699999999999999e+001 
1.8699999999999999e+001 2.0699999999999999e+001 2.9299999999999997e+001 3.7899999999999999e+001 4.5399999999999999e+001 
5.2899999999999999e+001 5.6613999999999999e+002 1.0793800000000001e+003 1.2220300000000002e+003 1.3646800000000003e+003 
1.3656800000000003e+003 1.3666800000000003e+003 1.4300300000000002e+003 1.4933800000000001e+003 
0.0000000000000000e+000 0.0000000000000000e+000 0.0000000000000000e+000 6.8499999999999996e+000 1.3699999999999999e+001 
1.3699999999999999e+001 1.3699999999999999e+001 5.1000000000000014e+000 -3.5000000000000000e+000 -3.5000000000000000e+000 
-3.5000000000000000e+000 2.7500000000000000e+000 9.0000000000000000e+000 9.0000000000000000e+000 9.0000000000002274e+000 
1.0000000000000227e+001 1.1000000000000000e+001 1.1000000000000000e+001 1.1000000000000000e+001 
CATALOG 
3
PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'
GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'
PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'
TIME SERIES  ' (S)  '
0.000000e+000 3.246019e+005 1.292260e+002 4.473485e+005
1.005464e+000 3.395254e+005 1.317514e+002 4.931588e+005
2.000000e+000 3.520170e+005 1.382876e+002 5.277908e+005
3.009312e+000 3.483150e+005 1.391391e+002 4.781485e+005
4.117419e+000 3.579895e+005 1.407468e+002 5.130500e+005
5.220287e+000 3.637217e+005 1.439100e+002 5.252984e+005
"""

        self.assertEqual(tpl.read_raw_file(filepath=filepath), expected)
    
    def test_info(self):

        tpl = TPL()

        filepath = os.path.join("data", "olga", "1.tpl")

        file = tpl.read_raw_file(filepath=filepath)
        
        tpl.set_info(file)

        expected = {
            'version': 'OLGA 2017.2.0.107',
            'input_file': '1.genkey',
            'pvt_file': '../../../00 Nuevos fluidos/Diesel_1.tab',
            'date': '23-06-08 08:39:01',
            'project': 'Supe',
            'title': '',
            'author': 'Jesus E Varajas',
            'network': 1,
            'geometry': '(M)',
            'branch': 'PIPELINE'
        }

        self.assertEqual(tpl.info.serialize(), expected)

    # def test_profile(self):

    #     tpl = TPL()

    #     tpl.read_raw_file(filepath=self.filepath)

    #     tpl.set_profile()

    # def test_data_structure(self):

    #     pass