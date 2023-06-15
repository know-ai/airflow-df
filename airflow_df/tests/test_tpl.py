import os
import unittest
from ..io.olga.tpl import TPL
from pandas import DataFrame as DF

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

    def test_delete_quotes(self):
        """
        Set delete_quotes as a static method with documentation
        """
        tpl = TPL()

        with self.subTest(f"Has delete_quotes method as staticmethod?"):

            self.assertTrue(hasattr(tpl.info, 'delete_quotes'))

    def test_profile(self):

        tpl = TPL()

        filepath = os.path.join("data", "olga", "1.tpl")

        file = tpl.read_raw_file(filepath=filepath)

        tpl.set_profile(file)

        with self.subTest(f"Test x Profile"):

            x_expected = [
                0.0,
                1.5,
                3.0,
                9.85,
                16.7,
                18.7,
                20.7,
                29.3,
                37.9,
                45.4,
                52.9,
                566.14,
                1079.38,
                1222.03,
                1364.68,
                1365.68,
                1366.68,
                1430.03,
                1493.38
            ]
            self.assertEqual(tpl.profile.x, x_expected)

        with self.subTest(f"Test y Profile"):

            y_expected = [
                0.0,
                0.0,
                0.0,
                6.85,
                13.7,
                13.7,
                13.7,
                5.1,
                -3.5,
                -3.5,
                -3.5,
                2.75,
                9.0,
                9.0,
                9.0,
                10.0,
                11.0,
                11.0,
                11.0 
            ]
            self.assertEqual(tpl.profile.y, y_expected)

        with self.subTest(f"Test Profile"):

            expected_profile = [(0.0, 0.0),
                (1.5, 0.0),
                (3.0, 0.0),
                (9.85, 6.85),
                (16.7, 13.7),
                (18.7, 13.7),
                (20.7, 13.7),
                (29.3, 5.1),
                (37.9, -3.5),
                (45.4, -3.5),
                (52.9, -3.5),
                (566.14, 2.75),
                (1079.38, 9.0),
                (1222.03, 9.0),
                (1364.68, 9.0),
                (1365.68, 10.0),
                (1366.68, 11.0),
                (1430.03, 11.0),
                (1493.38, 11.0)
            ]

            self.assertEqual(tpl.profile.profile, expected_profile)

        with self.subTest(f"Test Profile Serialized"):

            expected_profile = {
                'profile': expected_profile,
                'x': x_expected,
                'y': y_expected
            }

            self.assertEqual(tpl.profile.serialize(), expected_profile)

    def test_data_structure(self):

        tpl = TPL()

        filepath = os.path.join("data", "olga", "1.tpl")

        file = tpl.read_raw_file(filepath=filepath)

        tpl.set_data(file)

        expected_df_serialized = {
            "TIME SERIES  ' (S)  '": [0.000000e+000, 1.005464e+000, 2.000000e+000, 3.009312e+000, 4.117419e+000, 5.220287e+000],
            "PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'":[3.246019e+005, 3.395254e+005, 3.520170e+005, 3.483150e+005, 3.579895e+005, 3.637217e+005],
            "GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'": [1.292260e+002, 1.317514e+002, 1.382876e+002, 1.391391e+002, 1.407468e+002, 1.439100e+002],
            "PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'": [4.473485e+005, 4.931588e+005, 5.277908e+005, 4.781485e+005, 5.130500e+005, 5.252984e+005]
        }

        with self.subTest("Test Serialized Data"):

            self.assertDictEqual(expected_df_serialized, tpl.data.serialize())