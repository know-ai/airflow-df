import os
import unittest
from ..io.olga.tpl import TPL
from ..io.olga.olga import OlgaFormatter
from pandas import DataFrame as DF

class TestOlga(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()    

    def test_read(self):

        olga = OlgaFormatter()
        filepath = os.path.join("data", "olga", "1.tpl")
        file = olga.read(filepath=filepath)

        with self.subTest(f"Test Read File"):

            self.assertIsInstance(file, TPL)

        with self.subTest(f"Test Olga Formatter as List"):

            self.assertIsInstance(olga, list)

    def test_olga_formatter_items(self):

        olga = OlgaFormatter()
        filepath = os.path.join("data", "olga", "1.tpl")
        olga.read(filepath=filepath)
        
        for item in olga:

            with self.subTest(f"Test Info Item"):

                expected_info = {
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
                self.assertDictEqual(item.info.serialize(), expected_info)

            with self.subTest(f"Test Profile Item"):

                x_expected = [0.0, 1.5, 3.0, 9.85, 16.7, 18.7, 20.7, 29.3, 37.9, 45.4, 52.9, 566.14, 1079.38, 1222.03, 1364.68, 1365.68, 1366.68, 1430.03, 1493.38]
                y_expected = [0.0, 0.0, 0.0, 6.85, 13.7, 13.7, 13.7, 5.1, -3.5, -3.5, -3.5, 2.75, 9.0, 9.0, 9.0, 10.0, 11.0, 11.0, 11.0]
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
                expected_profile = {
                    'profile': expected_profile,
                    'x': x_expected,
                    'y': y_expected
                }
                self.assertEqual(item.profile.serialize(), expected_profile)

            expected_df_serialized = {
                "TIME SERIES  ' (S)  '": [0.000000e+000, 1.005464e+000, 2.000000e+000, 3.009312e+000, 4.117419e+000, 5.220287e+000],
                "PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'":[3.246019e+005, 3.395254e+005, 3.520170e+005, 3.483150e+005, 3.579895e+005, 3.637217e+005],
                "GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'": [1.292260e+002, 1.317514e+002, 1.382876e+002, 1.391391e+002, 1.407468e+002, 1.439100e+002],
                "PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'": [4.473485e+005, 4.931588e+005, 5.277908e+005, 4.781485e+005, 5.130500e+005, 5.252984e+005]
            }

            with self.subTest("Test Serialized Data"):

                self.assertDictEqual(expected_df_serialized, item.data.serialize())

    def test_read_files(self):

        olga = OlgaFormatter()
        filepath = os.path.join("data", "olga")
        files = olga.read(filepath=filepath)
        
        # Expected Values
        ## Expected Info
        expected_info = [
            {
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
            },
            {
                'version': 'OLGA 2017.2.0.107',
                'input_file': '2.genkey',
                'pvt_file': '../../../00 Nuevos fluidos/Diesel_1.tab',
                'date': '23-06-08 08:39:15',
                'project': 'Supe',
                'title': '',
                'author': 'Jesus E Varajas',
                'network': 1,
                'geometry': '(M)',
                'branch': 'PIPELINE'
            }
        ]
        expected_profiles = [
            {
                'profile': [(0.0, 0.0),
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
                ],
                'x': [0.0, 1.5, 3.0, 9.85, 16.7, 18.7, 20.7, 29.3, 37.9, 45.4, 52.9, 566.14, 1079.38, 1222.03, 1364.68, 1365.68, 1366.68, 1430.03, 1493.38],
                'y': [0.0, 0.0, 0.0, 6.85, 13.7, 13.7, 13.7, 5.1, -3.5, -3.5, -3.5, 2.75, 9.0, 9.0, 9.0, 10.0, 11.0, 11.0, 11.0]
            },
            {
                'profile': [(0.0, 0.0),
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
                ],
                'x': [0.0, 1.5, 3.0, 9.85, 16.7, 18.7, 20.7, 29.3, 37.9, 45.4, 52.9, 566.14, 1079.38, 1222.03, 1364.68, 1365.68, 1366.68, 1430.03, 1493.38],
                'y': [0.0, 0.0, 0.0, 6.85, 13.7, 13.7, 13.7, 5.1, -3.5, -3.5, -3.5, 2.75, 9.0, 9.0, 9.0, 10.0, 11.0, 11.0, 11.0]
            }
        ]
        expected_data = [
            {
                "TIME SERIES  ' (S)  '": [0.000000e+000, 1.005464e+000, 2.000000e+000, 3.009312e+000, 4.117419e+000, 5.220287e+000],
                "PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'":[3.246019e+005, 3.395254e+005, 3.520170e+005, 3.483150e+005, 3.579895e+005, 3.637217e+005],
                "GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'": [1.292260e+002, 1.317514e+002, 1.382876e+002, 1.391391e+002, 1.407468e+002, 1.439100e+002],
                "PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'": [4.473485e+005, 4.931588e+005, 5.277908e+005, 4.781485e+005, 5.130500e+005, 5.252984e+005]
            },
            {
                "TIME SERIES  ' (S)  '": [0.000000e+000, 1.005464e+000, 2.000000e+000, 3.000000e+000, 4.176193e+000, 5.162678e+000],
                "PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'":[3.754332e+005, 3.954313e+005, 4.134739e+005, 4.108997e+005, 4.232630e+005, 4.301147e+005],
                "GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'": [1.491664e+002, 1.524124e+002, 1.608624e+002, 1.618578e+002, 1.641531e+002, 1.672679e+002],
                "PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'": [5.222148e+005, 5.818665e+005, 6.359376e+005, 5.721821e+005, 6.134234e+005, 6.246638e+005]
            }
        ]

        with self.subTest(f"Test Read File"):

            self.assertIsInstance(files, list)

        for counter, item in enumerate(olga):

            with self.subTest(f"Test Read File"):

                self.assertIsInstance(item, TPL)

            with self.subTest(f"Test Info Item"):

                self.assertDictEqual(item.info.serialize(), expected_info[counter])

            with self.subTest(f"Test Profile Item"):

                self.assertDictEqual(item.profile.serialize(), expected_profiles[counter])

            with self.subTest(f"Test Data Structure Item"):
                
                self.assertDictEqual(item.data.serialize(), expected_data[counter])
