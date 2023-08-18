import os
import unittest
from ..io.olga import TPL, Genkey
from ..io import Olga
from types import GeneratorType

class TestOlga(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp()    

    def test_read(self):

        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        _generator = olga.read(filepath=filepath)

        with self.subTest(f"Test Read File"):

            self.assertIsInstance(_generator, GeneratorType)

    def test_olga_tpl(self):

        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath)

        for file in files:
            
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
                breakpoint()
                self.assertDictEqual(file.tpl.info.serialize(), expected_info)

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
                self.assertEqual(file.tpl.profile.serialize(), expected_profile)

                expected_df_serialized = {
                    "TIME SERIES  ' (S)  '": [0.000000e+000, 1.005464e+000, 2.000000e+000, 3.009312e+000, 4.117419e+000, 5.220287e+000],
                    "PT 'POSITION:' 'POS-1378M' '(PA)' 'Pressure'":[3.246019e+005, 3.395254e+005, 3.520170e+005, 3.483150e+005, 3.579895e+005, 3.637217e+005],
                    "GT 'POSITION:' 'POS-1378M' '(KG/S)' 'Total mass flow'": [1.292260e+002, 1.317514e+002, 1.382876e+002, 1.391391e+002, 1.407468e+002, 1.439100e+002],
                    "PTLKUP 'LEAK:' 'LEAK' '(PA)' 'Pressure at the position where Leak is positioned'": [4.473485e+005, 4.931588e+005, 5.277908e+005, 4.781485e+005, 5.130500e+005, 5.252984e+005]
                }

            with self.subTest("Test Serialized Data"):

                self.assertDictEqual(file.tpl.data.serialize(), expected_df_serialized)

    def test_olga_genkey_global_keywords(self):
        
        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath=filepath)
        
        for file in files:

            with self.subTest(f"Test Global Keywords Options"):
                expected = {
                    "TEMPERATURE": "ADIABATIC",
                    "FLASHMODEL": "HYDROCARBON", 
                    "MASSEQSCHEME": "2NDORDER", 
                    "ELASTICWALLS": "ON",
                    "FLOWMODEL": "OLGAHD"
                }
                self.assertDictEqual(expected, file.genkey["Global keywords"]["OPTIONS"])
            
            with self.subTest(f"Test Global Keywords CASE"):
                expected = {
                    "AUTHOR": "Jesus E Varajas",
                    "DATE": "02/09/2022",
                    "PROJECT": "Supe",
                    "INFO": "Modelo"
                }
                self.assertEqual(file.genkey["Global keywords"]["CASE"], expected)

            with self.subTest(f"Test Global Keywords Files"):
                expected = {
                    "PVTFILE": "../../../00 Nuevos fluidos/Diesel_1.tab"
                }
                self.assertEqual(file.genkey["Global keywords"]["FILES"], expected)
            
            with self.subTest(f"Test Global Keywords Integration"):
                expected = {
                    "ENDTIME": {
                        "VALUE": (5,),
                        "UNIT": "s"
                    }, 
                    "MAXDT": {
                        "VALUE": (10,),
                        "UNIT": "s"
                    },
                    "MINDT": {
                        "VALUE": (0.02,),
                        "UNIT": "s"
                    },
                    "MAXLAGFACT": 0,
                    "STARTTIME": {
                        "VALUE": (0,),
                        "UNIT": "s"
                    },
                    "DTSTART": {
                        "VALUE": (0.02,),
                        "UNIT": "s"
                    }
                }
                self.assertEqual(file.genkey["Global keywords"]["INTEGRATION"], expected)

            with self.subTest(f"Test Global Keywords OUTPUT"):
                expected = {
                    "WRITEFILE": "OFF"
                }
                self.assertEqual(file.genkey["Global keywords"]["OUTPUT"], expected)

            with self.subTest(f"Test Global Keywords TREND"):
                expected = {
                    "DTPLOT": {
                        "VALUE": (1,),
                        "UNIT": "s"
                    }
                }
                self.assertEqual(file.genkey["Global keywords"]["TREND"], expected)

            with self.subTest(f"Test Global Keywords Profile"):
                expected = {
                    "WRITEFILE": "ON",
                    "DTPLOT": {
                        "VALUE": (1,),
                        "UNIT": "s"
                    },
                    "DTTIME": {
                        "VALUE": (0,),
                        "UNIT": "s"
                    }
                }
                self.assertEqual(file.genkey["Global keywords"]["PROFILE"], expected)

            with self.subTest(f"Test Global Keywords Restart"):
                expected = {
                    "WRITE": "OFF",
                    "READFILE": "OFF"
                }
                self.assertEqual(file.genkey["Global keywords"]["RESTART"], expected)

            with self.subTest(f"Test Global Keywords Animate"):
                expected = {
                    "DTPLOT": {
                        "VALUE": (0,),
                        "UNIT": "s"
                    }
                }
                self.assertEqual(file.genkey["Global keywords"]["ANIMATE"], expected)

    def test_olga_genkey_library_keywords(self):
        
        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath=filepath)
        
        for file in files:
            
            with self.subTest(f"Test Library Keywords Material"):
                expected = [
                    {
                        "LABEL": "Stainless Steel",
                        "CAPACITY": {
                            "VALUE": (450,),
                            "UNIT": "J/kg-C"
                        },
                        "CONDUCTIVITY": {
                            "VALUE": (20,),
                            "UNIT": "W/m-K"
                        },
                        "DENSITY": {
                            "VALUE": (7850,),
                            "UNIT": "kg/m3"
                        },
                        "EMOD": {
                            "VALUE": (210000000000,),
                            "UNIT": "Pa"
                        }
                    },
                    {
                        "LABEL": "Fibra de vidrio",
                        "CAPACITY": {
                            "VALUE": (450,),
                            "UNIT": "J/kg-C"
                        },
                        "CONDUCTIVITY": {
                            "VALUE": (20,),
                            "UNIT": "W/m-C"
                        },
                        "DENSITY": {
                            "VALUE": (7850,),
                            "UNIT": "kg/m3"
                        },
                        "EMOD": {
                            "VALUE": (400000000000,),
                            "UNIT": "Pa"
                        }
                    },
                    {
                        "LABEL": "Concrete Coating HD",
                        "CAPACITY": {
                            "VALUE": (880,),
                            "UNIT": "J/kg-C"
                        },
                        "CONDUCTIVITY": {
                            "VALUE": (2.7,),
                            "UNIT": "W/m-K"
                        }, 
                        "DENSITY": {
                            "VALUE": (3000,),
                            "UNIT": "kg/m3"
                        },
                        "EMOD": {
                            "VALUE": (500000000000,),
                            "UNIT": "Pa"
                        }
                    }
                ]
                self.assertEqual(file.genkey["Library keywords"]["MATERIAL"], expected)

    def test_olga_genkey_connections(self):
        
        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath=filepath)
        
        for file in files:

            with self.subTest(f"Test Connections Connection"):
                expected = [
                    {
                        "TERMINALS": ("FLOWPATH_1 INLET", "NODE_1 FLOWTERM_1")
                    },
                    {
                        "TERMINALS": ("FLOWPATH_1 OUTLET", "NODE_2 FLOWTERM_1")
                    },
                    {
                        "TERMINALS": ("MANUALCONTROLLER_2 CONTR_1", "FLOWPATH_1 LEAK@VALVESIG")
                    },
                    {
                        "TERMINALS": ("MANUALCONTROLLER_1 CONTR_1", "FLOWPATH_1 V-out@VALVESIG")
                    },
                    {
                        "TERMINALS": ("MANUALCONTROLLER_3 CONTR_1", "FLOWPATH_1 V-in@VALVESIG")
                    },
                    {
                        "TERMINALS": ("MANUALCONTROLLER_4 CONTR_1", "NODE_1 PRESSURESIG")
                    },
                    {
                        "TERMINALS": ("MANUALCONTROLLER_5 CONTR_1", "FLOWPATH_1 Entrada@VALVESIG")
                    }
                ]
                self.assertEqual(file.genkey["Connections"]["CONNECTION"], expected)

    def test_olga_genkey_network_component(self):
        olga = Olga()
        filepath = os.path.join("data", "olga", "1")
        files = olga.read(filepath=filepath)
        
        for file in files:

            with self.subTest(f"Test Network Component Network Component"):
                expected = [
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'FLOWPATH', 
                            'TAG': 'FLOWPATH_1'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Pipeline'
                        },
                        'BRANCH': {
                            'FLUID': 'Diesel_1'
                        },
                        'GEOMETRY': {
                            'LABEL': 'GEOMETRY-1'
                        },
                        'PIPE': [
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-1',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    "VALUE": (3,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (0,),
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    "VALUE": (304.8,), 
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-2',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    "VALUE": (13.7,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    "VALUE": (13.7,),
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    "VALUE": (304.8,),
                                    'UNIT': 'mm'
                                }
                            }, 
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-3',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    "VALUE": (4,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (0,),
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    "VALUE": (304.8,),
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-4',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    "VALUE": (17.2,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    "VALUE": (-17.2,),
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    "VALUE": (203.2,),
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                }, 
                                'LABEL': 'Pipe-5',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2, 
                                'LENGTH': {
                                    "VALUE": (15,), 
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (0,), 'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    "VALUE": (203.2,),
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                }, 
                                'LABEL': 'Pipe-6',
                                'WALL': 'WALL-1', 
                                'NSEGMENT': 2, 
                                'LENGTH': {
                                    "VALUE": (1026.48,), 
                                    'UNIT': 'm'
                                }, 
                                'ELEVATION': {
                                    "VALUE": (12.5,), 
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    'VALUE': (283.999999999999,), 
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-7',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    'VALUE': (285.3,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (0,),
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (283.999999999999,),
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,), 
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-8',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    'VALUE': (2,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (2,), 
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    'VALUE': (283.999999999999,),
                                    'UNIT': 'mm'
                                }
                            },
                            {
                                'ROUGHNESS': {
                                    'VALUE': (0.0053,),
                                    'UNIT': 'mm'
                                },
                                'LABEL': 'Pipe-9',
                                'WALL': 'WALL-1',
                                'NSEGMENT': 2,
                                'LENGTH': {
                                    'VALUE': (126.7,),
                                    'UNIT': 'm'
                                },
                                'ELEVATION': {
                                    'VALUE': (0,), 
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    'VALUE': (283.999999999999,), 
                                    'UNIT': 'mm'
                                }
                            }
                        ],
                        'TRENDDATA': [
                            {
                                'ABSPOSITION': {
                                    'VALUE': (1378,), 
                                    'UNIT': 'm'
                                },
                                'VARIABLE': 'PT'
                            },
                            {
                                'ABSPOSITION': {
                                    'VALUE': (1378,),
                                    'UNIT': 'm'
                                },
                                'VARIABLE': 'GT'
                            }, 
                            {
                                'LEAK': 'LEAK', 
                                'VARIABLE': 'PTLKUP'
                            }
                        ],
                        'PROFILEDATA': {
                            'VARIABLE': ('GT', 'PT')
                        },
                        'HEATTRANSFER': [
                            {
                                'LABEL': 'Air', 
                                'PIPE': ('PIPE-15', 'PIPE-16', 'PIPE-17', 'PIPE-18', 'PIPE-19', 'PIPE-20', 'PIPE-21', 'PIPE-22', 'PIPE-23', 'PIPE-24', 'PIPE-25', 'PIPE-26'),
                                'HMININNERWALL': {
                                    'VALUE': (10,),
                                    'UNIT': 'W/m2-C'
                                },
                                'HOUTEROPTION': 'AIR', 
                                'TAMBIENT': {
                                    'VALUE': (21,), 
                                    'UNIT': 'C'
                                }
                            },
                            {
                                'LABEL': 'Water',
                                'PIPE': ('PIPE-5', 'PIPE-6', 'PIPE-1', 'PIPE-2', 'PIPE-3', 'PIPE-4'),
                                'HMININNERWALL': {
                                    'VALUE': (10,), 
                                    'UNIT': 'W/m2-C'
                                },
                                'HOUTEROPTION': 'WATER',
                                'TAMBIENT': {
                                    'VALUE': (21,), 
                                    'UNIT': 'C'
                                }
                            }, 
                            {
                                'LABEL': 'Soil', 
                                'PIPE': ('PIPE-7', 'PIPE-8', 'PIPE-9', 'PIPE-10', 'PIPE-11', 'PIPE-12', 'PIPE-13', 'PIPE-14'),
                                'HOUTEROPTION': 'HGIVEN',
                                'TAMBIENT': {
                                    'VALUE': (21,), 
                                    'UNIT': 'C'
                                },
                                'HAMBIENT': {
                                    'VALUE': (10000,),
                                    'UNIT': 'W/m2-C'
                                }
                            }
                        ], 
                        'LEAK': {
                            'LABEL': 'LEAK', 
                            'VALVETYPE': 'OLGAVALVE', 
                            'ABSPOSITION': {
                                'VALUE': (1000,), 
                                'UNIT': 'm'
                            },
                            'TIME': {
                                'VALUE': (0,), 
                                'UNIT': 's'
                            },
                            'BACKPRESSURE': {
                                'VALUE': (0,),
                                'UNIT': 'psig'
                            },
                            'DIAMETER': {
                                'VALUE': (0.5,), 
                                'UNIT': 'in'
                            }
                        }, 
                        'VALVE': [
                            {
                                'LABEL': 'C-1', 
                                'MODEL': 'HYDROVALVE',
                                'ABSPOSITION': {
                                    'VALUE': (16.7,), 
                                    'UNIT': 'm'
                                },
                                'DIAMETER': {
                                    'VALUE': (12,), 
                                    'UNIT': 'in'
                                }
                            },
                            {
                                'LABEL': 'C-2',
                                'MODEL': 'HYDROVALVE', 
                                'ABSPOSITION': {
                                    'VALUE': (20,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (20.32,), 
                                    'UNIT': 'cm'
                                }
                            },
                            {
                                'LABEL': 'C-4', 
                                'MODEL': 'HYDROVALVE', 
                                'ABSPOSITION': {
                                    'VALUE': (1145,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (28.4,), 
                                    'UNIT': 'cm'
                                }
                            },
                            {
                                'LABEL': 'C-7', 
                                'MODEL': 'HYDROVALVE', 
                                'ABSPOSITION': {
                                    'VALUE': (1364,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (28.4,), 
                                    'UNIT': 'cm'
                                }
                            }, 
                            {
                                'LABEL': 'C-8', 
                                'MODEL': 'HYDROVALVE', 
                                'ABSPOSITION': {
                                    'VALUE': (1366,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (28.4,), 
                                    'UNIT': 'cm'
                                }
                            }, 
                            {
                                'LABEL': 'V-in', 
                                'MODEL': 'HYDROVALVE', 
                                'TIME': {
                                    'VALUE': (0,), 
                                    'UNIT': 's'
                                }, 
                                'STROKETIME': {
                                    'VALUE': (0,), 
                                    'UNIT': 's'
                                }, 
                                'ABSPOSITION': {
                                    'VALUE': (40,), 
                                    'UNIT': 'm'
                                }, 
                                'SLIPMODEL': 'NOSLIP', 
                                'DIAMETER': {
                                    'VALUE': (20.32,), 
                                    'UNIT': 'cm'
                                }, 
                                'OPENING': 1
                            }, 
                            {
                                'LABEL': 'V-out', 
                                'MODEL': 'HYDROVALVE', 
                                'TIME': {
                                    'VALUE': (0,), 
                                    'UNIT': 's'
                                }, 
                                'STROKETIME': {
                                    'VALUE': (0,), 
                                    'UNIT': 's'
                                }, 
                                'ABSPOSITION': {
                                    "VALUE": (1410,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (20.32,), 
                                    'UNIT': 'cm'
                                }, 
                                'OPENING': 1
                            }, 
                            {
                                'LABEL': 'C-9', 
                                'MODEL': 'HYDROVALVE', 
                                'ABSPOSITION': {
                                    'VALUE': (1382,), 
                                    'UNIT': 'm'
                                }, 
                                'DIAMETER': {
                                    'VALUE': (28.4,), 
                                    'UNIT': 'cm'
                                }
                            }
                        ], 
                        'SHUTIN': {
                            'LABEL': 'SHUTIN-1', 
                            'TIME': {
                                'VALUE': (0,), 
                                'UNIT': 's'
                            }, 
                            'ACTIVATE': 'ON'
                        }, 
                        'CHECKVALVE': {
                            'LABEL': 'CHECK-1', 
                            'PIPE': 'Pipe-9', 
                            'SECTIONBOUNDARY': 3
                        }, 
                        'SOURCE': {
                            'LABEL': 'Entrada', 
                            'TIME': {
                                'VALUE': (0,), 
                                'UNIT': 's'
                            }, 
                            'SOURCETYPE': 'PRESSUREDRIVEN', 
                            'ABSPOSITION': {
                                'VALUE': (1000,), 
                                'UNIT': 'm'
                            }, 
                            'TOTALWATERFRACTION': {
                                'VALUE': (100,), 
                                'UNIT': '%'
                            }, 
                            'TEMPERATURE': {
                                'VALUE': (21,), 
                                'UNIT': 'C'
                            }, 
                            'PRESSURE': {
                                'VALUE': (20,), 
                                'UNIT': 'psig'
                            }, 
                            'DIAMETER': {
                                'VALUE': (1,), 
                                'UNIT': 'in'
                            }, 
                            'STROKETIME': {
                                'VALUE': (0,), 
                                'UNIT': 's'
                            }
                        }, 
                        'ANIMATETRENDDATA': {
                            'VARIABLE': 'VELOCITYPROFILE', 
                            'POSITION': 'POS-1'
                        }, 
                        'POSITION': {
                            'LABEL': 'POS-1', 
                            'ABSPOSITION': {
                                'VALUE': (1380,), 
                                'UNIT': 'm'
                            }
                        }, 
                        'CROSSDATA': {
                            'VARIABLE': 'U-PROFILE', 
                            'ABSPOSITION': {
                                'VALUE': (1380,), 
                                'UNIT': 'm'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'MANUALCONTROLLER', 
                            'TAG': 'MANUALCONTROLLER_1'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Control-Vout',
                            'TIME': {
                                'VALUE': (0,), 
                                'UNIT': 's'
                            },
                            'SETPOINT': 0.2727, 
                            'OPENINGTIME': {
                                'VALUE': (10,), 
                                'UNIT': 's'
                            },
                            'CLOSINGTIME': {
                                'VALUE': (30,),
                                'UNIT': 's'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'MANUALCONTROLLER', 
                            'TAG': 'MANUALCONTROLLER_2'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Control-Leak',
                            'TIME': {
                                'VALUE': (0, 2),
                                'UNIT': 's'
                            },
                            'SETPOINT': (0, 1),
                            'STROKETIME': {
                                'VALUE': (1,),
                                'UNIT': 's'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'MANUALCONTROLLER',
                            'TAG': 'MANUALCONTROLLER_3'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Control-Vin',
                            'TIME': {
                                'VALUE': (0,),
                                'UNIT': 's'
                            },
                            'SETPOINT': 1,
                            'OPENINGTIME': {
                                'VALUE': (10,),
                                'UNIT': 's'
                            },
                            'CLOSINGTIME': {
                                'VALUE': (60,),
                                'UNIT': 's'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'MANUALCONTROLLER',
                            'TAG': 'MANUALCONTROLLER_4'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Control-TKin', 
                            'TIME': {
                                'VALUE': (0,),
                                'UNIT': 's'
                            },
                            'SETPOINT': 0.7884,
                            'OPENINGTIME': {
                                'VALUE': (10,),
                                'UNIT': 's'
                            },
                            'CLOSINGTIME': {
                                'VALUE': (124.24933,),
                                'UNIT': 's'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'MANUALCONTROLLER',
                            'TAG': 'MANUALCONTROLLER_5'
                        },
                        'PARAMETERS': {
                            'LABEL': 'Control-Source', 
                            'TIME': {
                                'VALUE': (0,),
                                'UNIT': 's'
                            },
                            'SETPOINT': 0,
                            'STROKETIME': {
                                'VALUE': (0,),
                                'UNIT': 's'
                            }
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'NODE',
                            'TAG': 'NODE_1'
                        },
                        'PARAMETERS': {
                            'LABEL': 'TK-in',
                            'TYPE': 'PRESSURE',
                            'GASFRACEQ': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'WATERFRACEQ': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'FEEDNAME': 'P500',
                            'FEEDVOLFRACTION': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'TEMPERATURE': {
                                'VALUE': (30,), 
                                'UNIT': 'C'
                            },
                            'PRESSURE': {
                                'VALUE': (130,),
                                'UNIT': 'psig'
                            },
                            'TIME': {
                                'VALUE': (0,),
                                'UNIT': 'M'
                            },
                            'FLUID': 'Diesel_1'
                        }
                    },
                    {
                        'NETWORKCOMPONENT': {
                            'TYPE': 'NODE',
                            'TAG': 'NODE_2'
                        },
                        'PARAMETERS': {
                            'LABEL': 'TK-Out',
                            'TYPE': 'PRESSURE',
                            'GASFRACEQ': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'WATERFRACEQ': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'FEEDNAME': 'P500',
                            'FEEDVOLFRACTION': {
                                'VALUE': (1,),
                                "UNIT": "-"
                            },
                            'TEMPERATURE': {
                                'VALUE': (30,),
                                'UNIT': 'C'
                            },
                            'PRESSURE': {
                                'VALUE': (10,),
                                'UNIT': 'psig'
                            },
                            'TIME': {
                                'VALUE': (0,),
                                'UNIT': 's'
                            }, 
                            'FLUID': 'Diesel_1'
                        }
                    }
                ]
                self.assertEqual(file.genkey["Network Component"], expected)

    def test_read_files(self):

        olga = Olga()
        filepath = os.path.join("data", "olga")
        files = olga.read(filepath=filepath)

        for _, item in enumerate(files):

            with self.subTest(f"Test Read File"):

                self.assertIsInstance(item.tpl, TPL)
            
            with self.subTest(f"Test Read File"):

                self.assertIsInstance(item.genkey, Genkey)