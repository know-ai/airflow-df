import os
import unittest
from ..io.olga.genkey import Genkey
from pandas import DataFrame as DF

class TestGenkey(unittest.TestCase):

    def setUp(self) -> None:
        
        return super().setUp() 
    
    def test_read(self):

        filepath = os.path.join("data", "olga", "1.genkey")
        genkey = Genkey()
        file = genkey.read(filepath=filepath)

        with self.subTest(f"Test Primary Keys"):

            expected = [
                "Generated with OLGA version 2017.2.0",
                "Global keywords",
                "Library keywords",
                "Network Component",
                "Connections"
            ]
            self.assertListEqual(expected, list(file.keys()))

    def test_global_keywords_options(self):

        filepath = os.path.join("data", "olga", "1.genkey")
        genkey = Genkey()
        file = genkey.read(filepath=filepath)

        with self.subTest(f"Test Global Keywords Options"):
            expected = {
                "TEMPERATURE": "ADIABATIC",
                "FLASHMODEL": "HYDROCARBON", 
                "MASSEQSCHEME": "2NDORDER", 
                "ELASTICWALLS": "ON",
                "FLOWMODEL": "OLGAHD"
            }
            self.assertDictEqual(expected, file["Global keywords"]["OPTIONS"])
        
        with self.subTest(f"Test Global Keywords CASE"):
            expected = {
                "AUTHOR": "Jesus E Varajas",
                "DATE": "02/09/2022",
                "PROJECT": "Supe",
                "INFO": "Modelo"
            }
            self.assertEqual(file["Global keywords"]["CASE"], expected)

        with self.subTest(f"Test Global Keywords Files"):
            expected = {
                "PVTFILE": "../../../00 Nuevos fluidos/Diesel_1.tab"
            }
            self.assertEqual(file["Global keywords"]["FILES"], expected)
        
        with self.subTest(f"Test Global Keywords Integration"):
            expected = {
                "ENDTIME": {
                    "VALUE": 5,
                    "UNIT": "s"
                }, 
                "MAXDT": {
                    "VALUE": 10,
                    "UNIT": "s"
                },
                "MINDT": {
                    "VALUE": 0.02,
                    "UNIT": "s"
                },
                "MAXLAGFACT": 0,
                "STARTTIME": {
                    "VALUE": 0,
                    "UNIT": "s"
                },
                "DTSTART": {
                    "VALUE": 0.02,
                    "UNIT": "s"
                }
            }
            self.assertEqual(file["Global keywords"]["INTEGRATION"], expected)

        with self.subTest(f"Test Global Keywords OUTPUT"):
            expected = {
                "WRITEFILE": "OFF"
            }
            self.assertEqual(file["Global keywords"]["OUTPUT"], expected)

        with self.subTest(f"Test Global Keywords TREND"):
            expected = {
                "DTPLOT": {
                    "VALUE": 1,
                    "UNIT": "s"
                }
            }
            self.assertEqual(file["Global keywords"]["TREND"], expected)

        with self.subTest(f"Test Global Keywords Profile"):
            expected = {
                "WRITEFILE": "ON",
                "DTPLOT": {
                    "VALUE": 1,
                    "UNIT": "s"
                },
                "DTTIME": {
                    "VALUE": 0,
                    "UNIT": "s"
                }
            }
            self.assertEqual(file["Global keywords"]["PROFILE"], expected)

        with self.subTest(f"Test Global Keywords Restart"):
            expected = {
                "WRITE": "OFF",
                "READFILE": "OFF"
            }
            self.assertEqual(file["Global keywords"]["RESTART"], expected)

        with self.subTest(f"Test Global Keywords Animate"):
            expected = {
                "DTPLOT": {
                    "VALUE": 0,
                    "UNIT": "s"
                }
            }
            self.assertEqual(file["Global keywords"]["ANIMATE"], expected)

    def test_library_keywords(self):

        filepath = os.path.join("data", "olga", "1.genkey")
        genkey = Genkey()
        file = genkey.read(filepath=filepath)

        with self.subTest(f"Test Library Keywords Material"):
            expected = [
                {
                    "LABEL": "Stainless Steel",
                    "CAPACITY": {
                        "VALUE": 450,
                        "UNIT": "J/kg-C", 
                    },
                    "CONDUCTIVITY": {
                        "VALUE": 20,
                        "UNIT": "W/m-K"
                    },
                    "DENSITY": {
                        "VALUE": 7850,
                        "UNIT": "kg/m3"
                    },
                    "EMOD": {
                        "VALUE": 210000000000,
                        "UNIT": "Pa"
                    }
                },
                {
                    "LABEL": "Fibra de vidrio",
                    "CAPACITY": {
                        "VALUE": 450,
                        "UNIT": "J/kg-C"
                    },
                    "CONDUCTIVITY": {
                        "VALUE": 20,
                        "UNIT": "W/m-C"
                    },
                    "DENSITY": {
                        "VALUE": 7850,
                        "UNIT": "kg/m3"
                    },
                    "EMOD": {
                        "VALUE": 400000000000,
                        "UNIT": "Pa"
                    }
                },
                {
                    "LABEL": "Concrete Coating HD",
                    "CAPACITY": {
                        "VALUE": 880,
                        "UNIT": "J/kg-C"
                    },
                    "CONDUCTIVITY": {
                        "VALUE": 2.7,
                        "UNIT": "W/m-K"
                    }, 
                    "DENSITY": {
                        "VALUE": 3000,
                        "UNIT": "kg/m3"
                    },
                    "EMOD": {
                        "VALUE": 500000000000,
                        "UNIT": "Pa"
                    }
                }
            ]
            self.assertEqual(file["Library keywords"]["MATERIAL"], expected)

        with self.subTest(f"Test Library Keywords Wall"):
            expected = {
                "LABEL": "WALL-1",
                "THICKNESS": {
                    "VALUES": (1, 1.5875, 1),
                    "UNIT": "cm"
                },
                "MATERIAL": ("Fibra de vidrio", "Stainless Steel", "Concrete Coating HD"), 
                "ELASTIC": "ON"
            }
            self.assertEqual(file["Library keywords"]["WALL"], expected)

        with self.subTest(f"Test Library Keywords Cent Pump Curve"):
            expected = [
                {
                    "LABEL": "C-1",
                    "VOLUMEFLOW": {
                        "VALUES": (0, 181.9067, 363.6619, 545.2656, 681.582, 817.8984, 954.2148, 1090.531),
                        "UNIT": "m3/h"
                    },
                    "SPEED": {
                        "VALUES": (3299.76, 3299.76, 3299.76, 3299.76, 3299.76, 3299.76, 3299.76, 3299.76),
                        "UNIT": "rpm"
                    },
                    "GVF": {
                        "VALUE": 0,
                        "UNIT": "%"
                    },
                    "DENSITY": {
                        "VALUE": 997,
                        "UNIT": "kg/m3"
                    },
                    "EFFICIENCY": {
                        "VALUES": (63, 66.89, 69.22, 70, 69.56, 68.25, 66.06, 63),
                        "UNIT": "%"
                    }, 
                    "HEAD": {
                        "VALUES": (103.0491, 99.92642, 96.80372, 93.68102, 78.1651, 57.37963, 31.32459, 0),
                        "UNIT": "m"
                    }
                },
                {
                    "LABEL": "C-2",
                    "VOLUMEFLOW": {
                        "VALUES": (0, 188.5352, 376.9134, 565.1346, 706.4182, 847.7018, 988.9855, 1130.269),
                        "UNIT": "m3/h"
                    },
                    "SPEED": {
                        "VALUES": (3420, 3420, 3420, 3420, 3420, 3420, 3420, 3420),
                        "UNIT": "rpm"
                    },
                    "GVF": {
                        "VALUE": 0,
                        "UNIT": "%"
                    },
                    "DENSITY": {
                        "VALUE": 997,
                        "UNIT": "kg/m3"
                    },
                    "EFFICIENCY": {
                        "VALUES": (63, 66.89, 69.22, 70, 69.56, 68.25, 66.06, 63),
                        "UNIT": "%"
                    },
                    "HEAD": {
                        "VALUES": (110.696, 107.3415, 103.9871, 100.6327, 83.96541, 61.63753, 33.64906, 0),
                        "UNIT": "m"
                    }
                },
                {
                    "LABEL": "C-3",
                    "VOLUMEFLOW": {
                        "VALUES": (0, 198.4581, 396.7509, 594.8785, 743.5981, 892.3177, 1041.037, 1189.757),
                        "UNIT": "m3/h"
                    },
                    "SPEED": {
                        "VALUES": (3600, 3600, 3600, 3600, 3600, 3600, 3600, 3600),
                        "UNIT": "rpm"
                    },
                    "GVF": {
                        "VALUE": 0,
                        "UNIT": "%"
                    },
                    "DENSITY": {
                        "VALUE": 997,
                        "UNIT": "kg/m3"
                    },
                    "EFFICIENCY": {
                        "VALUES": (63, 66.89, 69.22, 70, 69.56, 68.25, 66.06, 63),
                        "UNIT": "%"
                    },
                    "HEAD": {
                        "VALUES": (122.6548, 118.938, 115.2212, 111.5044, 93.03646, 68.29643, 37.28428, 0),
                        "UNIT": "m"
                    }
                }
            ]
            self.assertEqual(file["Library keywords"]["CENTPUMPCURVE"], expected)

        with self.subTest(f"Test Library Keywords Timeseries"):
            expected = [
                {
                    "LABEL": "Clima aire",
                    "TYPE": "POINTS",
                    "TIME": {
                        "VALUE": 0,
                        "UNIT": "M"
                    },
                    "AMPLITUDE": 2, 
                    "PERIOD": {
                        "VALUE": 48,
                        "UNIT": "h"
                    },
                    "TIME": {
                        "VALUES": (0, 150, 300),
                        "UNIT": "s"
                    },
                    "SERIES": (0, 0, 49)
                },
                {
                    "LABEL": "Clima mar",
                    "AMPLITUDE": 2,
                    "PERIOD": {
                        "VALUE": 2,
                        "UNIT": "d"
                    }
                },
                {
                    "LABEL": "clima suelo",
                    "AMPLITUDE": 2,
                    "PERIOD": {
                        "VALUE": 2,
                        "UNIT": "d"
                    }
                }
            ]
            self.assertEqual(file["Library keywords"]["TIMESERIES"], expected)

    def test_connections(self):

        filepath = os.path.join("data", "olga", "1.genkey")
        genkey = Genkey()
        file = genkey.read(filepath=filepath)

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
            self.assertEqual(file["Connections"]["CONNECTION"], expected)

    def test_network_component(self):

        filepath = os.path.join("data", "olga", "1.genkey")
        genkey = Genkey()
        file = genkey.read(filepath=filepath)

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
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-1',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 3,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 0,
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 304.8, 
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-2',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 13.7,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 13.7,
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 304.8,
                                'UNIT': 'mm'
                            }
                        }, 
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-3',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 4,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 0,
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 304.8,
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-4',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 17.2,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': -17.2,
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 203.2,
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            }, 
                            'LABEL': 'Pipe-5',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2, 
                            'LENGTH': {
                                'VALUE': 15, 
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 0, 'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 203.2,
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            }, 
                            'LABEL': 'Pipe-6',
                            'WALL': 'WALL-1', 
                            'NSEGMENT': 2, 
                            'LENGTH': {
                                'VALUE': 1026.48, 
                                'UNIT': 'm'
                            }, 
                            'ELEVATION': {
                                'VALUE': 12.5, 
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 283.999999999999, 
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-7',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 285.3,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 0,
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 283.999999999999,
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053, 
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-8',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 2,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 2, 
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 283.999999999999,
                                'UNIT': 'mm'
                            }
                        },
                        {
                            'ROUGHNESS': {
                                'VALUE': 0.0053,
                                'UNIT': 'mm'
                            },
                            'LABEL': 'Pipe-9',
                            'WALL': 'WALL-1',
                            'NSEGMENT': 2,
                            'LENGTH': {
                                'VALUE': 126.7,
                                'UNIT': 'm'
                            },
                            'ELEVATION': {
                                'VALUE': 0, 
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 283.999999999999, 
                                'UNIT': 'mm'
                            }
                        }
                    ],
                    'TRENDDATA': [
                        {
                            'ABSPOSITION': {
                                'VALUE': 1378, 
                                'UNIT': 'm'
                            },
                            'VARIABLE': 'PT'
                        },
                        {
                            'ABSPOSITION': {
                                'VALUE': 1378,
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
                                'VALUE': 10,
                                'UNIT': 'W/m2-C'
                            },
                            'HOUTEROPTION': 'AIR', 
                            'TAMBIENT': {
                                'VALUE': 21, 
                                'UNIT': 'C'
                            }
                        },
                        {
                            'LABEL': 'Water',
                            'PIPE': ('PIPE-5', 'PIPE-6', 'PIPE-1', 'PIPE-2', 'PIPE-3', 'PIPE-4'),
                            'HMININNERWALL': {
                                'VALUE': 10, 
                                'UNIT': 'W/m2-C'
                            },
                            'HOUTEROPTION': 'WATER',
                            'TAMBIENT': {
                                'VALUE': 21, 
                                'UNIT': 'C'
                            }
                        }, 
                        {
                            'LABEL': 'Soil', 
                            'PIPE': ('PIPE-7', 'PIPE-8', 'PIPE-9', 'PIPE-10', 'PIPE-11', 'PIPE-12', 'PIPE-13', 'PIPE-14'),
                            'HOUTEROPTION': 'HGIVEN',
                            'TAMBIENT': {
                                'VALUE': 21, 
                                'UNIT': 'C'
                            },
                            'HAMBIENT': {
                                'VALUE': 10000,
                                'UNIT': 'W/m2-C'
                            }
                        }
                    ], 
                    'LEAK': {
                        'LABEL': 'LEAK', 
                        'VALVETYPE': 'OLGAVALVE', 
                        'ABSPOSITION': {
                            'VALUE': 1000, 
                            'UNIT': 'm'
                        },
                        'TIME': {
                            'VALUE': 0, 
                            'UNIT': 's'
                        },
                        'BACKPRESSURE': {
                            'VALUE': 0,
                            'UNIT': 'psig'
                        },
                        'DIAMETER': {
                            'VALUE': 0.5, 
                            'UNIT': 'in'
                        }
                    }, 
                    'VALVE': [
                        {
                            'LABEL': 'C-1', 
                            'MODEL': 'HYDROVALVE',
                            'ABSPOSITION': {
                                'VALUE': 16.7, 
                                'UNIT': 'm'
                            },
                            'DIAMETER': {
                                'VALUE': 12, 
                                'UNIT': 'in'
                            }
                        },
                        {
                            'LABEL': 'C-2',
                            'MODEL': 'HYDROVALVE', 
                            'ABSPOSITION': {
                                'VALUE': 20, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 20.32, 
                                'UNIT': 'cm'
                            }
                        },
                        {
                            'LABEL': 'C-4', 
                            'MODEL': 'HYDROVALVE', 
                            'ABSPOSITION': {
                                'VALUE': 1145, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 28.4, 
                                'UNIT': 'cm'
                            }
                        },
                        {
                            'LABEL': 'C-7', 
                            'MODEL': 'HYDROVALVE', 
                            'ABSPOSITION': {
                                'VALUE': 1364, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 28.4, 
                                'UNIT': 'cm'
                            }
                        }, 
                        {
                            'LABEL': 'C-8', 
                            'MODEL': 'HYDROVALVE', 
                            'ABSPOSITION': {
                                'VALUE': 1366, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 28.4, 
                                'UNIT': 'cm'
                            }
                        }, 
                        {
                            'LABEL': 'V-in', 
                            'MODEL': 'HYDROVALVE', 
                            'TIME': {
                                'VALUE': 0, 
                                'UNIT': 's'
                            }, 
                            'STROKETIME': {
                                'VALUE': 0, 
                                'UNIT': 's'
                            }, 
                            'ABSPOSITION': {
                                'VALUE': 40, 
                                'UNIT': 'm'
                            }, 
                            'SLIPMODEL': 'NOSLIP', 
                            'DIAMETER': {
                                'VALUE': 20.32, 
                                'UNIT': 'cm'
                            }, 
                            'OPENING': 1
                        }, 
                        {
                            'LABEL': 'V-out', 
                            'MODEL': 'HYDROVALVE', 
                            'TIME': {
                                'VALUE': 0, 
                                'UNIT': 's'
                            }, 
                            'STROKETIME': {
                                'VALUE': 0, 
                                'UNIT': 's'
                            }, 
                            'ABSPOSITION': {
                                'VALUE': 1410, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 20.32, 
                                'UNIT': 'cm'
                            }, 
                            'OPENING': 1
                        }, 
                        {
                            'LABEL': 'C-9', 
                            'MODEL': 'HYDROVALVE', 
                            'ABSPOSITION': {
                                'VALUE': 1382, 
                                'UNIT': 'm'
                            }, 
                            'DIAMETER': {
                                'VALUE': 28.4, 
                                'UNIT': 'cm'
                            }
                        }
                    ], 
                    'SHUTIN': {
                        'LABEL': 'SHUTIN-1', 
                        'TIME': {
                            'VALUE': 0, 
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
                            'VALUE': 0, 
                            'UNIT': 's'
                        }, 
                        'SOURCETYPE': 'PRESSUREDRIVEN', 
                        'ABSPOSITION': {
                            'VALUE': 1000, 
                            'UNIT': 'm'
                        }, 
                        'TOTALWATERFRACTION': {
                            'VALUE': 100, 
                            'UNIT': '%'
                        }, 
                        'TEMPERATURE': {
                            'VALUE': 21, 
                            'UNIT': 'C'
                        }, 
                        'PRESSURE': {
                            'VALUE': 20, 
                            'UNIT': 'psig'
                        }, 
                        'DIAMETER': {
                            'VALUE': 1, 
                            'UNIT': 'in'
                        }, 
                        'STROKETIME': {
                            'VALUE': 0, 
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
                            'VALUE': 1380, 
                            'UNIT': 'm'
                        }
                    }, 
                    'CROSSDATA': {
                        'VARIABLE': 'U-PROFILE', 
                        'ABSPOSITION': {
                            'VALUE': 1380, 
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
                            'VALUE': 0, 
                            'UNIT': 's'
                        },
                        'SETPOINT': 0.2727, 
                        'OPENINGTIME': {
                            'VALUE': 10, 
                            'UNIT': 's'
                        },
                        'CLOSINGTIME': {
                            'VALUE': 30,
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
                            'VALUES': (0, 2),
                            'UNIT': 's'
                        },
                        'SETPOINT': (0, 1),
                        'STROKETIME': {
                            'VALUE': 1,
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
                            'VALUE': 0,
                            'UNIT': 's'
                        },
                        'SETPOINT': 1,
                        'OPENINGTIME': {
                            'VALUE': 10,
                            'UNIT': 's'
                        },
                        'CLOSINGTIME': {
                            'VALUE': 60,
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
                            'VALUE': 0,
                            'UNIT': 's'
                        },
                        'SETPOINT': 0,
                        'STROKETIME': {
                            'VALUE': 0,
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
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'WATERFRACEQ': {
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'FEEDNAME': 'P500',
                        'FEEDVOLFRACTION': {
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'TEMPERATURE': {
                            'VALUE': 30, 
                            'UNIT': 'C'
                        },
                        'PRESSURE': {
                            'VALUE': 130,
                            'UNIT': 'psig'
                        },
                        'TIME': {
                            'VALUE': 0,
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
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'WATERFRACEQ': {
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'FEEDNAME': 'P500',
                        'FEEDVOLFRACTION': {
                            "VALUE": 1,
                            "UNIT": "-"
                        },
                        'TEMPERATURE': {
                            'VALUE': 30,
                            'UNIT': 'C'
                        },
                        'PRESSURE': {
                            'VALUE': 10,
                            'UNIT': 'psig'
                        },
                        'TIME': {
                            'VALUE': 0,
                            'UNIT': 's'
                        }, 
                        'FLUID': 'Diesel_1'
                    }
                }
            ]
            self.assertEqual(file["Network Component"], expected)