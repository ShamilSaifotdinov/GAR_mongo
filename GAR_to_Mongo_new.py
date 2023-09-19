import time
import xml.etree.ElementTree as ET
import re, os
import pprint

from pymongo import MongoClient

def get_col(region_code, colName):
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb://localhost:27017/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client[f'GAR{region_code}'][colName]

class parse_xml:
    def __init__(self, path, region_code):
        startTime = time.time()
        self.region_code = region_code
        self.regionPath = f'{path}{region_code}\\'

        for elem in self.file_types.keys():
            for name in os.listdir(path):
                if (re.match(f'{elem}_\d+_.*', name)):
                    self.file_types[elem] = name
                    break
        
        for elem in self.file_types.keys():
            for name in os.listdir(self.regionPath):
                if (re.match(f'{elem}_\d+_.*', name)):
                    self.file_types[elem] = name
                    break
        
        pprint.pprint(self.file_types)

        if self.file_types['AS_HOUSE_TYPES']: self.houseTypes = self.parse_dict(path + self.file_types['AS_HOUSE_TYPES'])
        if self.file_types['AS_ADDHOUSE_TYPES']: self.addhouseTypes = self.parse_dict(path + self.file_types['AS_ADDHOUSE_TYPES'])
        if self.file_types['AS_APARTMENT_TYPES']: self.apartTypes = self.parse_dict(path + self.file_types['AS_APARTMENT_TYPES'])
        if self.file_types['AS_ROOM_TYPES']: self.roomTypes = self.parse_dict(path + self.file_types['AS_ROOM_TYPES'])
        
        print(self.houseTypes)
        print(self.addhouseTypes)
        print(self.apartTypes)
        print(self.roomTypes)

        # self.addr()
        # self.objs('stead')
        # self.objs('carplace')
        # self.objs('apartment')
        # self.objs('room')
        # self.objs('house')
        # self.reestr()
        
        print("Start: ", time.ctime(startTime))
        endTime = time.time()
        print("End: ", time.ctime(endTime))
        print("Density: ", endTime - startTime)
    
    file_types = {
        'AS_ADDR_OBJ': '',
        'AS_ADDR_OBJ_PARAMS': '',
        'AS_ADM_HIERARCHY': '',
        'AS_MUN_HIERARCHY': '',
        'AS_OBJECT_LEVELS': '',
        'AS_PARAM_TYPES': '',
        
        'AS_STEADS': '',
        'AS_STEADS_PARAMS': '',
        'AS_CARPLACES': '',
        'AS_CARPLACES_PARAMS': '',
        
        'AS_HOUSES': '',
        'AS_HOUSES_PARAMS': '',
        'AS_HOUSE_TYPES': '',
        'AS_ADDHOUSE_TYPES': '',

        'AS_APARTMENTS': '',
        'AS_APARTMENTS_PARAMS': '',
        'AS_APARTMENT_TYPES': '',
        'AS_ROOMS': '',
        'AS_ROOMS_PARAMS': '',
        'AS_ROOM_TYPES': '',

        'AS_REESTR_OBJECTS': ''
    }
    
    def reestr(self):
        if self.file_types['AS_REESTR_OBJECTS']:
            reestrCol = get_col(self.region_code, 'reestr')
            print("REESTR_OBJECTS:", reestrCol.drop())

            result = self.parse_reestr(self.regionPath + self.file_types['AS_REESTR_OBJECTS'])

            reestrCol.insert_many(result)
            reestrCol.create_index('objectId', unique=True )
    
    def addr(self):
        addrCol = get_col(self.region_code, 'addr')
        print("ADDR_OBJ:", addrCol.drop())

        self.upd = {}

        # self.file_types['AS_OBJECT_LEVELS'] and self.parse_obj_levels(path + self.file_types['AS_OBJECT_LEVELS'])

        self.file_types['AS_ADDR_OBJ'] and self.parse_addr_obj(self.regionPath + self.file_types['AS_ADDR_OBJ'])
        self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm')
        self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun')
        self.file_types['AS_ADDR_OBJ_PARAMS'] and self.parse_params(self.regionPath + self.file_types['AS_ADDR_OBJ_PARAMS'])

        
        addrCol.insert_many(list(self.upd.values()))
        addrCol.create_index('objectId', unique=True )
    
    def objs(self, type):
        typeUp = type.upper()
        self.upd = {}

        steadsCol = get_col(self.region_code, type)
        print(f"{typeUp}S:", steadsCol.drop())

        self.file_types[f'AS_{typeUp}S'] and self.parse_objs(self.regionPath + self.file_types[f'AS_{typeUp}S'], typeUp)
        self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm')
        self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun')
        self.file_types[f'AS_{typeUp}S_PARAMS'] and self.parse_params(self.regionPath + self.file_types[f'AS_{typeUp}S_PARAMS'])
        
        if len(list(self.upd.values())):
            steadsCol.insert_many(list(self.upd.values()))
            steadsCol.create_index('objectId', unique=True )
        else: print(typeUp, "empty!")
        
    # Parsers

    def parse_dict(self, path):
        result = {}
        tree = ET.parse(path).getroot()
        for elem in tree:
            if 'SHORTNAME' in elem.attrib.keys(): 
                result[elem.attrib['ID']] = elem.attrib['SHORTNAME']
            else:
                result[elem.attrib['ID']] = ''
        return result
    
    def parse_reestr(self, path):
        tree = ET.iterparse(path)
        i = 0
        arr = []
        for event, elem in tree:
            i += 1
            if elem.tag == 'OBJECT' and elem.attrib['ISACTIVE'] == "1":
                try:
                    arr.append({
                        "objectId": int(elem.attrib['OBJECTID']),
                        "objectGIUD": elem.attrib['OBJECTGUID'],
                        "level": int(elem.attrib['LEVELID'])
                    })
                except Exception as e:
                    print(e)
            if (i % 100000) == 0: print("\tProcess:", i)
        else: print("\tProcess:", i)
        return arr
    
    def parse_obj_levels(self, path):
        print("LEVELS:", self.db['levels'].drop())

        tree = ET.iterparse(path)
        
        for event, elem in tree:
            if elem.tag == 'OBJECTLEVEL':
                self.db['levels'].insert_one({
                    "_id": int(elem.attrib['LEVEL']),
                    'name': elem.attrib['NAME']
                })

    def parse_addr_obj(self, path):
        tree = ET.iterparse(path)
        i = 0
        for event, elem in tree:
            i += 1
            if elem.tag == 'OBJECT' and elem.attrib['ISACTUAL'] == "1" and elem.attrib['ISACTIVE'] == "1":
                try:
                    self.upd[int(elem.attrib['OBJECTID'])] = {
                        "_id": int(elem.attrib['ID']),
                        "objectId": int(elem.attrib['OBJECTID']),
                        "name": elem.attrib['NAME'],
                        "typename": elem.attrib['TYPENAME'],
                        "level": int(elem.attrib['LEVEL'])
                    }
                except Exception as e:
                    print(e)
            if (i % 100000) == 0: print("\tProcess:", i)
        else: print("\tProcess:", i)           
    
    def parse_objs(self, path, type):
        print('parse_objs:', type)
        tree = ET.iterparse(path)
        i = 0
        for event, elem in tree:
            i += 1
            if elem.tag == type and elem.attrib['ISACTUAL'] == "1" and elem.attrib['ISACTIVE'] == "1":
                try:
                    dict = {
                        "_id": int(elem.attrib['ID']),
                        "objectId": int(elem.attrib['OBJECTID']),
                        "objectGUID": elem.attrib['OBJECTGUID']
                    }

                    if type == 'HOUSE':
                        if ('HOUSENUM' in elem.attrib.keys()): dict["houseNum"] = elem.attrib['HOUSENUM']
                        if ('ADDNUM1' in elem.attrib.keys()): dict["addNum1"] = elem.attrib['ADDNUM1']
                        if ('ADDNUM2' in elem.attrib.keys()): dict["addNum2"] = elem.attrib['ADDNUM2']
                        if ('HOUSETYPE' in elem.attrib.keys()): dict["houseType"] = self.houseTypes[elem.attrib['HOUSETYPE']]
                        if ('ADDTYPE1' in elem.attrib.keys()): dict["addType1"] = self.addhouseTypes[elem.attrib['ADDTYPE1']]
                        if ('ADDTYPE2' in elem.attrib.keys()): dict["addType2"] = self.addhouseTypes[elem.attrib['ADDTYPE2']]
                    else: 
                        dict["number"] = elem.attrib['NUMBER']
                    
                    if type == 'APARTMENT' and elem.attrib['APARTTYPE'] != '0': dict["aparttype"] = self.apartTypes[elem.attrib['APARTTYPE']]
                    if type == 'ROOM': dict["roomtype"] = self.roomTypes[elem.attrib['ROOMTYPE']]

                    self.upd[int(elem.attrib['OBJECTID'])] = dict
                except Exception as e:
                    print(elem.attrib['OBJECTID'], ":", e)
            if (i % 100000) == 0: print("\tProcess:", i)
        else: print("\tProcess:", i) 

    def parse_params(self, path):
        print('parse_params')

        tree = ET.iterparse(path)

        i = 0
        for event, elem in tree:
            i += 1
            if elem.tag == 'PARAM' and elem.attrib['CHANGEIDEND'] == "0" and int(elem.attrib['OBJECTID']) in self.upd:
                try:
                    if elem.attrib['TYPEID'] == "6": self.upd[int(elem.attrib['OBJECTID'])]['OKATO'] = elem.attrib['VALUE']
                    elif elem.attrib['TYPEID'] == "7":self.upd[int(elem.attrib['OBJECTID'])]['OKTMO'] = elem.attrib['VALUE']
                    elif elem.attrib['TYPEID'] == "10": self.upd[int(elem.attrib['OBJECTID'])]['CODE'] = elem.attrib['VALUE']
                    elif elem.attrib['TYPEID'] == "8": self.upd[int(elem.attrib['OBJECTID'])]['CadNum'] = elem.attrib['VALUE']
                except Exception as e:
                    print("Error:", e)
            if event == "end":
                elem.clear()
            if (i % 1000000) == 0: print("\tProcess:", i)
        else: print("\tProcess:", i)
    
    def parse_hierarchy(self, path, type):
        print('parse_hierarchy:', type)
        tree = ET.iterparse(path, events=("start", "end",))
        
        i = 0
        for event, elem in tree:
            i += 1
            if elem.tag == 'ITEM' and elem.attrib['ISACTIVE'] == "1" and int(elem.attrib['OBJECTID']) in self.upd:
                try:
                    if ('PARENTOBJID' in elem.attrib.keys()): self.upd[int(elem.attrib['OBJECTID'])][f'{type}_parentId'] = int(elem.attrib['PARENTOBJID'])
                    self.upd[int(elem.attrib['OBJECTID'])][f'{type}_path'] = [int(e) for e in elem.attrib['PATH'].split('.')]
                except Exception as e:
                    print(e)
            if event == "end":
                elem.clear()
            if (i % 1000000) == 0: print("\tProcess:", i)
        else: print("\tProcess:", i)

parse_xml('\\gar-xml\\', '87')