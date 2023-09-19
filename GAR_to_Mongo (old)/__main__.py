import time
import xml.etree.ElementTree as ET
import re, os, sys

from multiprocessing import Process
from threading import Thread

from update_field import update_field
from db import *

def main(argv):
    parse_xml('D:\\Documents\\АСКОН\\Автоопр\\gar-xml\\', '86')

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
        
        print(self.file_types)
        self.region = get_database(self.region_code)

        # self.file_types['AS_OBJECT_LEVELS'] and self.parse_obj_levels(path + self.file_types['AS_OBJECT_LEVELS'])
        # self.file_types['AS_PARAM_TYPES'] and self.parse_param_types(path + self.file_types['AS_PARAM_TYPES'])

        self.file_types['AS_ADDR_OBJ'] and self.parse_addr_obj(self.regionPath + self.file_types['AS_ADDR_OBJ'])
        self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm')
        self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun')
        self.file_types['AS_ADDR_OBJ_PARAMS'] and self.parse_params(self.regionPath + self.file_types['AS_ADDR_OBJ_PARAMS'], 'ADDR_OBJ')
        
        # # if self.file_types['AS_ADM_HIERARCHY']:
        # adm = Thread(target=self.parse_hierarchy, args=(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm',))
        # # if self.file_types['AS_MUN_HIERARCHY']:
        # mun = Thread(target=self.parse_hierarchy, args=(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun',))
        # # if self.file_types['AS_ADDR_OBJ_PARAMS']:
        # params = Thread(target=self.parse_params, args=(self.regionPath + self.file_types['AS_ADDR_OBJ_PARAMS'], 'ADDR_OBJ',))

        # adm.start()
        # mun.start()
        # params.start()
        # adm.join()
        # mun.join()
        # params.join()
        
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
        'AS_PARAM_TYPES': ''
    }
    
    def parse_param_types(self, path):
        table_name = 'public."PARAM_TYPES"'

        self.truncate(table_name)

        tree = ET.iterparse(path)
        sql = f"""INSERT INTO {table_name}(
                "ID", 
                "NAME", 
                "DESC", 
                "CODE"
                )
            VALUES (%s,%s,%s,%s) 
            RETURNING "ID";"""
        for event, elem in tree:
            if elem.tag == 'PARAMTYPE':
                print(f'AS_PARAM_TYPES: ' + str(self.insert((
                    elem.attrib['ID'],
                    elem.attrib['NAME'],
                    elem.attrib['DESC'],
                    elem.attrib['CODE']
                    ),
                    sql
                )))
    
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
        print("ADDR_OBJ:", self.region.drop())

        tree = ET.iterparse(path)

        arr = []
        
        for event, elem in tree:
            if elem.tag == 'OBJECT' and elem.attrib['ISACTUAL'] == "1" and elem.attrib['ISACTIVE'] == "1":
                # print('ADDR_OBJ: ', self.region.insert_one({
                try:
                    arr.append({
                        "_id": int(elem.attrib['ID']),
                        "objectId": int(elem.attrib['OBJECTID']),
                        "name": elem.attrib['NAME'],
                        "typename": elem.attrib['TYPENAME'],
                        "level": int(elem.attrib['LEVEL'])
                        })
                except Exception as e:
                    print(e)
        else: self.region.insert_many(arr)
    
    def parse_params(self, path, type):
        print('parse_params')

        tree = ET.iterparse(path)

        # OKATO = update_field(self.region_code, 'OKATO')
        # OKTMO = update_field(self.region_code, 'OKTMO')
        # CODE = update_field(self.region_code, 'CODE')

        params = update_field(self.region_code)
        
        for event, elem in tree:
            if elem.tag == 'PARAM' and elem.attrib['CHANGEIDEND'] == "0":
                try:
                    if elem.attrib['TYPEID'] == "6":
                        # OKATO.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                        params.add_value(int(elem.attrib['OBJECTID']), 'OKATO', elem.attrib['VALUE'])
                    elif elem.attrib['TYPEID'] == "7":
                        # OKTMO.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                        params.add_value(int(elem.attrib['OBJECTID']), 'OKTMO', elem.attrib['VALUE'])
                    elif elem.attrib['TYPEID'] == "10":
                        # CODE.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                        params.add_value(int(elem.attrib['OBJECTID']), 'CODE', elem.attrib['VALUE'])
                except Exception as e:
                    print("Error:", e)
            if event == "end":
                elem.clear()
        else:
            params.update_many()
            # proc_OKATO = Process(target=OKATO.update_many)
            # proc_OKATO.start()
            # proc_OKTMO = Process(target=OKTMO.update_many)
            # proc_OKTMO.start()
            # proc_CODE = Process(target=CODE.update_many)
            # proc_CODE.start()

            # proc_OKATO.join()
            # proc_OKTMO.join()
            # proc_CODE.join()
    
    def parse_hierarchy(self, path, type):
        self.region.update_many({}, {'$unset': {f'{type}_parentId': ""}})
        
        print('parse_hierarchy:', type)
        tree = ET.iterparse(path, events=("start", "end",))

        # hierarchy = update_field(self.region_code, f'{type}_parentId')
        hierarchy = update_field(self.region_code)
        
        for event, elem in tree:
            if elem.tag == 'ITEM' and elem.attrib['ISACTIVE'] == "1":
                try:
                    # hierarchy.add_value(int(elem.attrib['OBJECTID']), int(elem.attrib['PARENTOBJID']))
                    hierarchy.add_value(int(elem.attrib['OBJECTID']), f'{type}_parentId', int(elem.attrib['PARENTOBJID']))
                except Exception as e:
                    print(e)

            if event == "end":
                elem.clear()
        else:
            hierarchy.update_many()
    

if __name__ == '__main__':
  main(sys.argv)
