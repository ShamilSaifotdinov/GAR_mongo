import xml.etree.ElementTree as ET
import psycopg2
import os
import time
import re

conn = psycopg2.connect(
    host="localhost",
    database="GAR",
    user="postgres",
    password="123")

conn.autocommit = True
cur = conn.cursor()

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

        # self.file_types['AS_OBJECT_LEVELS'] and self.parse_obj_levels(path + self.file_types['AS_OBJECT_LEVELS'])
        # self.file_types['AS_PARAM_TYPES'] and self.parse_param_types(path + self.file_types['AS_PARAM_TYPES'])

        # self.file_types['AS_ADDR_OBJ'] and self.parse_addr_obj(self.regionPath + self.file_types['AS_ADDR_OBJ'])
        # self.file_types['AS_ADDR_OBJ_PARAMS'] and self.parse_params(self.regionPath + self.file_types['AS_ADDR_OBJ_PARAMS'], 'ADDR_OBJ')
        # self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'ADM')
        self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'MUN')
        
        print("Start: ", time.ctime(startTime))
        endTime = time.time()
        print("End: ", time.ctime(endTime))
        print("Density: ", endTime - startTime)

        conn.commit()
        conn.close()
    
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
        table_name = 'public."OBJECT_LEVELS"'

        self.truncate(table_name)

        tree = ET.iterparse(path)
        sql = f"""INSERT INTO {table_name}(
                "LEVEL", 
                "NAME"
                )
            VALUES (%s,%s) 
            RETURNING "LEVEL";"""
        for event, elem in tree:
            if elem.tag == 'OBJECTLEVEL':
                print(f'AS_OBJECT_LEVELS: ' + str(self.insert((
                    elem.attrib['LEVEL'],
                    elem.attrib['NAME']
                    ),
                    sql
                )))
    
    def parse_params(self, path, type):
        table_name = f'_{self.region_code}."{type}_PARAMS"'

        self.truncate(table_name)

        tree = ET.iterparse(path)
        sql = f"""INSERT INTO {table_name}(
                "ID", 
                "OBJECTID", 
                "CHANGEID", 
                "CHANGEIDEND", 
                "TYPEID", 
                "VALUE",
                "UPDATEDATE",
                "STARTDATE",
                "ENDDATE"
                )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) 
            RETURNING "ID";"""
        for event, elem in tree:
            if elem.tag == 'PARAM':
                print(f'AS_{type}_PARAMS: ' + str(self.insert((
                    elem.attrib['ID'],
                    elem.attrib['OBJECTID'], 
                    elem.attrib['CHANGEID'] if 'CHANGEID' in elem.attrib else None,
                    elem.attrib['CHANGEIDEND'],
                    elem.attrib['TYPEID'],
                    elem.attrib['VALUE'],
                    elem.attrib['UPDATEDATE'],
                    elem.attrib['STARTDATE'],
                    elem.attrib['ENDDATE']
                    ),
                    sql
                )))
    
    def parse_hierarchy(self, path, type):
        table_name = f' _{self.region_code}."{type}_HIERARCHY"'

        self.truncate(table_name)

        tree = ET.iterparse(path)
        sql = f"""INSERT INTO {table_name}("ID", "OBJECTID", "PARENTOBJID", "ISACTIVE", "PATH") 
                        VALUES (%s,%s,%s,%s,%s) 
                        RETURNING "ID";"""
        for event, elem in tree:
            if elem.tag == 'ITEM':
                print(f'AS_{type}_HIERARCHY: ' + str(self.insert((
                    elem.attrib['ID'],
                    elem.attrib['OBJECTID'], 
                    elem.attrib['PARENTOBJID'] if 'PARENTOBJID' in elem.attrib else None,
                    False if elem.attrib['ISACTIVE'] == "0" 
                        else (True if elem.attrib['ISACTIVE'] == "1" else None),
                    elem.attrib['PATH']
                    ),
                    sql
                )))

    def parse_addr_obj(self, path):
        table_name = f'_{self.region_code}."ADDR_OBJ"'

        self.truncate(table_name)

        tree = ET.iterparse(path)
        sql = f"""INSERT INTO {table_name}("ID", "OBJECTID", "NAME", "TYPENAME", "LEVEL", "ISACTUAL", "ISACTIVE") 
                        VALUES (%s,%s,%s,%s,%s,%s,%s) 
                        RETURNING "ID";"""
        for event, elem in tree:
            if elem.tag == 'OBJECT':
                print('ADDR_OBJ: ' + str(self.insert((
                    elem.attrib['ID'],
                    elem.attrib['OBJECTID'],
                    elem.attrib['NAME'],
                    elem.attrib['TYPENAME'],
                    elem.attrib['LEVEL'],
                    False if elem.attrib['ISACTUAL'] == "0" else True,
                    False if elem.attrib['ISACTIVE'] == "0" else True
                    ),
                    sql                    
                )))

    def insert(self, new_data, sql):
        try:
            cur.execute(sql, new_data)

            row_id = cur.fetchone()[0]

        except (Exception, psycopg2.DatabaseError) as error:
            print('ERROR psql: ', error)
        
        return row_id

    def truncate(self, name_table):
        try:
            print(cur.rowcount)
            cur.execute(f'TRUNCATE {name_table} RESTART IDENTITY')
            print(cur.rowcount)
        except (Exception, psycopg2.DatabaseError) as error:
            print('ERROR psql: ', error)

parse_xml('D:\\Documents\\АСКОН\\Автоопр\\gar-xml\\', '66')