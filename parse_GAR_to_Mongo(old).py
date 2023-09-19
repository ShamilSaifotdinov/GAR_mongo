from pymongo import MongoClient
import time
import xml.etree.ElementTree as ET
import re
import os

def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb://localhost:27017/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['GAR']

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
        self.db = get_database()
        self.region = self.db[self.region_code]
        # self.file_types['AS_OBJECT_LEVELS'] and self.parse_obj_levels(path + self.file_types['AS_OBJECT_LEVELS'])
        # self.file_types['AS_PARAM_TYPES'] and self.parse_param_types(path + self.file_types['AS_PARAM_TYPES'])

        # self.file_types['AS_ADDR_OBJ'] and self.parse_addr_obj(self.regionPath + self.file_types['AS_ADDR_OBJ'])
        # self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm')
        self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun')
        # self.file_types['AS_ADDR_OBJ_PARAMS'] and self.parse_params(self.regionPath + self.file_types['AS_ADDR_OBJ_PARAMS'], 'ADDR_OBJ')
        
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
    
    def parse_params(self, path, type):        
        print('parse_params')

        tree = ET.iterparse(path)
        
        for event, elem in tree:
            if elem.tag == 'PARAM' and elem.attrib['CHANGEIDEND'] == "0":
                try:
                    if elem.attrib['TYPEID'] == "6":
                        self.region.update_one(
                            { 'objectId': int(elem.attrib['OBJECTID']) },
                            {
                                '$set': {
                                    'OKATO': elem.attrib['VALUE']
                                }                            
                            }
                        )
                    elif elem.attrib['TYPEID'] == "7":
                        self.region.update_one(
                            { 'objectId': int(elem.attrib['OBJECTID']) },
                            {
                                '$set': {
                                    'OKTMO': elem.attrib['VALUE']
                                }                            
                            }
                        )
                    elif elem.attrib['TYPEID'] == "10":
                        self.region.update_one(
                            { 'objectId': int(elem.attrib['OBJECTID']) },
                            {
                                '$set': {
                                    'CODE': elem.attrib['VALUE']
                                }                            
                            }
                        )
                except Exception as e:
                    print(e)
    
    def parse_hierarchy(self, path, type):
        # table_name = f' _{self.region_code}."{type}_HIERARCHY"'

        # self.truncate(table_name)
        print('parse_hierarchy:', type)
        tree = ET.iterparse(path)
        
        for event, elem in tree:
            if elem.tag == 'ITEM' and elem.attrib['ISACTIVE'] == "1":
                try:
                    self.region.update_one(
                        { 'objectId': int(elem.attrib['OBJECTID']) },
                        {
                            '$set': {
                                f'{type}_parentId': int(elem.attrib['PARENTOBJID']) 
                                # if 'PARENTOBJID' in elem.attrib else None
                            }                            
                        }
                    )
                except Exception as e:
                    print(e)

    def parse_addr_obj(self, path):
        print("ADDR_OBJ:", self.region.drop())

        tree = ET.iterparse(path)
        
        for event, elem in tree:
            if elem.tag == 'OBJECT' and elem.attrib['ISACTUAL'] == "1" and elem.attrib['ISACTIVE'] == "1":
                # print('ADDR_OBJ: ', self.region.insert_one({
                try:
                    # print(
                    self.region.insert_one({
                        "_id": int(elem.attrib['ID']),
                        "objectId": int(elem.attrib['OBJECTID']),
                        "name": elem.attrib['NAME'],
                        "typename": elem.attrib['TYPENAME'],
                        "level": int(elem.attrib['LEVEL'])
                        })            
                    # )
                except Exception as e:
                    print(e)

"""
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
"""

parse_xml('D:\\Documents\\АСКОН\\Автоопр\\gar-xml\\', '25')

"""
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
#    Get the database
   dbname = get_database()
   collection_name = dbname["87"]
   collection_name.insert_many([item_1,item_2])
   item_details = collection_name.find()
   for item in item_details:
        # This does not give a very readable output
        print(item)
"""