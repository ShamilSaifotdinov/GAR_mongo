from pymongo import MongoClient, UpdateOne
from multiprocessing import Pool, cpu_count
import time
import xml.etree.ElementTree as ET
import re
import os
import sys

parse_region = '86'

def main(argv):
    parse_xml('D:\\Documents\\АСКОН\\Автоопр\\gar-xml\\', parse_region)

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
        self.file_types['AS_ADM_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_ADM_HIERARCHY'], 'adm')
        # self.file_types['AS_MUN_HIERARCHY'] and self.parse_hierarchy(self.regionPath + self.file_types['AS_MUN_HIERARCHY'], 'mun')
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

        OKATO = update_field(self.region, 'OKATO')
        OKTMO = update_field(self.region, 'OKTMO')
        CODE = update_field(self.region, 'CODE')
        
        for event, elem in tree:
            if elem.tag == 'PARAM' and elem.attrib['CHANGEIDEND'] == "0":
                try:
                    if elem.attrib['TYPEID'] == "6":
                        # self.region.update_one(
                        #     { 'objectId': int(elem.attrib['OBJECTID']) },
                        #     {
                        #         '$set': {
                        #             'OKATO': elem.attrib['VALUE']
                        #         }                            
                        #     }
                        # )
                        OKATO.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                    elif elem.attrib['TYPEID'] == "7":
                        # self.region.update_one(
                        #     { 'objectId': int(elem.attrib['OBJECTID']) },
                        #     {
                        #         '$set': {
                        #             'OKTMO': elem.attrib['VALUE']
                        #         }                            
                        #     }
                        # )
                        OKTMO.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                    elif elem.attrib['TYPEID'] == "10":
                        # self.region.update_one(
                        #     { 'objectId': int(elem.attrib['OBJECTID']) },
                        #     {
                        #         '$set': {
                        #             'CODE': elem.attrib['VALUE']
                        #         }                            
                        #     }
                        # )
                        CODE.add_value(int(elem.attrib['OBJECTID']), elem.attrib['VALUE'])
                except Exception as e:
                    print(e)
            if event == "end":
                elem.clear()
        else:
            OKATO.update_many()
            OKTMO.update_many()
            CODE.update_many()
    
    def parse_hierarchy(self, path, type):
        self.region.update_many({}, {'$unset': {f'{type}_parentId': ""}})
        
        print('parse_hierarchy:', type)
        tree = ET.iterparse(path, events=("start", "end",))

        hierarchy = update_field(self.region, f'{type}_parentId')

        # i = 0
        # updCnt = 0
        # newValues = {}
        
        for event, elem in tree:
            if elem.tag == 'ITEM' and elem.attrib['ISACTIVE'] == "1":
                try:
                    hierarchy.add_value(int(elem.attrib['OBJECTID']), int(elem.attrib['PARENTOBJID']))
                # pass                    

                    """
                    newValues[int(elem.attrib['OBJECTID'])] = int(elem.attrib['PARENTOBJID'])
                    i += 1
                    if (i % 10000) == 0: print("Process: " + str(i))
                    if (i % 1000000) == 0:
                        lsObjs = self.region.find({'objectId': {"$in": list(newValues.keys())}})
                        objIds = [elem["objectId"] for elem in lsObjs]
                        
                        arr = []
                        for id in objIds:    
                            arr.append(UpdateOne(
                                { 'objectId': id },
                                {
                                    '$set': {
                                        f'{type}_parentId': newValues[id]
                                        # if 'PARENTOBJID' in elem.attrib else None
                                    }                            
                                }))
                        self.region.bulk_write(arr)
                        updCnt += len(objIds)
                        print(str(updCnt) + ' updated!')
                        newValues = {}
                    
                    """
                
                except Exception as e:
                    print(e)

            if event == "end":
                elem.clear()
        else:
            hierarchy.update_many()

        """
        # try:
        #     print("Process: " + str(i))
            
        #     lsObjs = self.region.find({'objectId': {"$in": list(newValues.keys())}})
        #     objIds = [elem["objectId"] for elem in lsObjs]
            
        #     arr = []
        #     for id in objIds:    
        #         arr.append(UpdateOne(
        #             { 'objectId': id },
        #             {
        #                 '$set': {
        #                     f'{type}_parentId': newValues[id]
        #                     # if 'PARENTOBJID' in elem.attrib else None
        #                 }                            
        #             }))
        #     self.region.bulk_write(arr)
        #     updCnt += len(objIds)
        #     print(str(updCnt) + ' updated!')
        #     newValues = {}
        # except Exception as e:
        #     print(e)
        """
    
    def parse_addr_obj(self, path):
        print("ADDR_OBJ:", self.region.drop())

        tree = ET.iterparse(path)

        arr = []
        
        for event, elem in tree:
            if elem.tag == 'OBJECT' and elem.attrib['ISACTUAL'] == "1" and elem.attrib['ISACTIVE'] == "1":
                # print('ADDR_OBJ: ', self.region.insert_one({
                try:
                    # print(
                    # self.region.insert_one({
                    #     "_id": int(elem.attrib['ID']),
                    #     "objectId": int(elem.attrib['OBJECTID']),
                    #     "name": elem.attrib['NAME'],
                    #     "typename": elem.attrib['TYPENAME'],
                    #     "level": int(elem.attrib['LEVEL'])
                    #     })            
                    # )
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

def write_update(arr):
        db = get_database()
        region = db[parse_region]
        region.bulk_write(arr, ordered=False)

class update_field:
    def __init__(self, region, field):
        self.i = 0
        self.updCnt = 0
        self.newValues = {}
        self.region = region
        self.field = field

    def add_value(self, objId, parentId):
        self.newValues[objId] = parentId
        self.i += 1
        if (self.i % 100000) == 0: print("\tProcess: " + str(self.i))
        if (self.i % 1000000) == 0: self.update_many()

    def update_many(self):
        try:
            print("\tProcess: " + str(self.i))
            
            lsObjs = self.region.find({'objectId': {"$in": list(self.newValues.keys())}})
            objIds = [elem["objectId"] for elem in lsObjs]
            
            arr = []
            for id in objIds:    
                arr.append(UpdateOne(
                    { 'objectId': id },
                    {
                        '$set': {
                            self.field: self.newValues[id]
                        }                            
                    }))
            print("\tarr length:", len(arr))
            # self.get_chunks(arr)
            chunks = self.get_chunks(arr)
            with Pool(cpu_count()) as p:
                p.map(write_update, chunks)
                p.close()
                p.join()

            # self.region.bulk_write(arr, ordered=False)
            self.updCnt += len(objIds)
            print("\t" + str(self.updCnt) + ' updated!')
            self.newValues = {}
        except Exception as e:
            print("\t" + e)
    
    def get_chunks(self, arr):
        elemCount = 100
        chunks = []
        for i in range(0, len(arr), elemCount):
            chunks.append(arr[i:min(i + elemCount, len(arr))])
        
        # for elem in chunks: print("\tElem:", str(len(elem)))
        print("\tChunks:", str(len(chunks)))
        return chunks

if __name__ == '__main__':
  main(sys.argv)

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