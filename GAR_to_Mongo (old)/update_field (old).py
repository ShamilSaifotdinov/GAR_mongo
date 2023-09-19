
from pymongo import UpdateOne
from multiprocessing import Pool, cpu_count
from db import *

class update_field:
    def __init__(self, region_code, field):
        self.i = 0
        self.updCnt = 0
        self.newValues = {}
        self.region_code = region_code
        self.field = field

    def add_value(self, objId, parentId):
        self.newValues[objId] = parentId
        self.i += 1
        if (self.i % 100000) == 0: print("\tProcess: " + str(self.i))
        if (self.i % 1000000) == 0: self.update_many()

    def update_many(self):
        try:
            print("\tProcess: " + str(self.i))
            
            lsObjs = get_database(self.region_code).find({'objectId': {"$in": list(self.newValues.keys())}})
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
                p.starmap(write_update, chunks)
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
            chunks.append((self.region_code, arr[i:min(i + elemCount, len(arr))]))
        
        # for elem in chunks: print("\tElem:", str(len(elem)))
        print("\tChunks:", str(len(chunks)))
        return chunks
