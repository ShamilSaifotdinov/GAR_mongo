from pymongo import MongoClient
from multiprocessing import Pool, cpu_count
import csv, sys

chunk_size = 1000

def main(argv):    
    rows = get_rows()
    # writing to csv file
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile, delimiter=';', quoting=csv.QUOTE_ALL)
        
        # writing the fields
        csvwriter.writerow(fields)
        
        # writing the data rows
        csvwriter.writerows(rows)

def get_chunks(arr):
    step = chunk_size
    chunks = []
    for i in range(0, len(arr), step):
        chunks.append(arr[i:min(i + step, len(arr))])
    
    # for elem in chunks: print("\tElem:", str(len(elem)))
    print("\tChunks:", str(len(chunks)))
    return chunks

def get_col(region_code, colName):
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb://localhost:27017/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client[f'GAR{region_code}'][colName]

def get_elem(objId):
    level = reestr.find_one({'objectId': objId}, {'_id': 0, 'level': 1})['level']
    str = ''
    if level == 12:
        # room
        query = getRoom.find_one({'objectId': objId},{'_id': 0, 'objectId': 1, 'number': 1, 'roomtype': 1})
        str = f'{get_value(query, "roomtype")} {get_value(query, "number")}'
    elif level == 11:
        # apart
        query = getApart.find_one({'objectId': objId},{'_id': 0, 'objectId': 1, 'number': 1, 'aparttype': 1})
        str = f'{get_value(query, "aparttype")} {get_value(query, "number")}'
    elif level == 10:
        # house
        query = getHouse.find_one({'objectId': objId},{'_id': 0, 'objectId': 1,
            'houseNum': 1, 'houseType': 1,
            'addNum1': 1, 'addType1': 1,
            'addNum2': 1, 'addType2': 1
        })
        str = f'{get_value(query, "houseType")} {get_value(query, "houseNum")} ({get_value(query, "addType1")} {get_value(query, "addNum1")}, {get_value(query, "addType2")} {get_value(query, "addNum2")})'
    elif level == 9:
        # stead
        query = getStead.find_one({'objectId': objId},{'_id': 0, 'objectId': 1, 'number': 1})
        str = f'{get_value(query, "number")}'
    elif level == 17:
        # carplace
        query = getCarplace.find_one({'objectId': objId},{'_id': 0, 'objectId': 1, 'number': 1})
        str = f'{get_value(query, "number")}'
    else: 
        # addr
        query = getAddr.find_one({'objectId': objId},{'_id': 0, 'objectId': 1, 'typename': 1, 'name': 1})
        str = f'{get_value(query, "name")} {get_value(query, "typename")}'
    return str

def get_value(dict, key):
    if not(key in dict.keys()):
        return ''
    else: 
        return dict[key]

def note(arr):
    # arr = objsCol.find_one({'objectId': objId}, {'_id': 0, f'{type}_path': 1})[f'{type}_path']
    return ', '.join([get_elem(elem) for elem in arr])

def get_row(objs):
    arr = []
    for e in objs:
        try:
            arr.append([e['CadNum'] if 'CadNum' in e else '', e['objectGUID'], note(e[f'{type}_path'])])
        except Exception as e:
            print(e)
    print(f"Ready {chunk_size}!")
    return arr

def get_rows():
    objs = objsCol.find({},{'_id': 0, 'objectGUID': 1, f'{type}_path': 1, 'CadNum': 1})
    # i = 0
    arr = []
    # for e in objs:
    #     try:
    #         arr.append([e['CadNum'] if 'CadNum' in e else '', e['objectGUID'], note(e[f'{type}_path'])])
    #     except Exception as e:
    #         print(e)
        
    #     i += 1
    #     if (i % 100) == 0: print("Process:", i)

    # print("Process:", i)
    
    chunks = get_chunks(list(objs))

    p = Pool(processes=cpu_count())
    data = p.map(get_row, chunks)
    p.close()
    
    for elem in data: arr += elem
    print(len(arr))
    
    return arr

regionCode = '87'
typeOfObjs = 'house'

objsCol = get_col(regionCode, typeOfObjs)

getRoom = get_col(regionCode, 'room')
getApart = get_col(regionCode, 'apartment')
getHouse = get_col(regionCode, 'house')
getStead = get_col(regionCode, 'stead')
getCarplace = get_col(regionCode, 'carplace')
getAddr = get_col(regionCode, 'addr')
reestr = get_col(regionCode, 'reestr')

type = 'mun'
filename = f'{regionCode} {type} {typeOfObjs}.csv'


"""

obj = objsCol.find_one({"objectId": 103381665},{'_id': 0})
print(obj)

path = obj[f'{type}_path'][:-1]
print(path)

elems = list(regionCol.aggregate([
    {'$match': { 'objectId': {'$in': path}}},
    { '$project': { '_id': 0, 'level': 1, 'name': 1, 'typename': 1 } },
    {'$sort': {'level': 1}}
]))

print('Count:', len(elems), len(elems) == len(path))
result = ', '.join([f'{elem["typename"]} {elem["name"]}' for elem in elems]) + ', ' + obj['number']
# print(elems)
print(result)

"""

# objs = objsCol.find({},{'_id': 0, 'objectGUID': 1, f'{type}_path': 1, 'CadNum': 1})
# addr = {item['objectId']:item for item in regionCol.find({},{'_id': 0, 'objectId': 1, 'typename': 1, 'name': 1})}

# def getRow(arr):
#     elems = [addr[e] for e in arr]
#     return ', '.join([f'{elem["typename"]} {elem["name"]}' for elem in elems])

fields = ['Cad_num', 'GUID', 'Addr_note']
# rows = [[e['CadNum'] if 'CadNum' in e else '', e['objectGUID'], getRow(e[f'{type}_path'][:-1]) + ', ' + e['number']] for e in objs]
# rows = [
#     [e['CadNum'] if 'CadNum' in e else '', e['objectGUID'], note(e[f'{type}_path'])]
#     for e in objs
# ]

# print(note(4904027))

if __name__ == '__main__':
  main(sys.argv)