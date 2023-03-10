import sys
import math
import json
import string
import random
import uuid
import datetime
import os

def gen_string(low=5, high=100, maxValueFlag=False) -> string:
    value = ''
    if maxValueFlag:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
    else:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(low, high)))

    return value

def gen_float(maxValueFlag=False) -> float:
    if not maxValueFlag:
        value = round(random.uniform(100.0, 100.0), 2)
    else:
        value = round(random.uniform(0, 100.0), 2)
    return value

def gen_integer(maxValueFlag=False) -> int:
    if maxValueFlag:
        value = random.randint(100, 100)
    else:
        value = random.randint(0, 100)
    return value

#https://gist.github.com/rg3915/db907d7455a4949dbe69
def gen_date(min_year=1900, max_year=datetime.datetime.now().year) -> string:
    start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + datetime.timedelta(days=365 * years)
    return datetime.datetime.strftime(start + (end - start) * random.random(), '%Y-%m-%d')

#https://gist.github.com/rg3915/db907d7455a4949dbe69
def gen_datetime(min_year=1900, max_year=datetime.datetime.now().year) -> string:
    start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + datetime.timedelta(days=365 * years)
    return datetime.datetime.strftime(start + (end - start) * random.random(), '%Y-%m-%d %H:%M:%S')

#https://stackoverflow.com/questions/38397285/iterate-over-all-items-in-json-object
def recursive_iter(obj, keys=()) -> tuple[set, str]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from recursive_iter(v, keys + (k,))
    elif any(isinstance(obj, t) for t in (list, tuple)):
        for idx, item in enumerate(obj):
            yield from recursive_iter(item, keys + (idx,))
    else:
        yield keys, obj

#https://stackoverflow.com/questions/30648317/programmatically-accessing-arbitrarily-deeply-nested-values-in-a-dictionary
def deep_access(d, keylist) -> dict:
     val = d
     for key in keylist:
         val = val[key]
     return val

#https://stackoverflow.com/questions/21297475/set-a-value-deep-in-a-dict-dynamically
def deep_set(d, keylist, value) -> dict:
    val = d
    latest = keylist.pop()
    prevKey = ''
    for key in keylist:
        if isinstance(key, int):
            # val[prevKey] = [{latest: ''}]
            # val = val.setdefault(prevKey, [])
            val = val.setdefault(prevKey, []) # if the key doesn't exist, add to the master dictionary, else get the value for the key
        else:
            val = val.setdefault(key, {})
        prevKey = key
    if isinstance(val, list):
        val.append({latest: value})
    else:
        val.setdefault(latest, value)

def get_payload_definition() -> list:

    with open(os.path.join(os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0], 'SampleJSON.json')) as file:
        samplePayloadDict = json.load(file)

    jsonAttributePathList = list()
    for keys, item in recursive_iter(samplePayloadDict):
        # print(keys, item)
        jsonAttributePathList.append([list(keys), item])

    return jsonAttributePathList

def gen_payload(jsonAttributePathList, seed=0, maxValueFlag=False) -> str:
    masterDict = dict()
    for jsonAttributePath in jsonAttributePathList:
        # if jsonAttributePath == 
        item = jsonAttributePath[1]
        if item == 'string':
            value = gen_string(maxValueFlag)
        elif item == "guid":
            value = uuid.uuid4()
        elif item == "float":
            value = gen_float(maxValueFlag)
        elif item == "integer":
            value = gen_integer(maxValueFlag)
        elif item == "date":
            # value = datetime.datetime.strftime(datetime.datetime(random.randint(2020, 2023), random.randint(1, 12), random.randint(1, 28)), '%Y-%m-%d')
            value = gen_date(min_year=2020)
        elif item == 'datetime':
            # value = datetime.datetime.strftime(datetime.datetime(random.randint(2020, 2023), random.randint(1, 12), random.randint(1, 28), random.randint(1, 23), random.randint(0, 59), random.randint(0, 60))
            #     , '%Y-%m-%d %H:%M:%S')
            value = gen_datetime(min_year=2020)
        else:
            value = item
        deep_set(masterDict, jsonAttributePath[0][:], str(value))

    # print(json.dumps(masterDict, indent=4))
    
    return masterDict

def get_batch_specs(TargetThroughput:int) -> str:
    print(f'Parameter - Target Throughput - {TargetThroughput}')
    payloadDefinitionList = get_payload_definition()

    print(payloadDefinitionList)

    eventString = gen_payload(jsonAttributePathList=payloadDefinitionList, maxValueFlag=True)

    return eventString

if __name__ == '__main__':

    TargetThroughput = 10 #100001 #thoughput messages/sec

    batchSpecDict = get_batch_specs(TargetThroughput=TargetThroughput)

    print(json.dumps(batchSpecDict, indent=4))

