import sys
import math
import json
import string
import random
import uuid
import datetime

def gen_string(low=5, high=100, maxValueFlag=False) -> string:
    value = ''
    if maxValueFlag:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(low, high)))
    else:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=100))

    return value

def gen_float(maxValueFlag=False) -> float:
    if maxValueFlag:
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
    for key in keylist:
        val = val.setdefault(key, {})
    val.setdefault(latest, value)
     
def get_payload_definition() -> list:

    with open('main/sampleJSON.json') as file:
        samplePayloadDict = json.load(file)

    jsonAttributePathList = list()
    for keys, item in recursive_iter(samplePayloadDict):
        # print(keys, item)
        jsonAttributePathList.append([list(keys), item])

    return jsonAttributePathList

def gen_payload(jsonAttributePathList, seed=0, maxValueFlag=False) -> list:
    masterDict = dict()
    for jsonAttributePath in jsonAttributePathList:
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
        deep_set(masterDict, jsonAttributePath[0][:], str(value))

    # print(json.dumps(masterDict, indent=4))
    
    return str(masterDict)

def get_batch_specs(TargetThroughput:int) -> dict:
    print(f'Parameter - Target Throughput - {TargetThroughput}')
    payloadDefinitionList = get_payload_definition()

    eventString = gen_payload(jsonAttributePathList=payloadDefinitionList, maxValueFlag=True)

    print(f'EventString with max values - {eventString}')
    print(f'Message size (bytes) - {sys.getsizeof(eventString)}')
    MaxMessageSizeBytes = sys.getsizeof(eventString)

    MaxThroughputPerNode = round(((1*1000*1000) / MaxMessageSizeBytes) * 0.8) #multiplying by 0.8 just to give a little headroom

    if TargetThroughput < MaxThroughputPerNode:
        NodeThroughput = TargetThroughput
    else:
        NodeThroughput = math.floor(TargetThroughput/(math.ceil(TargetThroughput/MaxThroughputPerNode)))

    print(f'Max Node Throughput - {MaxThroughputPerNode}')
    print(f'Ideal Node Throughput - {NodeThroughput}')
    NumberOfNodes = 4 if math.floor(TargetThroughput/NodeThroughput) < 2 else 4 * math.floor(TargetThroughput/NodeThroughput) #default the number of nodes to 4
    # NodeThroughput = math.floor(NodeThroughput/4)
    print(f'Number of nodes - {NumberOfNodes}')
    print(f'Number of messages per sec in a batch per node - {NodeThroughput}')
    print(f'Message size (bytes) of a batch per node - {NodeThroughput * MaxMessageSizeBytes}')
    print(f'Target throughput Messages/Sec - {int(TargetThroughput)}')
    print(f'Basic Throughput Messages/Sec - {int(TargetThroughput/NodeThroughput) * NodeThroughput}')

    NodeMessageSpecList = list()
    NodesAboveAverage = int(TargetThroughput) - (int(TargetThroughput/NodeThroughput) * NodeThroughput)
    # print(NodesAboveAverage)
    MinNodeThroughput = math.floor(int(TargetThroughput) / (NumberOfNodes/4))
    MaxNodeThroughput = math.ceil(int(TargetThroughput) / (NumberOfNodes/4))
    for node in range(NumberOfNodes):
        if node%(NumberOfNodes/4) < NodesAboveAverage:
            NodeMessageSpecList.append({'NodeNum': str(node+1), 'NodeSec': math.floor(node/(NumberOfNodes/4)), 'NodeThroughput': str(NodeThroughput)+1})
        else:
            NodeMessageSpecList.append({'NodeNum': str(node+1), 'NodeSec': math.floor(node/(NumberOfNodes/4)), 'NodeThroughput': str(NodeThroughput)})


    myDict = dict()
    for nodespec in NodeMessageSpecList:
        if nodespec['NodeSec'] in myDict.keys():
            myDict[nodespec['NodeSec']] += nodespec['NodeThroughput']
        else:
            myDict[nodespec['NodeSec']] = nodespec['NodeThroughput']
    print(myDict)

    batchSpecDict = {
                    'PayloadDefinitionList': payloadDefinitionList
                    ,'NumberOfNodes': NumberOfNodes
                    ,'MinNodeThroughput': MinNodeThroughput
                    ,'MaxNodeThroughput': MaxNodeThroughput
                    ,'NodeMessageSpecList': NodeMessageSpecList
                    }
                    
    return batchSpecDict

if __name__ == '__main__':

    TargetThroughput = 1#100001 #thoughput messages/sec

    batchSpecDict = get_batch_specs(TargetThroughput=TargetThroughput)

    print(batchSpecDict)

    # #Speed Test
    # import time
    # start = time.time()
    # batchList = list()
    # for _ in range(batchSpecDict['NodeThroughput']):
    #     batchList.append(gen_payload(jsonAttributePathList=[_ for _ in batchSpecDict['PayloadDefinitionList']], maxValueFlag=False))
    # print(f'Duration to generate payload (sec) - {str(round(time.time() - start, 2))}')
    # # print(len(batchList))
    # # print(batchList)
