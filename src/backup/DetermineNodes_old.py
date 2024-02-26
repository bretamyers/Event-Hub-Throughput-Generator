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


#https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
def flatten_dict(dd, separator='||', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }


#https://stackoverflow.com/questions/6037503/python-unflatten-dict
def build_json_from_blueprint(field_dict):
    field_dict = dict(field_dict)
    new_field_dict = dict()
    field_keys = list(field_dict)
    field_keys.sort()

    for each_key in field_keys:
        field_value = field_dict[each_key]
        processed_key = str(each_key)
        current_key = None
        current_subkey = None
        for i in range(len(processed_key)):
            if processed_key[i] == "[":
                current_key = processed_key[:i]
                start_subscript_index = i + 1
                end_subscript_index = processed_key.index("]")
                current_subkey = int(processed_key[start_subscript_index : end_subscript_index])

                # reserve the remainder descendant keys to be processed later in a recursive call
                if len(processed_key[end_subscript_index:]) > 1:
                    current_subkey = "{}||{}".format(current_subkey, processed_key[end_subscript_index + 3:]) #update 2 -> 3
                break
            # next child key is a dictionary
            elif processed_key[i:i+2] == "||": #updated i -> i:i+2 and "." -> "||"
                split_work = processed_key.split("||", 1) #updated "." -> "||"
                if len(split_work) > 1:
                    current_key, current_subkey = split_work
                else:
                    current_key = split_work[0]
                break

        if current_subkey is not None:
            if current_key.isdigit():
                current_key = int(current_key)
            if current_key not in new_field_dict:
                new_field_dict[current_key] = dict()
            new_field_dict[current_key][current_subkey] = field_value
        else:
            new_field_dict[each_key] = field_value

    # Recursively unflatten each dictionary on each depth before returning back to the caller.
    all_digits = True
    highest_digit = -1
    for each_key, each_item in new_field_dict.items():
        if isinstance(each_item, dict):
            new_field_dict[each_key] = build_json_from_blueprint(each_item)

        # validate the keys can safely converted to a sequential list.
        all_digits &= str(each_key).isdigit()
        if all_digits:
            next_digit = int(each_key)
            if next_digit > highest_digit:
                highest_digit = next_digit

    # If all digits and can be sequential order, convert to list.
    if all_digits and highest_digit == (len(new_field_dict) - 1):
        digit_keys = list(new_field_dict)
        digit_keys.sort()
        new_list = []

        for k in digit_keys:
            i = int(k)
            if len(new_list) <= i:
                # Pre-populate missing list elements if the array index keys are out of order
                # and the current element is ahead of the current length boundary.
                while len(new_list) <= i:
                    new_list.append(None)
            new_list[i] = new_field_dict[k]
        new_field_dict = new_list
    return new_field_dict

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
        item = jsonAttributePath[1]
        # item = jsonAttributePath[1].split('}')[0] + '}'
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
    
    return str(masterDict)



def get_batch_specs(TargetThroughput:int) -> dict:
    print(f'Parameter - Target Throughput - {TargetThroughput}')
    payloadDefinitionList = get_payload_definition()

    eventString = gen_payload(jsonAttributePathList=payloadDefinitionList, maxValueFlag=True)

    # print(f'EventString with max values - {eventString}')
    print(f'Message size (bytes) - {sys.getsizeof(eventString)}')
    MaxMessageSizeBytes = sys.getsizeof(eventString)

    MaxThroughputPerNode = round(((1*1000*1000) / MaxMessageSizeBytes) * 0.8) #multiplying by 0.8 just to give a little headroom

    if TargetThroughput < MaxThroughputPerNode:
        NodeThroughput = TargetThroughput
    else:
        NodeThroughput = math.floor(TargetThroughput/(math.ceil(TargetThroughput/MaxThroughputPerNode)))

    print(f'Max Node Throughput - {MaxThroughputPerNode}')
    # print(f'Ideal Node Throughput - {NodeThroughput}')
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
            NodeMessageSpecList.append({'NodeNum': str(node+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(NodeThroughput)+1})
        else:
            NodeMessageSpecList.append({'NodeNum': str(node+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(NodeThroughput)})


    nodeBuckets = dict()
    for nodespec in NodeMessageSpecList:
        if nodespec['NodeSec'] in nodeBuckets.keys():
            nodeBuckets[nodespec['NodeSec']] += nodespec['NodeThroughput']
        else:
            nodeBuckets[nodespec['NodeSec']] = nodespec['NodeThroughput']
    print(f'Node Buckets (Node, Throughput) - {nodeBuckets}')

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