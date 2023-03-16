import sys
import math
import json
import random
import uuid
import faker
import os
import copy
import Helpers.TomlHelper
import DataFactory.PayloadFactory

# def gen_string(low=5, high=100, maxValueFlag=False) -> string:
#     value = ''
#     if maxValueFlag:
#         value = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
#     else:
#         value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(low, high)))

#     return value

# def gen_string_faker_text(low=5, high=100, seed=0, maxValueFlag=False) -> string:
#     fake = faker.Faker()
#     fake.seed(seed)
#     value = ''
#     if maxValueFlag:
#         value = fake.text(max_nb_chars=high)
#     else:
#         value = fake.text(max_nb_chars=random.randint(low, high))

# def gen_float(maxValueFlag=False) -> float:
#     if not maxValueFlag:
#         value = round(random.uniform(100.0, 100.0), 2)
#     else:
#         value = round(random.uniform(0, 100.0), 2)
#     return value


# def gen_integer(maxValueFlag=False) -> int:
#     if maxValueFlag:
#         value = random.randint(100, 100)
#     else:
#         value = random.randint(0, 100)
#     return value


# #https://gist.github.com/rg3915/db907d7455a4949dbe69
# def gen_date(min_year=1900, max_year=datetime.datetime.now().year) -> string:
#     start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
#     years = max_year - min_year + 1
#     end = start + datetime.timedelta(days=365 * years)
#     return datetime.datetime.strftime(start + (end - start) * random.random(), '%Y-%m-%d')


# #https://gist.github.com/rg3915/db907d7455a4949dbe69
# def gen_datetime(min_year=1900, max_year=datetime.datetime.now().year) -> string:
#     start = datetime.datetime(min_year, 1, 1, 00, 00, 00)
#     years = max_year - min_year + 1
#     end = start + datetime.timedelta(days=365 * years)
#     return datetime.datetime.strftime(start + (end - start) * random.random(), '%Y-%m-%d %H:%M:%S')


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


# # #https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
# # def flatten_dict_string(dd, separator='||', prefix=''):
# #     return { prefix + separator + k if prefix else k : v
# #              for kk, vv in dd.items()
# #              for k, v in flatten_dict_string(vv, separator, kk).items()
# #              } if isinstance(dd, dict) else { prefix : dd }



# #https://stackoverflow.com/questions/6037503/python-unflatten-dict
# def build_json_from_blueprint(field_dict):
#     field_dict = dict(field_dict)
#     new_field_dict = dict()
#     field_keys = list(field_dict)
#     field_keys.sort()

#     for each_key in field_keys:
#         field_value = field_dict[each_key]
#         processed_key = str(each_key)
#         current_key = None
#         current_subkey = None
#         for i in range(len(processed_key)):
#             if processed_key[i] == "[":
#                 current_key = processed_key[:i]
#                 start_subscript_index = i + 1
#                 end_subscript_index = processed_key.index("]")
#                 current_subkey = int(processed_key[start_subscript_index : end_subscript_index])

#                 # reserve the remainder descendant keys to be processed later in a recursive call
#                 if len(processed_key[end_subscript_index:]) > 1:
#                     current_subkey = "{}||{}".format(current_subkey, processed_key[end_subscript_index + 3:]) #update 2 -> 3
#                 break
#             # next child key is a dictionary
#             elif processed_key[i:i+2] == "||": #updated i -> i:i+2 and "." -> "||"
#                 split_work = processed_key.split("||", 1) #updated "." -> "||"
#                 if len(split_work) > 1:
#                     current_key, current_subkey = split_work
#                 else:
#                     current_key = split_work[0]
#                 break

#         if current_subkey is not None:
#             if current_key.isdigit():
#                 current_key = int(current_key)
#             if current_key not in new_field_dict:
#                 new_field_dict[current_key] = dict()
#             new_field_dict[current_key][current_subkey] = field_value
#         else:
#             new_field_dict[each_key] = field_value

#     # Recursively unflatten each dictionary on each depth before returning back to the caller.
#     all_digits = True
#     highest_digit = -1
#     for each_key, each_item in new_field_dict.items():
#         if isinstance(each_item, dict):
#             new_field_dict[each_key] = build_json_from_blueprint(each_item)

#         # validate the keys can safely converted to a sequential list.
#         all_digits &= str(each_key).isdigit()
#         if all_digits:
#             next_digit = int(each_key)
#             if next_digit > highest_digit:
#                 highest_digit = next_digit

#     # If all digits and can be sequential order, convert to list.
#     if all_digits and highest_digit == (len(new_field_dict) - 1):
#         digit_keys = list(new_field_dict)
#         digit_keys.sort()
#         new_list = []

#         for k in digit_keys:
#             i = int(k)
#             if len(new_list) <= i:
#                 # Pre-populate missing list elements if the array index keys are out of order
#                 # and the current element is ahead of the current length boundary.
#                 while len(new_list) <= i:
#                     new_list.append(None)
#             new_list[i] = new_field_dict[k]
#         new_field_dict = new_list
#     return new_field_dict


# # def get_payload_definition(JsonFilePath:str=None) -> list:

# #     if JsonFilePath is None:
# #         JsonFilePath = 'SampleJSON.json'

# #     with open(os.path.join(os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0], JsonFilePath)) as file:
# #         samplePayloadDict = json.load(file)


# #     jsonAttributePathDict = flatten_json.flatten(samplePayloadDict, separator='||')

# #     return jsonAttributePathDict


# def get_payload_definition(JsonFilePath:str=None) -> list:

#     if JsonFilePath is None:
#         JsonFilePath = 'SampleJSON.toml'

#     samplePayloadDict = json.loads(Helpers.TomlHelper.read_toml_file(os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), JsonFilePath))['SampleJSON'])
    

#     jsonAttributePathDict = dict()
#     for keys, item in recursive_iter(samplePayloadDict):
#         keyString = ''
#         for key_value in list(keys):
#             if isinstance(key_value, int):
#                 keyString = keyString.rstrip('||') + '[' + str(key_value) + ']||'
#             else:
#                 keyString += key_value + '||'
#         keyString = keyString.rstrip('||')
#         jsonAttributePathDict[keyString] = item

#     return jsonAttributePathDict


# def get_defined_datatype_value(dataType:str, maxValueFlag=False, fake=faker.Faker()):
#     # print('get_defined_datatype_value', maxValueFlag)
#     if dataType == 'string':
#         value = DataFactory.PayloadFactory.gen_string(maxValueFlag=maxValueFlag)
#     elif dataType == 'string_faker_text':
#         value = DataFactory.PayloadFactory.gen_string_faker_text(maxValueFlag=maxValueFlag, fake=fake)
#     elif dataType == "guid":
#         value = str(uuid.uuid4())
#     elif dataType == "float":
#         value = DataFactory.PayloadFactory.gen_float(maxValueFlag=maxValueFlag)
#     elif dataType == "integer":
#         value = DataFactory.PayloadFactory.gen_integer(maxValueFlag=maxValueFlag)
#     elif dataType == "date":
#         value = DataFactory.PayloadFactory.gen_date(min_year=2020)
#     elif dataType == 'datetime':
#         value = DataFactory.PayloadFactory.gen_datetime(min_year=2020)
#     else:
#         value = dataType
#     return value


# def gen_payload(jsonAttributePathDict, seed=0, maxValueFlag=False, fake=faker.Faker()) -> dict:
#     # print(jsonAttributePathDict)
#     # print('gen_payload', maxValueFlag)
#     for key, item in jsonAttributePathDict.items():
#         if len(item) > 0:
#             firstCharacter = item[0]
#             if firstCharacter == '{':
#                 dataType = item[1:-1]
#                 value = get_defined_datatype_value(dataType, maxValueFlag=maxValueFlag, fake=fake)
#             elif firstCharacter == '[':
#                 dataType = None
#                 value = random.choice([x.strip() for x in item[1:-1].split(',')])
#             else:
#                 dataType = None
#                 value = item
#         else:
#             dataType = None
#             value = item
        
#         jsonAttributePathDict[key] = value

#     jsonPayload = build_json_from_blueprint(jsonAttributePathDict)
#     return jsonPayload



def get_batch_specs(TargetThroughput:int, JsonFilePath:str=None) -> dict:
    print(f'Parameter - Target Throughput - {TargetThroughput}')
    payloadDefinitionDict = DataFactory.PayloadFactory.get_payload_definition(JsonFilePath)

    eventString = json.dumps(DataFactory.PayloadFactory.gen_payload(jsonAttributePathDict=copy.deepcopy(payloadDefinitionDict), maxValueFlag=True))
    print(eventString)

    # print(f'EventString with max values - {eventString}')
    MaxMessageSizeBytes = sys.getsizeof(eventString)
    print(f'Message size (bytes) - {MaxMessageSizeBytes}')

    MaxThroughputPerNode = round(((1*1000*1000) / MaxMessageSizeBytes) * 0.9) #multiplying by 0.8 just to give a little headroom

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
            NodeMessageSpecList.append({'NodeNum': str(int(node)+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(int(NodeThroughput)+1)})
        else:
            NodeMessageSpecList.append({'NodeNum': str(int(node)+1), 'NodeSec': str(math.floor(node/(NumberOfNodes/4))), 'NodeThroughput': str(NodeThroughput)})


    nodeBuckets = dict()
    for nodespec in NodeMessageSpecList:
        if nodespec['NodeSec'] in nodeBuckets.keys():
            nodeBuckets[nodespec['NodeSec']] = int(nodeBuckets[nodespec['NodeSec']]) + int(nodespec['NodeThroughput'])
        else:
            nodeBuckets[nodespec['NodeSec']] = nodespec['NodeThroughput']
    print(f'Node Buckets (Node, Throughput) - {nodeBuckets}')

    batchSpecDict = {
                    'PayloadDefinitionDict': payloadDefinitionDict
                    ,'NumberOfNodes': NumberOfNodes
                    ,'MinNodeThroughput': MinNodeThroughput
                    ,'MaxNodeThroughput': MaxNodeThroughput
                    ,'NodeMessageSpecList': NodeMessageSpecList
                    }
                    
    return batchSpecDict

if __name__ == '__main__':

    TargetThroughput = 1#100001 #thoughput messages/sec

    batchSpecDict = get_batch_specs(TargetThroughput=TargetThroughput)

    print(json.dumps(batchSpecDict))

    # #Speed Test
    # import time
    # start = time.time()
    # batchList = list()
    # for _ in range(batchSpecDict['NodeThroughput']):
    #     batchList.append(gen_payload(jsonAttributePathList=[_ for _ in batchSpecDict['payloadDefinitionList']], maxValueFlag=False))
    # print(f'Duration to generate payload (sec) - {str(round(time.time() - start, 2))}')
    # # print(len(batchList))
    # # print(batchList)
