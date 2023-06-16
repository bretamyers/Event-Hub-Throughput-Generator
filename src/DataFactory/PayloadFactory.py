import json
import string
import random
import uuid
import datetime
import os
import faker
import src.Helpers.TomlHelper as Helpers_TomlHelper
import src.DataFactory.DataTypeFactory as DataFactory_DataTypeFactory



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


# #https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
# def flatten_dict_string(dd, separator='||', prefix=''):
#     return { prefix + separator + k if prefix else k : v
#              for kk, vv in dd.items()
#              for k, v in flatten_dict_string(vv, separator, kk).items()
#              } if isinstance(dd, dict) else { prefix : dd }



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


# def get_payload_definition(JsonFilePath:str=None) -> list:

#     if JsonFilePath is None:
#         JsonFilePath = 'SampleJSON.json'

#     with open(os.path.join(os.path.split(os.path.join(os.path.dirname(os.path.abspath(__file__))))[0], JsonFilePath)) as file:
#         samplePayloadDict = json.load(file)


#     jsonAttributePathDict = flatten_json.flatten(samplePayloadDict, separator='||')

#     return jsonAttributePathDict


def get_payload_definition(JsonFilePath:str=None) -> list:

    if JsonFilePath is None:
        JsonFilePath = 'SampleJSON.toml'

    samplePayloadDict = json.loads(Helpers_TomlHelper.read_toml_file(os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), JsonFilePath))['SampleJSON'])
    

    jsonAttributePathDict = dict()
    for keys, item in recursive_iter(samplePayloadDict):
        keyString = ''
        for key_value in list(keys):
            if isinstance(key_value, int):
                keyString = keyString.rstrip('||') + '[' + str(key_value) + ']||'
            else:
                keyString += key_value + '||'
        keyString = keyString.rstrip('||')
        jsonAttributePathDict[keyString] = item

    return jsonAttributePathDict


def get_defined_datatype_value(keyTuple:tuple, elementName:str, datasetDict:dict, dataType:str, properties:list, maxValueFlag=False, fake=faker.Faker()):
    # print('get_defined_datatype_value', maxValueFlag)
    if dataType == 'string':
        value = DataFactory_DataTypeFactory.gen_string(maxValueFlag=maxValueFlag)
    elif dataType == 'string_faker_text':
        value = DataFactory_DataTypeFactory.gen_string_faker_text(maxValueFlag=maxValueFlag, fake=fake)
    elif dataType == "guid":
        value = str(uuid.uuid4())
    elif dataType == "float":
        value = DataFactory_DataTypeFactory.gen_float(maxValueFlag=maxValueFlag)
    elif dataType == "integer":
        value = DataFactory_DataTypeFactory.gen_integer(maxValueFlag=maxValueFlag)
    elif dataType == "date":
        value = DataFactory_DataTypeFactory.gen_date(keyTuple=keyTuple, elementName=elementName, datasetDict=datasetDict, properties=properties)
    elif dataType == 'datetime':
        value = DataFactory_DataTypeFactory.gen_datetime(keyTuple=keyTuple, elementName=elementName, datasetDict=datasetDict, properties=properties)
    elif dataType == 'epoch':
        value = DataFactory_DataTypeFactory.gen_epoch(keyTuple=keyTuple, elementName=elementName, datasetDict=datasetDict, properties=properties)
    elif dataType == 'base64':
        value = DataFactory_DataTypeFactory.gen_base64(myString='')
    elif dataType == 'product_code':
        value = DataFactory_DataTypeFactory.gen_product_code(codeLength=9)
    else:
        value = dataType
    return value


def gen_payload(jsonAttributePathDict:dict, keyTuple:tuple, datasetDict:dict, seed=0, maxValueFlag:bool=False, fake=faker.Faker()) -> dict:
    # print(jsonAttributePathDict)
    # print('gen_payload', maxValueFlag)
    for key, item in jsonAttributePathDict.items():
        if len(item) > 0:
            firstCharacter = item[0]
            if firstCharacter == '{':
                dataType = item[item.index('{')+1:item.index('}')]
                properties = item[item.index('}')+1:].strip().split(' ')
                value = get_defined_datatype_value(keyTuple, key, datasetDict, dataType, properties, maxValueFlag=maxValueFlag, fake=fake)
            elif firstCharacter == '[':
                dataType = None
                value = random.choice([x.strip() for x in item[1:-1].split(',')])
            else:
                dataType = None
                value = item
        else:
            dataType = None
            value = item
        
        jsonAttributePathDict[key] = value

    jsonPayload = build_json_from_blueprint(jsonAttributePathDict)
    return jsonPayload



if __name__ == '__main__':

    pass