import json
import string
import random
import uuid
import datetime
import os
import faker
import Helpers.TomlHelper


def gen_string(low=5, high=100, maxValueFlag=False) -> string:
    value = ''
    if maxValueFlag:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=100))
    else:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(low, high)))

    return value

def gen_string_faker_text(low=5, high=100, seed=0, maxValueFlag=False) -> string:
    fake = faker.Faker()
    faker.Faker.seed(seed)
    value = ''
    if maxValueFlag:
        value = fake.text(max_nb_chars=high)
    else:
        value = fake.text(max_nb_chars=random.randint(low, high))

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

    samplePayloadDict = json.loads(Helpers.TomlHelper.read_toml_file(os.path.join(os.path.abspath(os.path.join(__file__, "../../..")), JsonFilePath))['SampleJSON'])
    

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


def get_defined_datatype_value(dataType:str, maxValueFlag=False):
    if dataType == 'string':
        value = gen_string(maxValueFlag)
    elif dataType == "guid":
        value = str(uuid.uuid4())
    elif dataType == "float":
        value = gen_float(maxValueFlag)
    elif dataType == "integer":
        value = gen_integer(maxValueFlag)
    elif dataType == "date":
        value = gen_date(min_year=2020)
    elif dataType == 'datetime':
        value = gen_datetime(min_year=2020)
    else:
        value = dataType
    return value


def gen_payload(jsonAttributePathDict, seed=0, maxValueFlag=False) -> dict:
    for key, item in jsonAttributePathDict.items():
        firstCharacter = item[0]
        if firstCharacter == '{':
            dataType = item[1:-1]
            value = get_defined_datatype_value(dataType, maxValueFlag=maxValueFlag)
        elif firstCharacter == '[':
            dataType = None
            value = random.choice([x.strip() for x in item[1:-1].split(',')])
        else:
            dataType = None
            value = item

        jsonAttributePathDict[key] = value

    jsonPayload = build_json_from_blueprint(jsonAttributePathDict)
    return jsonPayload

if __name__ == '__main__':

    pass