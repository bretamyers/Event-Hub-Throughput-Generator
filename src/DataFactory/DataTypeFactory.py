
import random
import datetime
import string
import faker

def gen_string(low=5, high=10, maxValueFlag=False) -> string:
    # print('gen_string', maxValueFlag)
    value = ''
    if maxValueFlag:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=high))
    else:
        value = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(low, high)))

    return value


def gen_string_faker_text(low=10, high=20, seed=0, maxValueFlag=False, fake=faker.Faker()) -> string:
    value = ''
    if maxValueFlag:
        value = f''.join(random.choices(string.ascii_letters, k=high))
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
def gen_date(keyTuple, elementName, datasetDict, properties=['2020-01-01', 'increasing']) -> string:
    if keyTuple in datasetDict.keys():
        day = random.randint(1,2)
        myDate = datasetDict[keyTuple] + datetime.timedelta(days=day)
    else:
        myDate = datetime.datetime.strptime(f'{properties[0]}', '%Y-%m-%d' )

    datasetDict[keyTuple] = myDate
    
    return datetime.datetime.strftime(myDate, '%Y-%m-%d')


#https://gist.github.com/rg3915/db907d7455a4949dbe69
def gen_datetime(keyTuple, elementName, datasetDict, properties=['2020-01-01', 'increasing']) -> string:
    
    # print(keyTuple, elementName, datasetDict, properties)
    # datasetDict = {"keyTuple": {"elementName1||name||category": value, "elementName2||name": value, etc}}

    # Add the key if not found
    if keyTuple not in datasetDict.keys():
        datasetDict[keyTuple] = dict()

    # If json element is found, take existing value and add min/sec to it.
    # Else, default to the start datetime
    if elementName in datasetDict[keyTuple].keys():
        # min = random.randint(0,1)
        # sec = random.randint(1,5) if min == 0 else random.randint(0,1)
        # myDateTime = datasetDict[keyTuple][elementName] + datetime.timedelta(minutes=min, seconds=sec)
        sec = random.randint(1, 10)
        myDateTime = datasetDict[keyTuple][elementName] + datetime.timedelta(seconds=sec)
    else:
        myDateTime = datetime.datetime.strptime(f'{properties[0]} 00:00:00', '%Y-%m-%d %H:%M:%S' )

    # if elementName in datasetDict[keyTuple].keys():
    #     datasetDict[keyTuple][elementName] = myDateTime
    # else:
    #     datasetDict[keyTuple][elementName] = myDateTime
    
    datasetDict[keyTuple][elementName] = myDateTime

    return datetime.datetime.strftime(myDateTime, '%Y-%m-%d %H:%M:%S')



if __name__ == '__main__':
    pass
