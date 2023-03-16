
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



if __name__ == '__main__':
    pass
