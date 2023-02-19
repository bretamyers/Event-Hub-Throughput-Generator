
myDict = {"name": "Bret", "t": 1, "b": 2}

import json

myString = '{'
for key, value in myDict.items():
    print(key, value)
    myString += f'\\"{key}\\"\\:\\"{value}\\"'
myString += '}'
print(myString)

t = '\\{' + '\\,'.join([f'\\"{key}\\"\\:\\"{value}\\"' for key, value in myDict.items()]) + '\\}'
print(t)
