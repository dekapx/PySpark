# write code to read json file
import json

with open('src/chapter02/student-data.json', mode='r') as file:
    data = json.load(file)
    for entry in data:
        print(entry)