import csv

with open('src/chapter01/data.csv', mode='r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
