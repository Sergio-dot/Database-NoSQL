from array import array
import csv
from faker import Faker
import random

fake = Faker(['it_IT'])
status = ["Disputed", "Undisputed"]  # Status list
store = []
firstname = []
lastname = []

for i in range(5000):
    store.append(fake.company())

for x in range (300):
    firstname.append(fake.first_name())
    lastname.append(fake.last_name())

with open('dataset_75.csv', mode='w', newline='') as csv_file:
    fieldnames = ['ID', 'CUSTOMER_NAME','STORE_NAME', 'AMOUNT', 'TRANSACTION_DATE',
                  'STATUS']

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    co = 1
    for x in range(100000):
        fname = random.choice(firstname)
        lname = random.choice(lastname)
        store_name = random.choice(store)
        amount = random.randint(1000, 2000)
        transaction_date = fake.date_between(
            start_date='-10y', end_date='today')
        status_item = random.choice(status)
        writer.writerow({
            'ID': co,
            'CUSTOMER_NAME': fname + " " + lname,
            'STORE_NAME': store_name,
            'AMOUNT': amount,
            'TRANSACTION_DATE': transaction_date,
            'STATUS': status_item,
        })

        co += 1

