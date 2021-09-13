from array import array
import csv
from faker import Faker
import random

fake = Faker(['it_IT'])
status = ["Disputed", "Undisputed"]  # Status list
store = ["Amazon", "Apple", "Microsoft", "Mediaworld", "Coop", "Euronics", "Expert", "Unieuro", "Ikea", "Decathlon"]
city = ["Milano", "Torino", "Roma", "Napoli", "Reggio Calabria", "Messina", "Catania", "Genova", "Palermo", "Catanzaro"]
firstname_arr = []
lastname_arr = []
for i in range(100):
    firstname_arr.append(fake.first_name())
    lastname_arr.append(fake.last_name())

with open('dataset.csv', mode='w', newline='') as csv_file:
    fieldnames = ['ID', 'CUSTOMER_NAME','STORE_NAME', 'AMOUNT', 'TRANSACTION_DATE',
                  'STATUS']

    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    co = 1
    for x in range(100000):
        firstname = random.choice(firstname_arr)
        lastname = random.choice(lastname_arr)
        store_name = random.choice(store) + ", " + random.choice(city)
        amount = random.randint(1000, 2000)
        transaction_date = fake.date_between(
            start_date='-10y', end_date='today')
        status_item = random.choice(status)
        writer.writerow({
            'ID': co,
            'CUSTOMER_NAME': firstname + " " + lastname,
            'STORE_NAME': store_name,
            'AMOUNT': amount,
            'TRANSACTION_DATE': transaction_date,
            'STATUS': status_item,
        })

        co += 1
