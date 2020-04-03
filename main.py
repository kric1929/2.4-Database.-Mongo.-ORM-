import re
import csv
import json
from pymongo import ASCENDING
from pymongo import MongoClient


client = MongoClient()


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    mongo_db = client[db]
    tickets_collection = mongo_db['tickets']

    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        ticket_list = list(reader)
        ticket_dict = json.loads(json.dumps(ticket_list))
        for info_ticket in ticket_dict:
            info_ticket['Цена'] = int(info_ticket['Цена'])
        tickets_collection.insert_many(ticket_dict)

    return tickets_collection


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    mongo_db = client[db]
    tickets_collection = mongo_db['tickets']
    sorting_prices_collection = tickets_collection.find().sort('Цена', ASCENDING)

    return sorting_prices_collection


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    mongo_db = client[db]
    tickets_collection = mongo_db['tickets']
    regex = re.compile(r'.*' + name + r'.*', re.IGNORECASE)
    tickets_sorted = tickets_collection.find({'Исполнитель': regex}).sort('Цена', ASCENDING)

    return tickets_sorted


if __name__ == '__main__':
    print('Исходная коллекция:')
    tickets_collection = read_data('artists.csv', 'concerts')
    print(list(tickets_collection.find()))
    print('\nБилеты по возрастанию цены:')
    tickets_by_price = find_cheapest('concerts')
    print(list(tickets_by_price))
    print('\nБилеты по имени исполнителя:')
    sorted_find_by_name = find_by_name('to', 'concerts')
    for artist in sorted_find_by_name:
        print(artist)
