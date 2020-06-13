import csv
import re
import pprint
import datetime as dt
import psycopg2 as pg

# from sqlalchemy import create_engine, Column, Integer, String, Date
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base

from pymongo import MongoClient

# Base = declarative_base()
# engine = create_engine('postgresql://Tickets_db@localhost')
# Session = sessionmaker(bind=engine)
# session = Session()
#
#
# class Tickets_db(Base):
#     __tablename__ = 'Tickets'
#
#     Author = Column(String, nullable=False)
#     Price = Column(Integer, nullable=False)
#     Place = Column(String, nullable=False)
#     Date = Column(Date, nullable=False)
#
# def create_all():
#     Base.metadata.create_all(engine)
#
# def drop_all():
#     Base.metadata.drop_all(engine)

client = MongoClient()
netology_db = client['netology2']


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    all_tickets = []
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = re.split('\.', row['Дата'])
            day = int(date[0])
            month = int(date[1])
            print(day, month)
            print(row)
            ticket = {'Author': row['Исполнитель'],
                      'Price': int(row['Цена']),
                      'Place': row['Место'],
                      'Date': dt.datetime(year=2020, month=month, day=day)}
            all_tickets.append(ticket)
        result = db.insert_many(all_tickets)
        print(result.inserted_ids)
        # print(db.list_collections())


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """

    print(list(db.find().sort('Price')))


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """

    regex = re.compile(f'^.*{name}.*$', re.IGNORECASE)
    # print(regex)
    myquery = {'Author': {'$regex': regex}}
    found_tickets = []
    # print(found_tickets)
    # print(list(db.find()))
    for ticket in db.find(myquery).sort('Price'):
        # print(ticket)
        found_tickets.append(ticket)
    # print(found_tickets)
    return found_tickets

def data_sort(db):
    print(list(db.find().sort('Date')))


if __name__ == '__main__':
    # create_all()


    event = netology_db['event']
    # read_data('artists.csv', event)
    # netology_db.event.delete_many({})
    # print(list(event.find()))
    print(find_by_name('i', event))
    print('------------------------')
    find_cheapest(event)
    print('------------------------')
    data_sort(event)
    # print(netology_db.list_collection_names())
