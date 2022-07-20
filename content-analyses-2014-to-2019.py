import re

from pymongo import MongoClient

ascii_content = {}

def make_cluster():
    try:
        cluster = MongoClient()
        return cluster
    except Exception as error:
        print(error)


def retrieve_collection(cluster, year_input):
    try:
        year = 'tx_' + year_input
        db = cluster['transaction']
        collection = db[year]
        return collection
    except Exception as error:
        print(error)


def clean_dict():
    ascii_content.clear()


def insert_dict(key):
    ascii_content[key] = {
        'count': 1,
    }


years = [2014,2015,2016,2017,2018,2019]

cluster = make_cluster()


for year_to_see in years:
    readable = 0
    not_readable = 0
    collection = retrieve_collection(cluster, str(year_to_see))
    for document in collection.find():

        tx_ascii = str(bytes.fromhex(document['script'][4:]))
        tx_ascii = tx_ascii[2:len(tx_ascii) - 1]

        if "\\" not in tx_ascii[:3]:
            readable += 1
        else:
            not_readable += 1

        if "\\" not in tx_ascii[:3]:
            tx_ascii = [tx_ascii]
            for ascii_script in tx_ascii:
                matches = re.findall('\\b\\w{3,}', ascii_script, re.DOTALL)
                if len(matches) > 0:
                    if ascii_content.get(matches[0]):
                        ascii_content[matches[0]]['count'] += 1
                    else:
                        insert_dict(matches[0])

    with open(f'ascii_{year_to_see}-analyses.csv', 'w') as file:
        for content in ascii_content:
            if ascii_content[content]['count'] > 1:
                file.write(f'{content} {ascii_content[content]["count"]}\n')

    print(f"{year_to_see}: readable({readable}) and not readable({not_readable})")
    clean_dict()
