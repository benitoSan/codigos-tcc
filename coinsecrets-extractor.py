import json
import urllib.request
import time
import concurrent.futures
import sys

TRANSACTIONS_KNOWABLE = {}
TRANSACTIONS_UNKNOWABLE = {}
BLOCKS_PENDING = []


def request(url):
    try:
        request = urllib.request.urlopen(url)
        if request.getcode() == 200:
            return request
        else:
            return None
    except Exception as e:
        print('Error in requests: {} in url: {}'.format(e, url))
        return None


def parsing(req):
    json_str = req.read()
    encoding = req.info().get_content_charset('utf-8')
    dictionary = json.loads(json_str.decode(encoding))
    return dictionary


def filter_extracted_transaction(dictionary):
    for op_return in range(len(dictionary['op_returns'])):
        transaction_id = dictionary['op_returns'][op_return]['txid']
        script = dictionary['op_returns'][op_return]['script']
        hex_t = dictionary['op_returns'][op_return]['hex']
        ascii_t = dictionary['op_returns'][op_return]['ascii']
        if len(dictionary['op_returns'][op_return]['protocols']) == 0:
            insert_transaction_unknowable(transaction_id, script, hex_t, ascii_t)
        else:
            protocols = dictionary['op_returns'][op_return]['protocols'][0]['name']
            insert_transaction_knowable(transaction_id, script, hex_t, ascii_t, protocols)


def insert_transaction_knowable(transaction_id, script, hex_t, ascii_t, protocols):
    TRANSACTIONS_KNOWABLE[transaction_id] = {
        'script': script,
        'hex': hex_t,
        'ascii': ascii_t,
        'protocols': protocols,
    }


def insert_transaction_unknowable(transaction_id, script, hex_t, ascii_t):
    TRANSACTIONS_UNKNOWABLE[transaction_id] = {
        'script': script,
        'hex': hex_t,
        'ascii': ascii_t,
    }


def save(block_height, protocol_status, transactions, year):
    if len(transactions) != 0:
        try:
            with open('path/to/save' + year +
                      '_transactions/block_' + str(block_height) + '_' + protocol_status, 'w') as file:
                for transaction in transactions:
                    script = transactions[transaction]['script']
                    hex_t = transactions[transaction]['hex']
                    ascii_t = transactions[transaction]['ascii']
                    if transactions == 'knowable':
                        protocol_t = transactions[transaction]['protocols']
                        file.write('{},{},{},{},{}\n'.format(transaction, script, hex_t, protocol_t, ascii_t))
                    else:
                        file.write('{},{},{},{}\n'.format(transaction, script, hex_t, ascii_t))
            print('Data from block {} {} transactions has been saved with success!'.format(block_height, protocol_status))
        except Exception as error:
            print(f'Error in saving: {error}')


def process_program(block_height, year):
    url = 'http://api.coinsecrets.org/block/'+str(block_height)
    response = request(url)
    if response:
        dictionary = parsing(response)
        if len(dictionary['op_returns']) != 0:
            filter_extracted_transaction(dictionary)
            save(block_height, 'knowable', TRANSACTIONS_KNOWABLE, year)
            save(block_height, 'unknowable', TRANSACTIONS_UNKNOWABLE, year)
    else:
        print('*Block {} was not verified!'.format(block_height))
        return False, block_height

    block_height += 1
    TRANSACTIONS_KNOWABLE.clear()
    TRANSACTIONS_UNKNOWABLE.clear()
    return True, 0


def main():

    block_height = int(sys.argv[1])
    block_end = int(sys.argv[2])
    year = sys.argv[3]

    initial = int(block_height)
    end = int(block_end)
    begin = time.perf_counter()

    if initial != end:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            result = [executor.submit(process_program, block, year) for block in range(initial, end + 1)]
            for res in concurrent.futures.as_completed(result):
                if not res.result()[0]:
                    BLOCKS_PENDING.append(res.result()[1])
                    print(f'{res.result()[1]} has been put in pending...')
    else:
        process_program(initial, year)

    if len(BLOCKS_PENDING) != 0:
        for block in BLOCKS_PENDING:
            result, block = process_program(block, year)
            if not result:
                BLOCKS_PENDING.append(block)

    finish = time.perf_counter()

    print(f'Total time: {round(finish - begin, 2)}')


main()
