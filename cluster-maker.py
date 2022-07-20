import time
import gc

from pymongo import MongoClient


class Cluster:

    def __init__(self, ain, aout):
        self.add_in = set(ain)
        self.add_out = set(aout)
        self.tx_count = 1

    def intersect(self, oc):
        return self.add_in.intersection(oc.add_in)

    def addCluster(self, oc):
        self.add_in = self.add_in.union(oc.add_in)
        self.add_out = self.add_out.union(oc.add_out)
        self.tx_count += oc.tx_count  # somar outputs do cluster


def test(a, allclusters):
    sc = set()
    for c in allclusters:
        for address in c.add_in:
            sc.add(address)

    sa = set()
    for c in a:
        for address in c.add_in:
            sa.add(address)

    # se forem iguais, pegou todos os endereços
    if not len(sc) == len(sa):
        print('Erro')

    # verifica se nao clusterizou alguem
    for i in range(len(allclusters)):
        for j in range(i + 1, len(allclusters)):
            if allclusters[i].intersect(allclusters[j]):
                print('Erro - item nao clusterizado')


# retorna os indices dos clusters com interseccao nao nula
def find_clusters(a, c):
    r = []
    for i in range(len(c)):
        if c[i].intersect(a):
            r.append(i)
    return r


# entrada um lista de lista de objetos Clusters
# saida uma lista de listas de objetos Clusters clusterizados
def make_clusters(l):
    clusters = []

    for a in l:
        r = find_clusters(a, clusters)
        # se a intersecao é vazia, um cluster novo
        if len(r) == 0:
            clusters.append(a)
        # se a intersecao e apenas com um cluster, junte os dois
        elif len(r) == 1:
            clusters[r[0]].addCluster(a)
        # caso contrario, junte todo mundo! ( e remova os juntados...)
        elif len(r) > 1:
            clusters[r[0]].addCluster(a)
            for i in range(len(r) - 1, 0, -1):
                clusters[r[0]].addCluster(clusters[r[i]])
                del clusters[r[i]]
    return clusters


def write_clusters(clusters, month, year):
    with open(f'/path/to/save/{year}/tx_{year}', 'w') as file:
        for cluster in clusters:
            if len(cluster.add_out) > 0:
                file.write(f'{cluster.add_in}:{cluster.add_out}\n')
            else:
                file.write(f'{cluster.add_in}:\n')


# function
def filterSmallerClaster(c):
    if c.tx_count > 1 and len(c.add_out) > 0:
        return True
    else:
        return False


def main():
    try:
        cluster = MongoClient()
        # for i in range(2017, 2020, 1):
        year = '2019'
        db = cluster['transaction']
        collection = db['tx_' + year]
        start = time.perf_counter()
        month = 1
        tocluster = []
        for x in collection.find({'$and': [{'protocol': 'Not knowable'},
                                           {'addresses_senders': {'$ne': "Newly_generated_coins"}},
                                           {'year': int(year)}]}):
            if isinstance(x['addresses_senders'], list):
                try:
                    addresses_senders = x['addresses_senders']
                    addresses_receivers = x['addresses_receivers']
                    if x['addresses_receivers'][-1] in x['addresses_senders']:
                        addresses_receivers.pop()
                    if '???' in x['addresses_receivers']:
                        index = addresses_receivers.index('???')
                        addresses_receivers.pop(index)
                    tocluster.append(Cluster(addresses_senders, addresses_receivers))
                except:
                    tocluster.append(Cluster(addresses_senders, addresses_receivers))
        cl = list(filter(filterSmallerClaster, make_clusters(tocluster)))
        if len(cl) > 0:
            write_clusters(cl, month, year)

        print(f'Clusters {year}: {month} done!')
        gc.collect()
        month += 1
        finish = time.perf_counter()

        print(f'Total time: {round(finish - start, 2)}')
        print(f'Clusters {year}, done!')
    except Exception as error:
        print(error)


main()
