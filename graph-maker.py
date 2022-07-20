import time
import sys
import gc


import networkx as nx
import matplotlib.pyplot as plt
from networkx.utils import is_string_like
from networkx.drawing.layout import shell_layout, \
    circular_layout, kamada_kawai_layout, spectral_layout, \
    spring_layout, random_layout, planar_layout

CLUSTERS = []
CLUSTERS_ADDRESSES_OUTPUT = []


#################################################################################
###         Make graph

def make_edges():
    edges = []
    count = 0
    for addresses_output in CLUSTERS_ADDRESSES_OUTPUT:
        for address_output in addresses_output:
            if address_output != '':
                edges.append([count, address_output])
        count += 1
    return edges


def create_graph(edges, month, year):
    nodes_output = []
    G = nx.Graph()
    G.add_nodes_from(CLUSTERS)
    for cluster_addresses_output in CLUSTERS_ADDRESSES_OUTPUT:
        for address_output in cluster_addresses_output:
            nodes_output.append(address_output)
    G.add_edges_from(edges)
    G.add_nodes_from(nodes_output)
    plt.figure(figsize=(80, 60))
    pos = nx.spring_layout(G)
    ec = nx.draw_networkx_edges(G, pos, edge_color='Black')
    nc = nx.draw_networkx_nodes(G, pos, nodelist=CLUSTERS, node_color='r', node_size=100)
    ac = nx.draw_networkx_nodes(G, pos, nodelist=nodes_output, node_color='b', node_size=100)
    #plt.savefig(f'/home/nellter/PyCharm/PycharmProjects/IC_projeto/{year}/graph_{str(month)}')
    plt.savefig(f'/home/azureuser/graph/2019/graph_{str(year)}')

##########################################################################
###                 Data extraction

def insert_cluster(cluster):
    CLUSTERS.append(cluster)


def insert_add_output(addresses_output):
    CLUSTERS_ADDRESSES_OUTPUT.append(addresses_output)


def read_clusters(month, year):
    count = 0
    try:
        #with open(f'/home/nellter/PyCharm/PycharmProjects/IC_projeto/{year}/tx_{year}', 'r') as file:
        with open(f'/home/azureuser/graph/2019/tx_{year}', 'r') as file:
            clusters = file.read()
            clusters = clusters.split('\n')
            for cluster in clusters:
                if len(cluster) > 0:
                    cluster_split, addresses_output = split_line(cluster)
                    insert_cluster(count)#cluster_split)
                    if not addresses_output[0] == '':
                        insert_add_output(addresses_output)
                    count += 1
    except Exception as error:
        print(error)


def split_line(cluster):
    a = cluster.replace('{', '')
    b = a.replace('}', '')
    c = b.replace("'", '')
    cluster_split = c.split(':', 2)
    addresses_output_split = cluster_split[1].split(',')
    return cluster_split[0], addresses_output_split


def main():
    year = "2019"
    month = 1
    start = time.perf_counter()
    while month <= 1:
        read_clusters(month, year)
        edges = make_edges()
        create_graph(edges, month, year)
        CLUSTERS.clear()
        CLUSTERS_ADDRESSES_OUTPUT.clear()
        print(f'Clusters {year}: {month} done!')
        gc.collect()
        month += 1
    finish = time.perf_counter()
    print(f'Total time: {round(finish - start, 2)}')
    print(f'Clusters {year}, done!')


main()
