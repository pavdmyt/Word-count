import sys
import multiprocessing as mp
from mapreduce import Map, partition, Reduce


def get_data(file_name):
    """Transforms text from file into a list."""
    data_lst = []
    with open(file_name, 'r') as f:
        for line in f:
            data_lst.extend(line.split())

    return data_lst


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('Specify input file for reading!')
        sys.exit(1)

    # Load data from file
    data = get_data(sys.argv[1])

    # Build a pool of 12 processes
    pool = mp.Pool(processes=12)

    # Run Map tasks
    map_res = pool.map(Map, data)

    # Run Partitioner
    consolidated_data = partition(map_res)

    # Run Reduce tasks
    reduced = pool.map(Reduce, consolidated_data.items())

    # Print top 20 most frequent words
    for pair in sorted(reduced, key=lambda tup: tup[1], reverse=True)[:20]:
        print(pair[0], ': ', pair[1])
