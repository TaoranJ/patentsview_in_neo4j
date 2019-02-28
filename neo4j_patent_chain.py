# -*- coding: utf-8 -*-

"""Get all patent citation chains. Output files are saved using names
output/ge10/chains.year.ge10.chunk.pkl.

Examples
--------

.. code-block:: bash

   find output/ge10/ -maxdepth 1 -type f -name "pids.*.pkl" -print0 | \
sort -z | xargs -0 -n 1 -I {} python neo4j_patent_chain.py credential.txt {}

"""

import argparse
import pickle
import os

from techflow.neo4j_handler import Neo4jHandler

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file.')
    pparser.add_argument('ipath', help='Path to output.')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, None)
    ipath = args.ipath
    pids = pickle.load(open(ipath, 'rb'))
    print('Working on {}'.format(os.path.basename(ipath).split('.')[1]))
    for ix, chains in handler.query_citation_chain(pids):
        opath = ipath.replace('pids', 'patent.chain.raw')
        opath = opath.replace('.pkl', '.{:02d}.pkl'.format(ix + 1))
        pickle.dump(chains, open(opath, 'wb'))
