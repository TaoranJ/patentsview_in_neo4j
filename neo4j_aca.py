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
import json

from techflow.neo4j_handler import Neo4jHandler

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file.')
    pparser.add_argument('ipath', help='Path to output.')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, None)
    ipath = args.ipath
    pids = json.load(open(ipath, 'r'))
    aca = handler.query_assignee_relationship(pids)
    json.dump(aca, open('aca.json', 'w'))
