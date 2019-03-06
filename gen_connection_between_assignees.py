# -*- coding: utf-8 -*-

"""Generate connections between assignees.

Examples
--------

.. code-block:: bash

   python gen_connection_between_assignees.py credential.txt [pid.json]

"""

import argparse
import json
import itertools
from collections import Counter
import math

from handler.neo4j_handler import Neo4jHandler


def visualization_format(aca, opath, max_size=35, min_size=5):
    """Scale size of assignee node to [min_size, max_size]."""
    nodes = set(itertools.chain(*aca))
    edges = list(set([tuple(sorted(e)) for e in aca]))
    res = {'nodes': {}, 'edges': edges}
    citation_received = Counter([e[0] for e in aca])
    unit = math.log(citation_received.most_common(1)[0][1] + 1, 2) /\
        (max_size - min_size)
    for node in nodes:
        res['nodes'][node] = {
                'id': node,
                'marker': {
                    'radius': int(math.log(citation_received[node] + 1, 2) /
                                  unit + min_size)
                }}
    json.dump(res, open(opath + '.aca.json', 'w'))


if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file.')
    pparser.add_argument('ipath', help='Path to output.')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, None)
    pids = json.load(open(args.ipath, 'r'))
    visualization_format(handler.query_connection_between_assignees(pids),
                         args.ipath)
