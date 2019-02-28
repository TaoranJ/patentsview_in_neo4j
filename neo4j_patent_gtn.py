# -*- coding: utf-8 -*-

"""Collect patents which received more than 10 citations. Outputs are saved
using names output/ge10/pids.year.ge10.pkl.

Patent-[:CITE]->Patent
Inventor-[:INVENT]->Patent
Asignee-[:OWN]->Patent
Asignee-[:LOCATE_AT]->Location
Inventor-[:LOCATE_AT]->Location

Examples
--------

.. code-block:: bash

   python neo4j_patent_gtn.py --min-num-cites 10 credential.txt

"""

import argparse
import pickle
import os

from techflow.neo4j_handler import Neo4jHandler


if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file')
    pparser.add_argument('--min-num-cites', type=int, default=5,
                         help='Minimum number of cites received.')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, None)
    years = [str(year) for year in range(1976, 2018)]
    min_num_cites = args.min_num_cites
    for year in years:
        tw = (year + '-01-01', year + '-12-31')
        result = handler.query_patents_by_time_window(tw,
                                                      min_num=min_num_cites)
        opath = os.path.join('output', 'ge' + str(min_num_cites),
                             'pids.' + str(year) + '.ge' + str(min_num_cites)
                             + '.pkl')
        pickle.dump(result, open(opath, 'wb'))
