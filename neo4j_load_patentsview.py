# -*- coding: utf-8 -*-

"""Load PatentsView dataset into neo4j database."""

import argparse

from handler.neo4j_handler import Neo4jHandler

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file')
    pparser.add_argument('data', help='path to raw patent data')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, args.data)
    handler.load_patentsview()
