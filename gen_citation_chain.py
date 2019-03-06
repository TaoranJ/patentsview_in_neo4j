#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Generate citation chains for patents, assignees and inventors.

Patent citation chain:
[target patent, patents who cite the target patent ordered by granted time]
Assignee citation chain:
[target assignee, patents who cite the target assignee ordered by granted time]
Inventor citation chain:
[target inventor, patents who cite the target inventor ordered by granted time]

Examples
--------

.. code-block:: bash

   # generate patent citation chain
   python gen_citation_chain.py --patent credential.txt
   # generate assignee citation chain
   python gen_citation_chain.py --assignee credential.txt
   # generate inventor citation chain
   python gen_citation_chain.py --inventor credential.txt

"""

import argparse
import pickle
import os

from handler.neo4j_handler import Neo4jHandler


def retrieve_target_patents(handler, args):
    """Collect all patents in the database and then filter by the number of
    citatoins received.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    ipath = os.path.join(args.opath, 'pids' +
                         '.min_cites' + str(args.min_num_cites) +
                         '.max_cites' + str(args.max_num_cites) + '.pkl')
    print('Trying to find file of target patents: {}'.format(ipath))
    if os.path.exists(ipath):
        print('Found file {}'.format(ipath))
        pids = pickle.load(open(ipath, 'rb'))
    else:
        print('{} not found. Generating ...'.format(ipath))
        years = [str(year) for year in range(1976, 2018)]
        pids = set([])
        for year in years:
            tw = (year + '-01-01', year + '-12-31')
            pids |= set(handler.query_patents_by_time_window(
                tw, min_num=args.min_num_cites, max_num=args.max_num_cites))
        pickle.dump(pids, open(ipath, 'wb'))
    return ipath, pids


def retrieve_patent_citation_chains(handler, args):
    """Generate patent citation chains.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    ipath, pids = retrieve_target_patents(handler, args)
    opath = os.path.join(args.opath, 'patent.chain.raw')
    if not os.path.exists(opath):
        os.makedirs(opath)
    for ix, chains in handler.query_patent_citation_chain(pids):
        ofp = os.path.join(opath, os.path.basename(ipath).replace(
            'pids', 'patent.chain.raw').replace('.pkl', '.{:03d}.pkl'.format(
                ix + 1)))
        pickle.dump(chains, open(ofp, 'wb'))


def retrieve_target_assignees(handler, args):
    """Retrieve target assignees associated to target patents.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    ipath = os.path.join(args.opath, 'assignees' +
                         '.min_cites' + str(args.min_num_cites) +
                         '.max_cites' + str(args.max_num_cites) + '.pkl')
    print('Trying to find file of target assignees: {}'.format(ipath))
    if os.path.exists(ipath):
        print('Found file {}'.format(ipath))
        assignees = pickle.load(open(ipath, 'rb'))
    else:
        print('{} not found. Generating ...'.format(ipath))
        _, pids = retrieve_target_patents(handler, args)
        assignees = handler.query_assignee_by_pid(pids)
        pickle.dump(assignees, open(ipath, 'wb'))
    return assignees


def retrieve_assignee_citation_chains(handler, args):
    """Generate raw assignee citation chains for a set of assignees.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    assignees = retrieve_target_assignees(handler, args)
    opath = os.path.join(args.opath, 'assignee.chain.raw')
    if not os.path.exists(opath):
        os.makedirs(opath)
    assignee_chain_len = {}
    for ix, chains in handler.query_citation_chain_of_assignee(assignees):
        ofp = os.path.join(opath,
                           'assignee.chains.raw.{:03d}.pkl'.format(ix))
        for key, value in chains.items():
            assignee_chain_len[key] = len(value)
        pickle.dump(chains, open(ofp, 'wb'))
    opath = os.path.join(opath, 'assignee.chain.len.pkl')
    pickle.dump(assignee_chain_len, open(opath, 'wb'))


def retrieve_target_inventors(handler, args):
    """Retrieve target inventors associated to target patents.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    ipath = os.path.join(args.opath, 'inventors' +
                         '.min_cites' + str(args.min_num_cites) +
                         '.max_cites' + str(args.max_num_cites) + '.pkl')
    print('Trying to find file of target inventors: {}'.format(ipath))
    if os.path.exists(ipath):
        print('Found file {}'.format(ipath))
        inventors = pickle.load(open(ipath, 'rb'))
    else:
        print('{} not found. Generating ...'.format(ipath))
        _, pids = retrieve_target_patents(handler, args)
        inventors = handler.query_inventor_by_pid(pids)
        pickle.dump(inventors, open(ipath, 'wb'))
    return inventors


def retrieve_inventor_citation_chains(handler, args):
    """Generate citation chains given a set of target inventors.

    Parameters
    ----------
    handler : :class:`Neo4jHandler`
        Handler for neo4j database.
    args : dict
        Command line options.

    """

    inventors = retrieve_target_inventors(handler, args)
    opath = os.path.join(args.opath, 'inventor.chain.raw')
    if not os.path.exists(opath):
        os.makedirs(opath)
    inventor_chain_len = {}
    for ix, chains in handler.query_citation_chain_of_inventor(inventors):
        ofp = os.path.join(opath,
                           'inventor.chains.raw.{:03d}.pkl'.format(ix))
        for key, value in chains.items():
            inventor_chain_len[key] = len(value)
        pickle.dump(chains, open(ofp, 'wb'))
    opath = os.path.join(opath, 'inventor.chain.len.pkl')
    pickle.dump(inventor_chain_len, open(opath, 'wb'))
    return inventors


if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    group = pparser.add_mutually_exclusive_group(required=True)
    group.add_argument('--patent', action='store_true',
                       help='Generate patent citation chain')
    group.add_argument('--assignee', action='store_true',
                       help='Generate assignee citation chain')
    group.add_argument('--inventor', action='store_true',
                       help='Generate inventor citation chain')
    pparser.add_argument('--min-num-cites', type=int, default=20,
                         help='Minimum number of cites received.')
    pparser.add_argument('--max-num-cites', type=int, default=200,
                         help='Maximum number of cites received.')
    pparser.add_argument('--opath', type=str, default='output/',
                         help='Output path.')
    pparser.add_argument('credential', help='Auth file')
    args = pparser.parse_args()
    if not os.path.exists(args.opath):
        os.makedirs(args.opath)
    handler = Neo4jHandler(args.credential, None)
    if args.patent:
        retrieve_patent_citation_chains(handler, args)
    elif args.assignee:
        retrieve_assignee_citation_chains(handler, args)
    elif args.inventor:
        retrieve_inventor_citation_chains(handler, args)
