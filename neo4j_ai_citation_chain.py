# -*- coding: utf-8 -*-

"""Generate assignee and inventor citation chain.

Examples
--------

.. code-block:: bash

   python neo4j_ai_citation_chain.py credential.txt \
output/patent.citation.chain/target.pids.pkl

"""

import argparse
import pickle
import os

from techflow.neo4j_handler import Neo4jHandler

basepath_assignee = 'output/assignee.citation.chain/'
basepath_inventor = 'output/inventor.citation.chain/'

if not os.path.exists(basepath_assignee):
    os.makedirs(basepath_assignee)
if not os.path.exists(basepath_inventor):
    os.makedirs(basepath_inventor)


def find_assignees_and_inventors_by_pids(args):
    """Given a set of target pids, tind associated target assignees and
    inventors. Will dump to target.assignee.pkl and target.inventor.pkl."""
    assignee_path = os.path.join(os.path.join(basepath_assignee,
                                              'target.assignee.pkl'))
    inventor_path = os.path.join(os.path.join(basepath_inventor,
                                              'target.inventor.pkl'))
    if os.path.exists(inventor_path) and os.path.exists(assignee_path):
        print('Loading target assignees and target inventors.')
        return pickle.load(open(assignee_path, 'rb')),\
            pickle.load(open(inventor_path, 'rb'))
    # generate data if datafile doesn't exist
    assignees, inventors = set(), set()
    pids = list(pickle.load(open(args.ipath, 'rb')))
    for assignee_ids in handler.query_assignee_by_pid(pids):
        assignees |= assignee_ids
    for inventor_ids in handler.query_inventor_by_pid(pids):
        inventors |= inventor_ids
    pickle.dump(assignees, open(assignee_path, 'wb'))
    pickle.dump(inventors, open(inventor_path, 'wb'))
    return assignees, inventors


def get_assignee_citation_chain(assignees):
    """Generate raw assignee citation chains for a set of assignees.
    {aid : citation_chain}. Will dump to assignee.chains.raw.[001].pkl
    and assignee.chains.len.pkl"""
    assignee_chain_len, assignees = {}, list(assignees)
    for ix, chains in handler.query_citation_chain_of_assignee(assignees):
        opath = os.path.join(basepath_assignee,
                             'assignee.chains.raw.{:03d}.pkl'.format(ix + 1))
        for key, value in chains.items():
            assignee_chain_len[key] = len(value)
        pickle.dump(chains, open(opath, 'wb'))
    opath = os.path.join(basepath_assignee, 'assignee.chains.len.pkl')
    pickle.dump(assignee_chain_len, open(opath, 'wb'))


def get_inventor_citation_chain(inventors):
    """Generate raw inventor citation chains for a set of inventors.
    {iid: citation chain}. Will dump to inventor.chains.raw.[001].pkl
    and inventor.chains.len.pkl"""
    inventor_chain_len, inventors = {}, list(inventors)
    for ix, chains in handler.query_citation_chain_of_inventor(inventors):
        opath = os.path.join(basepath_inventor,
                             'inventor.chains.raw.{:03d}.pkl'.format(ix + 1))
        for key, value in chains.items():
            inventor_chain_len[key] = len(value)
        pickle.dump(chains, open(opath, 'wb'))
    opath = os.path.join(basepath_inventor, 'inventor.chains.len.pkl')
    pickle.dump(inventor_chain_len, open(opath, 'wb'))


def select_the_longest_history(args, assignee_chain_len, inventor_chain_len):
    """Select the longest aid and iid for each pid.

    It's supposed that every aid can find its place in assignee_chain_len, and
    associated value should be a positive number. inventor_chain_len is the
    same.

    """

    assignee_chain_len = pickle.load(open(assignee_chain_len, 'rb'))
    inventor_chain_len = pickle.load(open(inventor_chain_len, 'rb'))
    pids = list(pickle.load(open(args.ipath, 'rb')))
    pid2assignee, pid2inventor = {}, {}
    for pairs in handler.query_pid2aid(pids):  # target pids
        for pid, aids in pairs.items():  # {pid: aids}
            tmp = sorted([(aid, assignee_chain_len[aid]) for aid in aids],
                         key=lambda x: x[1], reverse=True)
            pid2assignee[pid] = tmp[0][0]
    for pairs in handler.query_pid2iid(pids):  # target pids
        for pid, iids in pairs.items():  # {pid: iids}
            tmp = sorted([(iid, inventor_chain_len[iid]) for iid in iids],
                         key=lambda x: x[1], reverse=True)
            pid2inventor[pid] = tmp[0][0]
    pickle.dump(pid2assignee, open(os.path.join(basepath_assignee,
                                                'pid2assignee.pkl'), 'wb'))
    pickle.dump(pid2inventor, open(os.path.join(basepath_inventor,
                                                'pid2inventor.pkl'), 'wb'))


if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file.')
    pparser.add_argument('ipath', help='Target pids')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, None)
    assignees, inventors = find_assignees_and_inventors_by_pids(args)
    if not os.path.exists(os.path.join(basepath_assignee,
                                       'assignee.chains.len.pkl')):
        get_assignee_citation_chain(assignees)
    if not os.path.exists(os.path.join(basepath_inventor,
                                       'inventor.chains.len.pkl')):
        get_inventor_citation_chain(inventors)
    assignee_chain_len = os.path.join(basepath_assignee,
                                      'assignee.chains.len.pkl')
    inventor_chain_len = os.path.join(basepath_inventor,
                                      'inventor.chains.len.pkl')
    select_the_longest_history(args, assignee_chain_len, inventor_chain_len)
