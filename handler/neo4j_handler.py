# -*- coding: utf-8 -*-

""""""

import datetime

from neo4j.v1 import GraphDatabase
from neo4j.v1.types.spatial import WGS84Point
import numpy as np

from .patentsview_handler import PatentsViewHandler


def to_epoch(date):
    return (date - datetime.datetime(1970, 1, 1)) / datetime.timedelta(days=1)


class Neo4jHandler(object):
    """API interface for manipulations of patent citation network saved in
    neo4j.

    Parameters
    ----------
    credential : str
        Path to credential file.
    data : str
        Dir to data files.

    Attributes
    ----------
    _username : str
        Username
    _password : str
        Password
    _data : str
        Dir to data files.

    """

    def __init__(self, credential, data):
        super(Neo4jHandler, self).__init__()
        with open(credential, 'r') as ifp:
            lines = ifp.readlines()
            self._username = lines[0].strip()
            self._password = lines[1].strip()
        self._data = data

    def create_patent_nodes(self, chunks=300):
        """CREATE patent nodes in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading patent nodes.')
        data = PatentsViewHandler(self._data).construct_patent_nodes(chunks)
        print('Finish loading patent nodes.')
        with graph.session() as session:
            session.run(('CREATE CONSTRAINT ON (p:Patent) '
                         'ASSERT p.pid IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for pid, attrs in chunk.iterrows():
                    self.create_patent_node(tx, pid, attrs)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_patent_node(self, tx, pid, attrs):
        """CREATE one patent node.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        pid : str
            Patent id.
        attrs : :class:`pandas.DataFrame`
            Attributes associated with this patent.

        """

        attrs = attrs.where(attrs.notnull(), None).to_dict()
        attrs['pid'] = pid
        attrs['date'] = attrs['date'].date()
        try:
            attrs['application_date'] = datetime.date.fromisoformat(
                    attrs['application_date'])
        except ValueError:  # invalid date, e.g., '1968-05-00'
            attrs['application_date'] = None
        st = ('CREATE (p:Patent {pid: $pid, type: $type, date: $date, '
              'application_id: $application_id, series_code: $series_code, '
              'application_date: $application_date, dependent: $dependent, '
              'independent: $independent, cpc_section: $cpc_section, '
              'cpc_subsection: $cpc_subsection, cpc_group: $cpc_group, '
              'cpc_subgroup: $cpc_subgroup, ipc_section: $ipc_section, '
              'ipc_class: $ipc_class, ipc_subclass: $ipc_subclass, '
              'ipc_main_group: $ipc_main_group, ipc_subgroup: $ipc_subgroup, '
              'nber_category: $nber_category, '
              'nber_subcategory: $nber_subcategory, '
              'upc_category: $upc_category, upc_subclass: $upc_subclass, '
              'foreigncitation: $foreigncitation, '
              'otherreference: $otherreference, '
              'applicationcitation: $applicationcitation})')
        tx.run(st, **attrs)

    def create_citation_relationships(self, chunks=1000):
        """MERGE citation relationships in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading citation relationships.')
        data = PatentsViewHandler(self._data).construct_patent_citations(
                chunks)
        print('Finish loading citation relationships.')
        with graph.session() as session:
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for pid, cites in chunk.iterrows():
                    self.create_citation_relationship(tx, pid, cites)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_citation_relationship(self, tx, pid, cites):
        """CREATE citation relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        pid : str
            The select patent.
        cites : :class:`pandas.DataFrame`
            List of patent ids cited by pid.

        """

        for cite in cites['citation_id']:
            st = ('MATCH (a:Patent {pid: $pid}), (b:Patent {pid: $cite}) '
                  'MERGE (a)-[:CITE]->(b)')
            tx.run(st, pid=pid, cite=cite)

    def create_assignee_nodes(self, chunks=50):
        """CREATE assignee nodes in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading assignee nodes.')
        data = PatentsViewHandler(self._data).construct_assignee_nodes(chunks)
        print('Finish loading assignee nodes.')
        with graph.session() as session:
            session.run(('CREATE CONSTRAINT ON (a:Assignee) '
                         'ASSERT a.assignee_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for assignee_id, attrs in chunk.iterrows():
                    self.create_assignee_node(tx, assignee_id, attrs)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_assignee_node(self, tx, assignee_id, attrs):
        """CREATE one assignee node.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        assignee_id : str
            Assignee id.
        attrs : :class:`pandas.DataFrame`
            Attributes associated with this assignee.

        """

        attrs = attrs.where(attrs.notnull(), None).to_dict()
        attrs['assignee_id'] = assignee_id
        attrs = {key: value.strip() for key, value in attrs.items()}
        statement = ('CREATE (a:Assignee {assignee_id: $assignee_id, '
                     'assignee_name: $assignee_name, '
                     'assignee_type: $assignee_type})')
        tx.run(statement, **attrs)

    def create_patent_assignee_relationships(self, chunks=600):
        """CREATE patent-assignee relationships in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading patent-assignee relationships.')
        data = PatentsViewHandler(self._data).construct_patent_assignee_edges(
                chunks)
        print('Finish loading patent-assignee relationships.')
        with graph.session() as session:
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for pid, assignee_ids in chunk.iterrows():
                    self.create_patent_assignee_relationship(tx, pid,
                                                             assignee_ids)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_patent_assignee_relationship(self, tx, pid, assignee_ids):
        """Insert patent-assignee relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        pid : str
            Patent id.
        assignee_ids : list
            List of assignees who own the select patent.

        """

        assignee_ids = assignee_ids['assignee_id']
        for assignee_id in assignee_ids:
            assignee_id = assignee_id.strip()
            statement = ('MATCH (a:Assignee {assignee_id: $assignee_id}), '
                         '(b:Patent {pid: $pid}) MERGE (a)-[:OWN]->(b)')
            tx.run(statement, assignee_id=assignee_id, pid=pid)

    def create_inventor_nodes(self, chunks=100):
        """CREATE inventor nodes in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading inventor nodes.')
        data = PatentsViewHandler(self._data).construct_inventor_nodes(chunks)
        print('Finish loading inventor nodes.')
        with graph.session() as session:
            session.run(('CREATE CONSTRAINT ON (i:Inventor) '
                         'ASSERT i.inventor_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for inventor_id, attrs in chunk.iterrows():
                    self.create_inventor_node(tx, inventor_id, attrs)
                tx.commit()
                print('Finisth working on {}-th batch'.format(ix))

    def create_inventor_node(self, tx, inventor_id, attrs):
        """Insert one inventor node.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        inventor_id : str
            Inventor id.
        attrs : :class:`pandas.DataFrame`
            Attributes associated to the select inventor.

        """

        attrs = attrs.where(attrs.notnull(), None).to_dict()
        attrs['inventor_name'] = attrs['inventor_name'].strip()
        attrs['inventor_id'] = inventor_id
        statement = ('CREATE (a:Inventor {inventor_id: $inventor_id, '
                     'inventor_name: $inventor_name})')
        tx.run(statement, **attrs)

    def create_patent_inventor_relationships(self, chunks=600):
        """CREATE patent-inventor relationship in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading patent-inventor relationships.')
        handler = PatentsViewHandler(self._data)
        data = handler.construct_patent_inventor_edges(chunks)
        print('Finish loading patent-inventor relationships.')
        with graph.session() as session:
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for pid, attrs in chunk.iterrows():
                    self.create_patent_inventor_relationship(tx, pid, attrs)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_patent_inventor_relationship(self, tx, pid, inventor_ids):
        """Insert patent-inventor relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        pid : str
            Id of the selected patent.
        inventor_ids : list
            List of inventors who invented the selected patent.

        """

        inventor_ids = inventor_ids['inventor_id']
        for inventor_id in inventor_ids:
            statement = ('MATCH (a:Inventor {inventor_id: $inventor_id}), '
                         '(b:Patent {pid: $pid}) MERGE (a)-[:INVENT]->(b)')
            tx.run(statement, inventor_id=inventor_id, pid=pid)

    def create_location_nodes(self, chunks=50):
        """CREATE location nodes in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading location nodes.')
        data = PatentsViewHandler(self._data).construct_location_nodes(chunks)
        print('Finish loading location nodes.')
        with graph.session() as session:
            session.run(('CREATE CONSTRAINT ON (l:Location) '
                         'ASSERT l.location_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for location_id, attrs in chunk.iterrows():
                    self.create_location_node(tx, location_id, attrs)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_location_node(self, tx, location_id, attrs):
        """Insert one location node.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        location_id : str
            Location id.
        attrs : :class:`pandas.DataFrame`
            Attributes associated with this location.

        """

        attrs = attrs.where(attrs.notnull(), None).to_dict()
        attrs['location_id'] = location_id
        attrs['gps'] = WGS84Point(
                (float(attrs['longitude']), float(attrs['latitude'])))
        del attrs['longitude']
        del attrs['latitude']
        statement = ('CREATE (a:Location {location_id: $location_id, '
                     'city: $city, state: $state, country: $country, '
                     'gps: $gps, county: $county, state_fips: $state_fips, '
                     'county_fips: $county_fips})')
        tx.run(statement, **attrs)

    def create_assignee_location_relationships(self, chunks=50):
        """CREATE assignee-location relationship in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading patent-inventor relationships.')
        handler = PatentsViewHandler(self._data)
        data = handler.construct_assignee_location_edges(chunks)
        print('Finish loading patent-inventor relationships.')
        with graph.session() as session:
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for assignee_id, location_id in chunk.iterrows():
                    self.create_assignee_location_relationship(tx, assignee_id,
                                                               location_id)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_assignee_location_relationship(self, tx, assignee_id,
                                              location_id):
        """Insert assignee-location relationship for the select assignee.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        assignee_id : str
            Id of the selected assignee.
        location_id : list
            List of locations associated to the selected assignee.

        """

        location_ids = location_id['location_id']
        for location_id in location_ids:
            statement = ('MATCH (a:Assignee {assignee_id: $assignee_id}), '
                         '(b:Location {location_id: $location_id}) '
                         'MERGE (a)-[:LOCATE_AT]->(b)')
            tx.run(statement, location_id=location_id, assignee_id=assignee_id)

    def create_inventor_location_relationships(self, chunks=300):
        """CREATE inventor-location relationship in neo4j database.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading inventor-location relationships.')
        handler = PatentsViewHandler(self._data)
        data = handler.construct_inventor_location_edges(chunks)
        print('Finish loading inventor-location relationships.')
        with graph.session() as session:
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('Working on {}-th batch'.format(ix))
                tx = session.begin_transaction()
                for inventor_id, location_id in chunk.iterrows():
                    self.create_inventor_location_relationship(tx, inventor_id,
                                                               location_id)
                tx.commit()
                print('Finish working on {}-th batch'.format(ix))

    def create_inventor_location_relationship(self, tx, inventor_id,
                                              location_id):
        """Insert inventor-location relationship for the select assignee.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        inventor_id : str
            Id of the selected inventor.
        location_id : list
            List of locations associated to the selected inventor.

        """

        location_ids = location_id['location_id']
        for location_id in location_ids:
            statement = ('MATCH (a:Inventor {inventor_id: $inventor_id}), '
                         '(b:Location {location_id: $location_id}) '
                         'MERGE (a)-[:LOCATE_AT]->(b)')
            tx.run(statement, location_id=location_id, inventor_id=inventor_id)

    def query_patents_by_time_window(self, tw, min_num=5):
        """Query patents by time window.

        Parameters
        ----------
        tw : list
            Time window (start_time, end_time), e.g., ('1976-01-01',
            '1976-12-31').
        min_num : int
            Patents which recived less than min_num citations are dropped.

        Returns
        -------
        list
            List of pid of elected patents.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        start, end = tw
        with graph.session() as session:
            statement = ('MATCH (p:Patent)<-[:CITE]-(cites:Patent) '
                         'WHERE p.date >= date($s) and p.date <= date($e) '
                         'WITH p, count(distinct cites) AS num_cites '
                         'WHERE num_cites > $num '
                         'RETURN p.pid ORDER BY p.date DESC')
            result = session.run(statement, s=start, e=end, num=min_num)
            return [e[0] for e in result.values()]

    def query_citation_chain(self, pids):
        """Query the citation chain for a given pid.

        Parameters
        ----------
        pids : list
            List of pids to query.

        Returns
        -------
        ix : int
            The ix-th chunk.
        list
            For each pid, a citation chain is generated.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        pid_chunks = np.array_split(pids, 50)
        with graph.session() as session:
            for ix, pid_chunk in enumerate(pid_chunks):
                chains = []
                tx = session.begin_transaction()
                for pid in pid_chunk:
                    statement = ('MATCH (p:Patent)<-[:CITE]-(cites:Patent) '
                                 'WHERE p.pid = $pid '
                                 'RETURN p, cites ORDER BY cites.date ASC')
                    result = tx.run(statement, pid=pid).values()
                    patent = dict(result[0][0])  # target patent
                    patent['date'] = datetime.datetime(
                            patent['date'].year, patent['date'].month,
                            patent['date'].day)
                    if 'application_date' in patent:
                        patent['application_date'] = datetime.datetime(
                                patent['application_date'].year,
                                patent['application_date'].month,
                                patent['application_date'].day)
                    chain = [patent]
                    for pair in result:
                        patent = dict(pair[1])
                        patent['date'] = datetime.datetime(
                                patent['date'].year, patent['date'].month,
                                patent['date'].day)
                        if 'application_date' in patent:
                            patent['application_date'] = datetime.datetime(
                                    patent['application_date'].year,
                                    patent['application_date'].month,
                                    patent['application_date'].day)
                        chain.append(patent)
                    chains.append(chain)
                tx.commit()
                print('{}/{}'.format(ix + 1, len(pid_chunks)))
                yield ix, chains

    def query_assignee_by_pid(self, pids):
        """Get assignees associated with the patents of interest.

        Parameters
        ----------
        pids : list
            Patents of interest.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        pid_chunks = np.array_split(pids, 50)
        with graph.session() as session:
            for ix, pid_chunk in enumerate(pid_chunks):
                assignee_ids = set()
                tx = session.begin_transaction()
                for pid in pid_chunk:
                    statement = ('MATCH (p:Patent)<-[:OWN]-(a:Assignee) '
                                 'WHERE p.pid = $pid '
                                 'RETURN a.assignee_id')
                    result = tx.run(statement, pid=pid).values()
                    assignee_ids |= set(np.array(result).flatten().tolist())
                tx.commit()
                print('{}/{}'.format(ix + 1, len(pid_chunks)))
                yield assignee_ids

    def query_citation_chain_of_assignee(self, aids):
        """Query the citation chain for a given assignee.

        Parameters
        ----------
        aids : list
            List of assignee ids to query.

        Returns
        -------
        ix : int
            The ix-th chunk.
        list
            For each pid, a citation chain is generated.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        aid_chunks = np.array_split(aids, 200)
        with graph.session() as session:
            for ix, aid_chunk in enumerate(aid_chunks):
                print('Working on assignee chunks {:03d}/{:03d}'.format(
                    ix + 1, 200))
                chains = {}
                tx = session.begin_transaction()
                for aid in aid_chunk:
                    statement = ('MATCH (cites:Patent)-[:CITE]->(p:Patent)'
                                 '<-[:OWN]-(a:Assignee) '
                                 'WHERE a.assignee_id = $aid '
                                 'WITH DISTINCT cites '
                                 'RETURN cites.pid, cites.date '
                                 'ORDER BY cites.date ASC')
                    result = tx.run(statement, aid=aid).values()
                    chain = []  # generate citation chain for each aid
                    for patent in result:
                        pid, date = patent
                        date = datetime.datetime(date.year, date.month,
                                                 date.day)
                        chain.append((pid, to_epoch(date)))
                    chains[aid] = chain
                tx.commit()
                print('Finish working on {}/{}'.format(ix + 1, 200))
                yield ix, chains

    def query_pid2aid(self, pids):
        """Get assignees associated with the patents of interest.

        Parameters
        ----------
        pids : list
            Patents of interest.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        pid_chunks = np.array_split(pids, 50)
        with graph.session() as session:
            for ix, pid_chunk in enumerate(pid_chunks):
                tx = session.begin_transaction()
                pairs_chunk = {}
                for pid in pid_chunk:
                    statement = ('MATCH (p:Patent)<-[:OWN]-(a:Assignee) '
                                 'WHERE p.pid = $pid '
                                 'RETURN a.assignee_id')
                    result = tx.run(statement, pid=pid).values()
                    matched_assignees = np.array(result).flatten().tolist()
                    if matched_assignees:
                        pairs_chunk[pid] = matched_assignees
                tx.commit()
                print('{}/{}'.format(ix + 1, len(pid_chunks)))
                yield pairs_chunk

    def query_inventor_by_pid(self, pids):
        """Get inventors associated with the patents of interest.

        Parameters
        ----------
        pids : list
            Patents of interest.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        pid_chunks = np.array_split(pids, 50)
        with graph.session() as session:
            for ix, pid_chunk in enumerate(pid_chunks):
                inventor_ids = set()
                tx = session.begin_transaction()
                for pid in pid_chunk:
                    statement = ('MATCH (p:Patent)<-[:INVENT]-(i:Inventor) '
                                 'WHERE p.pid = $pid '
                                 'RETURN i.inventor_id')
                    result = tx.run(statement, pid=pid).values()
                    inventor_ids |= set(np.array(result).flatten().tolist())
                tx.commit()
                print('{}/{}'.format(ix + 1, len(pid_chunks)))
                yield inventor_ids

    def query_citation_chain_of_inventor(self, iids):
        """Query the citation chain for a given inventor.

        Parameters
        ----------
        iids : list
            List of inventor ids to query.

        Returns
        -------
        ix : int
            The ix-th chunk.
        list
            For each pid, a citation chain is generated.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        iid_chunks = np.array_split(iids, 200)
        with graph.session() as session:
            for ix, iid_chunk in enumerate(iid_chunks):
                print('Working on inventor chunks {:03d}/{:03d}'.format(
                    ix + 1, 200))
                chains = {}
                tx = session.begin_transaction()
                for iid in iid_chunk:
                    statement = ('MATCH (cites:Patent)-[:CITE]->(p:Patent)'
                                 '<-[:INVENT]-(a:Inventor) '
                                 'WHERE a.inventor_id = $iid '
                                 'WITH DISTINCT cites '
                                 'RETURN cites.pid, cites.date '
                                 'ORDER BY cites.date ASC')
                    result = tx.run(statement, iid=iid).values()
                    chain = []
                    for patent in result:
                        pid, date = patent
                        date = datetime.datetime(date.year, date.month,
                                                 date.day)
                        chain.append((pid, to_epoch(date)))
                    chains[iid] = chain
                tx.commit()
                print('Finished working on {}/{}'.format(ix + 1, 200))
                yield ix, chains

    def query_pid2iid(self, pids):
        """Get inventors associated with the patents of interest.

        Parameters
        ----------
        pids : list
            Patents of interest.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        pid_chunks = np.array_split(pids, 50)
        with graph.session() as session:
            for ix, pid_chunk in enumerate(pid_chunks):
                tx = session.begin_transaction()
                pairs_chunk = {}
                for pid in pid_chunk:
                    statement = ('MATCH (p:Patent)<-[:INVENT]-(i:Inventor) '
                                 'WHERE p.pid = $pid '
                                 'RETURN i.inventor_id')
                    result = tx.run(statement, pid=pid).values()
                    matched_inventors = np.array(result).flatten().tolist()
                    if matched_inventors:
                        pairs_chunk[pid] = matched_inventors
                tx.commit()
                print('{}/{}'.format(ix + 1, len(pid_chunks)))
                yield pairs_chunk

    def query_assignee_relationship(self, pids):
        """

        Parameters
        ----------
        pids : list
            List of assignee ids to query.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        aca = []
        with graph.session() as session:
            tx = session.begin_transaction()
            for pid in pids:
                statement = ('MATCH (A1:Assignee)-[:OWN]->(p1:Patent)'
                             '<-[:CITE]-(p2:Patent)<-[:OWN]-(A2:Assignee) '
                             'WHERE p2.pid = $pid AND p1.date >= date($s) '
                             'RETURN A2.assignee_name, A1.assignee_name')
                result = tx.run(statement, pid=pid, s='2015-01-01').values()
                if result:
                    aca += result
            tx.commit()
        return aca
