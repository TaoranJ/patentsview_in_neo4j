# -*- coding: utf-8 -*-

import datetime

from neo4j import GraphDatabase
from neo4j.types.spatial import WGS84Point
import numpy as np

from .patentsview_handler import PatentsViewHandler


def to_epoch(date):
    return (date - datetime(1970, 1, 1)) / datetime.timedelta(days=1)


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

    def load_patentsview(self):
        """Load PatentsView dataset into Neo4j database."""
        self.create_patent_nodes()
        self.create_assignee_nodes()
        self.create_inventor_nodes()
        self.create_location_nodes()
        self.create_citation_relationships()
        self.create_patent_assignee_relationships()
        self.create_patent_inventor_relationships()
        self.create_assignee_location_relationships()
        self.create_inventor_location_relationships()
        self.create_cpc_nodes_and_edges()
        self.create_uspc_nodes_and_edges()
        self.create_ipcr_nodes_and_edges()
        self.create_nber_nodes_and_edges()

    def create_patent_nodes(self, chunks=500):
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
            session.run(('CREATE CONSTRAINT ON (p:patent) '
                         'ASSERT p.pid IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:03d}/{:03d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for pid, attrs in chunk.iterrows():
                    self.create_patent_node(tx, pid, attrs)
                tx.commit()
            session.run('CREATE INDEX ON :patent(date)')

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
            attrs['application_date'] = datetime.datetime.strptime(
                    attrs['application_date'], '%Y-%m-%d').date()
        except ValueError:  # invalid date, e.g., '1968-05-00'
            attrs['application_date'] = None
        st = ('CREATE (p:patent {pid: $pid, type: $type, date: $date, '
              'application_id: $application_id, series_code: $series_code, '
              'application_date: $application_date, dependent: $dependent, '
              'independent: $independent, foreigncitation: $foreigncitation, '
              'otherreference: $otherreference, '
              'applicationcitation: $applicationcitation})')
        tx.run(st, **attrs)

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
            session.run(('CREATE CONSTRAINT ON (a:assignee) '
                         'ASSERT a.assignee_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:03d}/{:03d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for assignee_id, attrs in chunk.iterrows():
                    self.create_assignee_node(tx, assignee_id, attrs)
                tx.commit()

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
        attrs = {key: value.strip() if value else ''
                 for key, value in attrs.items()}
        statement = ('CREATE (a:assignee {assignee_id: $assignee_id, '
                     'assignee_name: $assignee_name, '
                     'assignee_type: $assignee_type})')
        tx.run(statement, **attrs)

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
            session.run(('CREATE CONSTRAINT ON (i:inventor) '
                         'ASSERT i.inventor_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:03d}/{:03d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for inventor_id, attrs in chunk.iterrows():
                    self.create_inventor_node(tx, inventor_id, attrs)
                tx.commit()

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
        statement = ('CREATE (a:inventor {inventor_id: $inventor_id, '
                     'inventor_name: $inventor_name})')
        tx.run(statement, **attrs)

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
            session.run(('CREATE CONSTRAINT ON (l:location) '
                         'ASSERT l.location_id IS UNIQUE'))
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:03d}/{:03d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for location_id, attrs in chunk.iterrows():
                    self.create_location_node(tx, location_id, attrs)
                tx.commit()

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
        statement = ('CREATE (a:location {location_id: $location_id, '
                     'city: $city, state: $state, country: $country, '
                     'gps: $gps, county: $county, state_fips: $state_fips, '
                     'county_fips: $county_fips})')
        tx.run(statement, **attrs)

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
                print('[BATCH {:04d}/{:04d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, citation in chunk.iterrows():
                    self.create_citation_relationship(tx, citation)
                tx.commit()

    def create_citation_relationship(self, tx, citation):
        """CREATE citation relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        citation : str
            patent id: citation_id.

        """

        st = ('MATCH (a:patent {pid: $pid}), (b:patent {pid: $cite}) '
              'MERGE (a)-[:CITES]->(b)')
        tx.run(st, pid=str(citation['patent_id']),
               cite=str(citation['citation_id']))

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
                print('[BATCH {:04d}/{:04d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_patent_assignee_relationship(tx, rel)
                tx.commit()

    def create_patent_assignee_relationship(self, tx, rel):
        """Insert patent-assignee relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.
        rel : :class:`pandas.DataFrame`
            Patent id: assignee id

        """

        statement = ('MATCH (a:assignee {assignee_id: $assignee_id}), '
                     '(b:patent {pid: $pid}) MERGE (a)-[:OWNS]->(b)')
        tx.run(statement, assignee_id=rel['assignee_id'].strip(),
               pid=rel['patent_id'].strip())

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
                print('[BATCH {:04d}/{:04d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_patent_inventor_relationship(tx, rel)
                tx.commit()

    def create_patent_inventor_relationship(self, tx, rel):
        """Insert patent-inventor relationships for the select patent.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        statement = ('MATCH (a:inventor {inventor_id: $inventor_id}), '
                     '(b:patent {pid: $pid}) MERGE (a)-[:INVENTS]->(b)')
        tx.run(statement, inventor_id=str(rel['inventor_id']),
               pid=str(rel['patent_id']))

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
                print('[BATCH {:04d}/{:04d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_assignee_location_relationship(tx, rel)
                tx.commit()

    def create_assignee_location_relationship(self, tx, rel):
        """Insert assignee-location relationship for the select assignee.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        statement = ('MATCH (a:assignee {assignee_id: $assignee_id}), '
                     '(b:location {location_id: $location_id}) '
                     'MERGE (a)-[:LOCATES_AT]->(b)')
        tx.run(statement, location_id=str(rel['location_id']),
               assignee_id=str(rel['assignee_id']))

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
                print('[BATCH {:04d}/{:04d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_inventor_location_relationship(tx, rel)
                tx.commit()

    def create_inventor_location_relationship(self, tx, rel):
        """Insert inventor-location relationship for the select assignee.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        statement = ('MATCH (a:inventor {inventor_id: $inventor_id}), '
                     '(b:location {location_id: $location_id}) '
                     'MERGE (a)-[:LOCATES_AT]->(b)')
        tx.run(statement, location_id=str(rel['location_id']),
               inventor_id=str(rel['inventor_id']))

    def create_cpc_nodes_and_edges(self, chunks=10000):
        """Create cpc nodes and edges.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading cpc nodes.')
        handler = PatentsViewHandler(self._data)
        nodes, data = handler.construct_cpc_nodes(chunks)
        print('Finish loading cpc nodes.')
        with graph.session() as session:
            self.create_cpc_nodes(session, nodes)
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:05d}/{:05d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_cpc_edge(tx, rel)
                tx.commit()

    def create_cpc_nodes(self, session, nodes):
        """Create cpc nodes."""

        session.run(('CREATE CONSTRAINT ON (c:cpc_section) '
                    'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:cpc_subsection) '
                     'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:cpc_group) '
                     'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:cpc_subgroup) '
                     'ASSERT c.id IS UNIQUE'))
        items = {0: 'cpc_section', 1: 'cpc_subsection', 2: 'cpc_group',
                 3: 'cpc_subgroup'}
        for k, v in items.items():
            tx = session.begin_transaction()
            for node in nodes[k]:
                node_type = 'MERGE (c:{} '.format(str(v))
                st = (node_type + '{id: $id})')
                tx.run(st, id=str(node))
            tx.commit()

    def create_cpc_edge(self, tx, rel):
        """Insert patent-cpc relationship.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        st = ('MATCH (p:patent {pid: $pid}), (c:cpc_section {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['cpc_section']))
        st = ('MATCH (p:patent {pid: $pid}), (c:cpc_subsection {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['cpc_subsection']))
        st = ('MATCH (p:patent {pid: $pid}), (c:cpc_group {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['cpc_group']))
        st = ('MATCH (p:patent {pid: $pid}), (c:cpc_subgroup {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['cpc_subgroup']))

    def create_uspc_nodes_and_edges(self, chunks=10000):
        """Create uspc nodes and edges.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading uspc nodes.')
        handler = PatentsViewHandler(self._data)
        nodes, data = handler.construct_uspc_nodes(chunks)
        print('Finish loading uspc nodes.')
        with graph.session() as session:
            self.create_uspc_nodes(session, nodes)
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:05d}/{:05d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_uspc_edge(tx, rel)
                tx.commit()

    def create_uspc_nodes(self, session, nodes):
        """Create uspc nodes."""

        session.run(('CREATE CONSTRAINT ON (c:uspc_mainclass) '
                    'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:uspc_subclass) '
                     'ASSERT c.id IS UNIQUE'))
        items = {0: 'uspc_mainclass', 1: 'uspc_subclass'}
        for k, v in items.items():
            tx = session.begin_transaction()
            for node in nodes[k]:
                node_type = 'MERGE (c:{} '.format(str(v))
                st = (node_type + '{id: $id})')
                tx.run(st, id=str(node))
            tx.commit()

    def create_uspc_edge(self, tx, rel):
        """Insert patent-cpc relationship.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        st = ('MATCH (p:patent {pid: $pid}), (c:uspc_mainclass {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['uspc_mainclass']))
        st = ('MATCH (p:patent {pid: $pid}), (c:uspc_subclass {id: $id}) '
              'MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['uspc_subclass']))

    def create_ipcr_nodes_and_edges(self, chunks=10000):
        """Create ipcr nodes and edges.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading ipcr nodes.')
        handler = PatentsViewHandler(self._data)
        nodes, data = handler.construct_ipcr_nodes(chunks)
        print('Finish loading ipcr nodes.')
        with graph.session() as session:
            self.create_ipcr_nodes(session, nodes)
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:05d}/{:05d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_ipcr_edge(tx, rel)
                tx.commit()

    def create_ipcr_nodes(self, session, nodes):
        """Create uspc nodes."""

        session.run(('CREATE CONSTRAINT ON (c:ipcr_section) '
                    'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:ipcr_class) '
                     'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:ipcr_subclass) '
                     'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:ipcr_group) '
                     'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:ipcr_subgroup) '
                     'ASSERT c.id IS UNIQUE'))
        items = {0: 'ipcr_section', 1: 'ipcr_class', 2: 'ipcr_subclass',
                 3: 'ipcr_group', 4: 'ipcr_subgroup'}
        for k, v in items.items():
            tx = session.begin_transaction()
            for node in nodes[k]:
                node_type = 'MERGE (c:{} '.format(str(v))
                st = (node_type + '{id: $id})')
                tx.run(st, id=str(node))
            tx.commit()

    def create_ipcr_edge(self, tx, rel):
        """Insert patent-cpc relationship.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:ipcr_section {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['ipcr_section']))
        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:ipcr_class {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['ipcr_class']))
        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:ipcr_subclass {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['ipcr_subclass']))
        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:ipcr_group {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['ipcr_group']))
        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:ipcr_subgroup {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['ipcr_subgroup']))

    def create_nber_nodes_and_edges(self, chunks=10000):
        """Create nber nodes and edges.

        Parameters
        ----------
        chunks : int
            Number of batches to process.

        """

        graph = GraphDatabase.driver('bolt://localhost:7687',
                                     auth=(self._username, self._password))
        print('Loading nber nodes.')
        handler = PatentsViewHandler(self._data)
        nodes, data = handler.construct_nber_nodes(chunks)
        print('Finish loading nber nodes.')
        with graph.session() as session:
            self.create_nber_nodes(session, nodes)
            for ix, chunk in enumerate(data, start=1):  # batch insert
                print('[BATCH {:05d}/{:05d}]'.format(ix, chunks))
                tx = session.begin_transaction()
                for index, rel in chunk.iterrows():
                    self.create_nber_edge(tx, rel)
                tx.commit()

    def create_nber_nodes(self, session, nodes):
        """Create uspc nodes."""

        session.run(('CREATE CONSTRAINT ON (c:nber_category) '
                    'ASSERT c.id IS UNIQUE'))
        session.run(('CREATE CONSTRAINT ON (c:nber_subcategory) '
                     'ASSERT c.id IS UNIQUE'))
        items = {0: 'nber_category', 1: 'nber_subcategory'}
        for k, v in items.items():
            tx = session.begin_transaction()
            for node in nodes[k]:
                node_type = 'MERGE (c:{} '.format(str(v))
                st = (node_type + '{id: $id})')
                tx.run(st, id=str(node))
            tx.commit()

    def create_nber_edge(self, tx, rel):
        """Insert patent-cpc relationship.

        Parameters
        ----------
        tx : :class:`neo4j.Database.session.transaction`
            A neo4j transaction.

        """

        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:nber_category {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['nber_category']))
        st = ('MATCH (p:patent {pid: $pid}), '
              '(c:nber_subcategory {id: $id}) MERGE (p)-[:BELONGS_TO]->(c)')
        tx.run(st, pid=str(rel['patent_id']), id=str(rel['nber_subcategory']))

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
                    patent['date'] = datetime(
                            patent['date'].year, patent['date'].month,
                            patent['date'].day)
                    if 'application_date' in patent:
                        patent['application_date'] = datetime(
                                patent['application_date'].year,
                                patent['application_date'].month,
                                patent['application_date'].day)
                    chain = [patent]
                    for pair in result:
                        patent = dict(pair[1])
                        patent['date'] = datetime(
                                patent['date'].year, patent['date'].month,
                                patent['date'].day)
                        if 'application_date' in patent:
                            patent['application_date'] = datetime(
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
                        date = datetime(date.year, date.month, date.day)
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
                        date = datetime(date.year, date.month, date.day)
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
