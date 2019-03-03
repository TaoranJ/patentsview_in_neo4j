# -*- coding: utf-8 -*-


import os
import collections

import pandas as pd
import numpy as np


class PatentsViewHandler(object):
    """Class handling PatentsView data.

    Parameters
    ----------
    ipath : str
        Dir to PatentsView data.

    Attributes
    ----------
    _ipath : str
        Dir to PatentsView data.

    """

    def __init__(self, ipath):
        super(PatentsViewHandler, self).__init__()
        self._ipath = ipath

    def construct_patent_nodes(self, chunks=None):
        """Construct patent nodes.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        opath = os.path.join(self._ipath, 'patent.node.pkl.bz2')
        if os.path.exists(opath):
            patents = pd.read_pickle(opath)
            return np.array_split(patents, chunks) if chunks else patents
        patents = self._patent()
        patents = patents.join(self._claim(), how='left')
        patents = patents.join(self._application(), how='left')
        patents = patents.join(self._foreigncitation(), how='left')
        patents['foreigncitation'] = patents['foreigncitation'].replace(
                np.NaN, 0)
        patents = patents.join(self._otherreference(), how='left')
        patents['otherreference'] = patents['otherreference'].replace(np.NaN,
                                                                      0)
        patents = patents.join(self._usapplicationcitation(), how='left')
        patents['applicationcitation'] =\
            patents['applicationcitation'].replace(np.NaN, 0)
        patents.to_pickle(opath)
        return np.array_split(patents, chunks) if chunks else patents

    def _patent(self):
        """Read table patent. All 6,819,362 records in table are valid. Each
        patent node has an "id" and two attributes "type" and "date".

        Returns
        -------
        :class:`pandas.DataFrame`
            Data on granted patents.

        """

        print('Loading patent.tsv')
        ipath = os.path.join(self._ipath, 'patent.tsv.bz2')
        opath = os.path.join(self._ipath, 'patent.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        patent = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                             dtype=str)
        patent.drop(columns=['number', 'country', 'abstract', 'title', 'kind',
                             'num_claims', 'filename', 'withdrawn'],
                    inplace=True)
        patent['date'] = pd.to_datetime(patent['date'], errors='coerce')
        patent.rename(columns={'id': 'pid'}, inplace=True)
        patent.dropna(axis='index', subset=['pid'], how='any', inplace=True)
        patent.set_index('pid', inplace=True, verify_integrity=True)
        patent.to_pickle(opath)
        return patent

    def _application(self):
        """Read table application. All 6,819,362 records in table are valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Information on the applications for granted patent.

        """

        print('Loading application.tsv.')
        ipath = os.path.join(self._ipath, 'application.tsv.bz2')
        opath = os.path.join(self._ipath, 'application.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        application = pd.read_csv(ipath, sep='\t', quoting=3,
                                  lineterminator='\n', dtype=str)
        application.drop(columns=['number', 'country'], inplace=True)
        application.dropna(axis='index', how='any', inplace=True)
        application.rename(columns={'patent_id': 'pid',
                                    'id': 'application_id',
                                    'date': 'application_date'}, inplace=True)
        application.set_index('pid', inplace=True, verify_integrity=True)
        application.to_pickle(opath)
        return application

    def _claim(self):
        """ Read table claim.tsv. Out of 96,694,251 records in table,
        94,128,045 have valid patent_id and dependent fields. Overall,
        6,817,926 patents are found.

        Returns
        -------
        :class:`pandas.DataFrame`
            Patent claims and their dependency.

        """

        print('Loading claim.tsv.')
        ipath = os.path.join(self._ipath, 'claim.tsv.bz2')
        opath = os.path.join(self._ipath, 'claim.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        chunks = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                             dtype=str, chunksize=1000000)
        claim = []
        for chunk in chunks:  # must have dependent and patent_id
            chunk.drop(columns=['uuid', 'text', 'sequence', 'exemplary'],
                       inplace=True)
            chunk.dropna(axis='index', subset=['dependent', 'patent_id'],
                         how='any', inplace=True)
            chunk.loc[chunk.loc[:, 'dependent'] != '-1', 'dependent'] = 'D'
            chunk.loc[chunk.loc[:, 'dependent'] == '-1', 'dependent'] = 'I'
            claim.append(chunk)
        claim = pd.concat(claim).groupby('patent_id').aggregate(
                lambda x: collections.Counter(tuple(x)))
        claim = claim.apply(lambda e: pd.Series([int(e['dependent']['D']),
                                                 int(e['dependent']['I'])],
                            index=['dependent', 'independent']), axis=1)
        claim.index.rename('pid', inplace=True)
        claim.to_pickle(opath)
        return claim

    def _foreigncitation(self):
        """Read table foreigncitation. Out of 25,374,575 records in table,
        25,374,573 are valid. In total, 3,638,518 patents are involved.

        Returns
        -------
        :class:`pandas.DataFrame`
            Citations made to foreign patents by US patents.

        """

        print('Loading foreigncitation.tsv')
        ipath = os.path.join(self._ipath, 'foreigncitation.tsv.bz2')
        opath = os.path.join(self._ipath, 'foreigncitation.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        foreigncitation = pd.read_csv(ipath, sep='\t', quoting=3,
                                      lineterminator='\n', dtype=str)
        foreigncitation.drop(columns=['uuid', 'date', 'country', 'category',
                                      'sequence'], inplace=True)
        foreigncitation.dropna(axis='index', how='any', inplace=True)
        foreigncitation = foreigncitation.groupby('patent_id').aggregate(
                lambda x: len(list(x)))
        foreigncitation.rename(columns={'number': 'foreigncitation'},
                               inplace=True)
        foreigncitation.index.rename('pid', inplace=True)
        foreigncitation.to_pickle(opath)
        return foreigncitation

    def _otherreference(self):
        """Read table otherreference. Out of 36,101,604 records in table,
        36,101,591 are valid. 3,005,105 patents are found.

        Returns
        -------
        :class:`pandas.DataFrame`
            Non-patent citations mentioned in patents (e.g. articles, papers,
            etc.).

        """

        print('Loading otherreference.tsv')
        ipath = os.path.join(self._ipath, 'otherreference.tsv.bz2')
        opath = os.path.join(self._ipath, 'otherreference.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        otherreference = pd.read_csv(ipath, sep='\t', quoting=3, dtype=str,
                                     lineterminator='\n').drop(
                                             columns=['uuid', 'sequence'])
        otherreference.dropna(axis='index', how='any', inplace=True)
        otherreference = otherreference.groupby('patent_id').aggregate(
                lambda x: len(list(x)))
        otherreference.rename(columns={'text': 'otherreference'}, inplace=True)
        otherreference.index.rename('pid', inplace=True)
        otherreference.to_pickle(opath)
        return otherreference

    def _usapplicationcitation(self):
        """Read table usapplicationcitation. All 32,145,240 records in table
        are valid. 2,754,563 patents are found.

        Returns
        -------
        :class:`pandas.DataFrame`
            Citations made to US patent applications by US patents.

        """

        print('Loading usapplicationcitation.tsv')
        ipath = os.path.join(self._ipath, 'usapplicationcitation.tsv.bz2')
        opath = os.path.join(self._ipath, 'usapplicationcitation.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        usapplicationcitation = pd.read_csv(ipath, sep='\t', quoting=3,
                                            lineterminator='\n', dtype=str)
        usapplicationcitation.drop(columns=['uuid', 'date', 'name', 'kind',
                                            'number', 'country', 'category',
                                            'sequence'], inplace=True)
        usapplicationcitation.dropna(axis='index', how='any', inplace=True)
        usapplicationcitation = usapplicationcitation.groupby(
                'patent_id').aggregate(lambda x: len(list(x)))
        usapplicationcitation.rename(columns={
            'application_id': 'applicationcitation'}, inplace=True)
        usapplicationcitation.index.rename('pid', inplace=True)
        usapplicationcitation.to_pickle(opath)
        return usapplicationcitation

    def construct_assignee_nodes(self, chunks=None):
        """Construct assignee nodes.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        assignees = self._assignee()
        return np.array_split(assignees, chunks) if chunks else assignees

    def _assignee(self):
        """Read table assignee. All 506,284 records in table are valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Disambiguated assignee data.

        """

        print('Loading assignee.tsv.')
        ipath = os.path.join(self._ipath, 'assignee.tsv.bz2')
        opath = os.path.join(self._ipath, 'assignee.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        assignee = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                               dtype=str)
        assignee['name_first'] = assignee['name_first'].map(
                lambda x: str(x).strip() if isinstance(x, str) else '')
        assignee['name_last'] = assignee['name_last'].map(
                lambda x: str(x).strip() if isinstance(x, str) else '')
        assignee['organization'] = assignee['organization'].map(
                lambda x: str(x).strip() if isinstance(x, str) else '')
        assignee['assignee_name'] = assignee['name_first'] + ' '\
            + assignee['name_last'] + ' ' + assignee['organization']
        assignee.drop(columns=['name_first', 'name_last', 'organization'],
                      inplace=True)
        assignee.dropna(axis='index', subset=['id'], how='all', inplace=True)
        assignee.rename(columns={'id': 'assignee_id',
                                 'type': 'assignee_type'}, inplace=True)
        assignee.set_index('assignee_id', inplace=True, verify_integrity=True)
        assignee.to_pickle(opath)
        return assignee

    def construct_inventor_nodes(self, chunks=None):
        """Construct inventor nodes.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        inventors = self._inventor()
        return np.array_split(inventors, chunks) if chunks else inventors

    def _inventor(self):
        """Read table inventor. All 3,772,041 records in table are valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Disambiguated inventor data.

        """

        print('Loading inventor.tsv')
        ipath = os.path.join(self._ipath, 'inventor.tsv.bz2')
        opath = os.path.join(self._ipath, 'inventor.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        inventor = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                               dtype=str)
        inventor.dropna(axis='index', how='all', subset=['id'], inplace=True)
        inventor['name_first'] = inventor['name_first'].map(
                lambda x: str(x).strip() if isinstance(x, str) else '')
        inventor['name_last'] = inventor['name_last'].map(
                lambda x: str(x).strip() if isinstance(x, str) else '')
        inventor['inventor_name'] = inventor['name_first'] + ' '\
            + inventor['name_last']
        inventor.drop(columns=['name_first', 'name_last'], inplace=True)
        inventor.rename(columns={'id': 'inventor_id'}, inplace=True)
        inventor.set_index('inventor_id', inplace=True, verify_integrity=True)
        inventor.to_pickle(opath)
        return inventor

    def construct_location_nodes(self, chunks=None):
        """Construct location nodes.

        Returns
        -------
        list
            Dataframe chunks.

        """

        locations = self._location()
        return np.array_split(locations, chunks) if chunks else locations

    def _location(self):
        """Read table location. All 141,189 records in table are valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Disambiguated location data, including latitude and longitude.

        """

        print('Loading location.tsv')
        ipath = os.path.join(self._ipath, 'location.tsv.bz2')
        opath = os.path.join(self._ipath, 'location.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        location = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                               dtype=str)
        location.dropna(axis='index', how='all',
                        subset=['city', 'state', 'country', 'latitude',
                                'longitude', 'county', 'state_fips',
                                'county_fips'], inplace=True)
        location.rename(columns={'id': 'location_id'}, inplace=True)
        location.set_index('location_id', inplace=True, verify_integrity=True)
        location.to_pickle(opath)
        return location

    def construct_patent_citations(self, chunks=None):
        """Construct patent citation edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        citations = self._uspatentcitation()
        return np.array_split(citations, chunks) if chunks else citations

    def _uspatentcitation(self):
        """Read table uspatentcitation. Out of 98,207,057 records in table,
        98,207,034 are valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Citations made to US granted patents by US patents.

        """

        print('Loading uspatentcitation.tsv')
        ipath = os.path.join(self._ipath, 'uspatentcitation.tsv.bz2')
        opath = os.path.join(self._ipath, 'uspatentcitation.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        uspatentcitation = pd.read_csv(ipath, sep='\t', quoting=3,
                                       lineterminator='\n',
                                       usecols=['patent_id', 'citation_id'],
                                       dtype=str)
        uspatentcitation.dropna(axis='index', how='any', inplace=True)
        uspatentcitation.to_pickle(opath)
        return uspatentcitation

    def construct_patent_assignee_edges(self, chunks=None):
        """Construct patent-assignee edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        patent_assignee = self._patent_assignee()
        return np.array_split(patent_assignee, chunks)\
            if chunks else patent_assignee

    def _patent_assignee(self):
        """Read table patent_assignee. All 6,070,101 records in table are
        valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Crosswalk between patent and assignee tables.

        """

        print('Loading patent_assignee.tsv')
        ipath = os.path.join(self._ipath, 'patent_assignee.tsv.bz2')
        opath = os.path.join(self._ipath, 'patent_assignee.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        patent_assignee = pd.read_csv(ipath, sep='\t', quoting=3,
                                      lineterminator='\n', dtype=str)
        patent_assignee.dropna(axis='index', how='any', inplace=True)
        patent_assignee.to_pickle(opath)
        return patent_assignee

    def construct_patent_inventor_edges(self, chunks=None):
        """Construct patent-inventor edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        patent_inventor = self._patent_inventor()
        return np.array_split(patent_inventor, chunks)\
            if chunks else patent_inventor

    def _patent_inventor(self):
        """Read table patent_inventor. All 16,237,888 records in table are
        valid.

        Returns
        -------
        :class:`pandas.DataFrame`
            Crosswalk between patent and inventor tables.

        """

        print('Loading patent_inventor.tsv')
        ipath = os.path.join(self._ipath, 'patent_inventor.tsv.bz2')
        opath = os.path.join(self._ipath, 'patent_inventor.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        patent_inventor = pd.read_csv(ipath, sep='\t', quoting=3,
                                      lineterminator='\n', dtype=str)
        patent_inventor.dropna(axis='index', how='any', inplace=True)
        patent_inventor.to_pickle(opath)
        return patent_inventor

    def construct_assignee_location_edges(self, chunks=None):
        """Construct assignee-location edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        assignee_location = self._location_assignee()
        return np.array_split(assignee_location, chunks)\
            if chunks else assignee_location

    def _location_assignee(self):
        """Read table location_assignee. All 619,055 records in table are
        valid.

        .. note::

           Locations and assignees are many-to-many relationships.

        Returns
        -------
        :class:`pandas.DataFrame`
            Crosswalk between location and assignee tables.

        """

        print('Loading location_assignee.tsv')
        ipath = os.path.join(self._ipath, 'location_assignee.tsv.bz2')
        opath = os.path.join(self._ipath, 'location_assignee.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        location_assignee = pd.read_csv(ipath, sep='\t', quoting=3,
                                        lineterminator='\n', dtype=str)
        location_assignee.dropna(axis='index', how='any', inplace=True)
        location_assignee.to_pickle(opath)
        return location_assignee

    def construct_inventor_location_edges(self, chunks=None):
        """Construct inventor-location edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        inventor_location = self._location_inventor()
        return np.array_split(inventor_location, chunks)\
            if chunks else inventor_location

    def _location_inventor(self):
        """Read table location_inventor. All 16,237,556 records in table are
        valid.

        .. note::

           Locations and inventors are many-to-many relationships.

        Returns
        -------
        :class:`pandas.DataFrame`
            Crosswalk between location and inventor tables.

        """

        print('Loading location_inventor.tsv')
        ipath = os.path.join(self._ipath, 'location_inventor.tsv.bz2')
        opath = os.path.join(self._ipath, 'location_inventor.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        location_inventor = pd.read_csv(ipath, sep='\t', quoting=3,
                                        lineterminator='\n', dtype=str)
        location_inventor.dropna(axis='index', how='any', inplace=True)
        location_inventor.to_pickle(opath)
        return location_inventor

    def construct_cpc_nodes(self, chunks=None):
        """Construct patent citation edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        cpc = self._cpc_current()
        cpc_section = set([str(e) for e in cpc.loc[:, 'cpc_section'].tolist()])
        cpc_subsection = set([str(e)
                              for e in cpc.loc[:, 'cpc_subsection'].tolist()])
        cpc_group = set([str(e) for e in cpc.loc[:, 'cpc_group'].tolist()])
        cpc_subgroup = set([str(e)
                            for e in cpc.loc[:, 'cpc_subgroup'].tolist()])
        nodes = (cpc_section, cpc_subsection, cpc_group, cpc_subgroup)
        return nodes, np.array_split(cpc, chunks) if chunks else cpc

    def _cpc_current(self):
        """Read table cpc_current. All 36,846,878 records in table are valid.

        Returns
        ----------
        :class:`pandas.DataFrame`
            Current CPC classification of the patent.

        """

        print('Loading cpc_current.tsv')
        ipath = os.path.join(self._ipath, 'cpc_current.tsv.bz2')
        opath = os.path.join(self._ipath, 'cpc_current.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        cpc = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                          dtype=str)
        cpc.drop(columns=['uuid', 'category', 'sequence'], inplace=True)
        cpc.dropna(axis='index', how='any', inplace=True)
        cpc.rename(columns={'section_id': 'cpc_section',
                            'group_id': 'cpc_group',
                            'subsection_id': 'cpc_subsection',
                            'subgroup_id': 'cpc_subgroup'}, inplace=True)
        cpc.to_pickle(opath)
        return cpc

    def construct_uspc_nodes(self, chunks=None):
        """Construct patent citation edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        uspc = self._uspc_current()
        uspc_mainclass = set(uspc.loc[:, 'uspc_mainclass'].tolist())
        uspc_subclass = set(uspc.loc[:, 'uspc_subclass'].tolist())
        nodes = (uspc_mainclass, uspc_subclass)
        return nodes, np.array_split(uspc, chunks) if chunks else uspc

    def _uspc_current(self):
        """Read table uspc_current. Out of 22,885,509 records in table,
        21,982,779 are valid.

        .. note::

           Current USPC classification data for all patents up to **May 2015**.

        Returns
        -------
        :class:`pandas.DataFrame`
            Current USPC classification data for all patents up to May 2015.

        """

        print('Loading uspc_current.tsv')
        ipath = os.path.join(self._ipath, 'uspc_current.tsv.bz2')
        opath = os.path.join(self._ipath, 'uspc_current.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        uspc_current = pd.read_csv(ipath, sep='\t', quoting=3,
                                   lineterminator='\n', dtype=str)
        uspc_current.drop(columns=['uuid', 'sequence'], inplace=True)
        uspc_current.replace('No longer published', np.NaN, inplace=True)
        uspc_current.dropna(axis='index', how='any', inplace=True)
        uspc_current.rename(columns={'mainclass_id': 'uspc_mainclass',
                                     'subclass_id': 'uspc_subclass'},
                            inplace=True)
        uspc_current.to_pickle(opath)
        return uspc_current

    def construct_ipcr_nodes(self, chunks=None):
        """Construct patent citation edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        ipcr = self._ipcr()
        ipcr_section = set(ipcr.loc[:, 'ipcr_section'].tolist())
        ipcr_class = set(ipcr.loc[:, 'ipcr_class'].tolist())
        ipcr_subclass = set(ipcr.loc[:, 'ipcr_subclass'].tolist())
        ipcr_group = set(ipcr.loc[:, 'ipcr_group'].tolist())
        ipcr_subgroup = set(ipcr.loc[:, 'ipcr_subgroup'].tolist())
        nodes = (ipcr_section, ipcr_class, ipcr_subclass, ipcr_group,
                 ipcr_subgroup)
        return nodes, np.array_split(ipcr, chunks) if chunks else ipcr

    def _ipcr(self):
        """Read table ipcr. Out of 13,854,255 records in table, 13,685,911 are
        valid.

        .. todo::

           May use ``all`` to ``dropna``, because most missing data has missing
           value in main_group and subgroup fields.

        Returns
        -------
        :class:`pandas.DataFrame`
            International Patent Classification data for all patents (as of
            publication date).

        """

        print('Loading ipcr.tsv')
        ipath = os.path.join(self._ipath, 'ipcr.tsv.bz2')
        opath = os.path.join(self._ipath, 'ipcr.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        ipcr = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                           dtype=str)
        ipcr.drop(columns=['uuid', 'classification_level', 'symbol_position',
                           'classification_value', 'classification_status',
                           'classification_data_source', 'action_date',
                           'ipc_version_indicator', 'sequence'], inplace=True)
        ipcr.dropna(axis='index', subset=['patent_id', 'section', 'ipc_class',
                                          'subclass', 'main_group',
                                          'subgroup'], how='any', inplace=True)
        ipcr.rename(columns={'section': 'ipcr_section',
                             'ipc_class': 'ipcr_class',
                             'subclass': 'ipcr_subclass',
                             'main_group': 'ipcr_group',
                             'subgroup': 'ipcr_subgroup'}, inplace=True)
        ipcr.to_pickle(opath)
        return ipcr

    def construct_nber_nodes(self, chunks=None):
        """Construct patent citation edges.

        Parameters
        ----------
        chunks : int
            Number of chunks expected.

        Returns
        -------
        list
            Dataframe chunks.

        """

        nber = self._nber()
        nber_cat = set(nber.loc[:, 'nber_category'].tolist())
        nber_subcat = set(nber.loc[:, 'nber_subcategory'].tolist())
        nodes = (nber_cat, nber_subcat)
        return nodes, np.array_split(nber, chunks) if chunks else nber

    def _nber(self):
        """Read table nber. All 5,105,937 records in table are valid.

        .. note::

           NBER classification data for patents up to **May 2015**.

        Returns
        -------
        :class:`pandas.DataFrame`
            NBER classification data for all patents up to May 2015.

        """

        print('Loading nber.tsv')
        ipath = os.path.join(self._ipath, 'nber.tsv.bz2')
        opath = os.path.join(self._ipath, 'nber.pkl.bz2')
        if os.path.exists(opath):
            return pd.read_pickle(opath)
        nber = pd.read_csv(ipath, sep='\t', quoting=3, lineterminator='\n',
                           dtype=str).drop(columns=['uuid'])
        nber.dropna(axis='index', inplace=True)
        nber.rename(columns={'category_id': 'nber_category',
                             'subcategory_id': 'nber_subcategory'},
                    inplace=True)
        nber.to_pickle(opath)
        return nber
