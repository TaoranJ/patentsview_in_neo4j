# -*- coding: utf-8 -*-

"""Load patent dataset into neo4j database.

Patent-[:CITE]->Patent
Inventor-[:INVENT]->Patent
Asignee-[:OWN]->Patent
Asignee-[:LOCATE_AT]->Location
Inventor-[:LOCATE_AT]->Location
Patent-[:CATEGORIZE_TO]->CPC_Section
Patent-[:CATEGORIZE_TO]->CPC_Subsection
Patent-[:CATEGORIZE_TO]->CPC_Group
Patent-[:CATEGORIZE_TO]->CPC_Subgroup
Patent-[:CATEGORIZE_TO]->IPC_Section
Patent-[:CATEGORIZE_TO]->IPC_Class
Patent-[:CATEGORIZE_TO]->IPC_Subclass
Patent-[:CATEGORIZE_TO]->IPC_Main_Group
Patent-[:CATEGORIZE_TO]->IPC_Subgroup
Patent-[:CATEGORIZE_TO]->NBER_Category
Patent-[:CATEGORIZE_TO]->NBER_Subcategory
Patent-[:CATEGORIZE_TO]->UPC_Category
Patent-[:CATEGORIZE_TO]->UPC_Subclass

"""

import argparse

from techflow.neo4j_handler import Neo4jHandler

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('credential', help='Auth file')
    pparser.add_argument('patent_dir', help='Dir to raw patent data')
    args = pparser.parse_args()
    handler = Neo4jHandler(args.credential, args.patent_dir)
    handler.create_patent_nodes()
    handler.create_citation_relationships()
    handler.create_assignee_nodes()
    handler.create_patent_assignee_relationships()
    handler.create_inventor_nodes()
    handler.create_patent_inventor_relationships()
    handler.create_location_nodes()
    handler.create_assignee_location_relationships()
    handler.create_inventor_location_relationships()
