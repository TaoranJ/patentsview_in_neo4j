# neo4j_for_patentsview
Scripts of handling PatentsView in Neo4j database

## Installation

```bash
pip install pandas numpy neo4j
```

## Database scheme

Nodes:
- `patent`, `assignee`, `inventor`, `location`
- `cpc_section`, `cpc_subsection`, `cpc_group`, `cpc_subgroup`
- `ipcr_section`, `ipcr_class`, `ipcr_subclass`, `ipcr_maingroup`, `ipcr_subgroup`
- `nber_category`, `nber_subcategory`
- `uspc_mainclass`, `uspc_subclass`, `location`

Edges:
- `CITES`
- `OWNS`
- `INVENTS`
- `BELONGS_TO`
- `LOCATES_AT`
