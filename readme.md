# Tolkien Gateway Project

A semantic web application that builds a Knowledge Graph from the Tolkien Gateway wiki, following Linked Data principles and integrating with external knowledge bases.

## Project Overview

This project extracts data from [Tolkien Gateway](https://tolkiengateway.net/) and transforms it into a structured RDF Knowledge Graph with:

- ~ 27,199 RDF triples describing Tolkien's legendarium
- ~ 1,728 wiki pages converted to entities
- ~ 1,203 character entities with detailed information
- Schema.org vocabulary for semantic interoperability
- SHACL validation ensuring data quality
- Multilingual labels (English, French, German, Spanish, Italian)
- External alignments to DBpedia (26 entities)
- SPARQL endpoint for querying with reasoning
- Linked Data interface with content negotiation (HTML/Turtle)

## Quick Start

### Prerequisites

```bash
# Create and activate virtual environment
python -m venv venv

# Linux OS
source venv/bin/activate 

# Window OS
venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

## Project Structure

```
.
├── src/extract/              # Web scraping scripts
│   ├── scrape_characters_categories.py
│   ├── scrape_all_characters.py
│   └── scrape_characters_json.py
├── parsers/                  # Data processing pipeline
│   ├── Step1_parse_all_pages.py
│   ├── Step2_rdf_generator.py
│   ├── Step3_shacl_generator.py
│   ├── Step4_enrich_with_metw_and_csv.py
│   ├── Step5_add_multilingual_labels.py
│   ├── Step6_align_external_kgs.py
│   └── Step7_sparql_queries.py
├── web/                      # Linked Data Interface
│   ├── app.py               # Flask application
│   └── templates/
│       ├── index.html       # Home page
│       ├── entity.html      # Entity display
│       └── 404.html         # Error page
├── data/
│   ├── raw/                  # Scraped data
│   │   ├── characters_categories.txt
│   │   ├── all_characters.txt
│   │   └── json_pages.json
│   ├── normalized/           # Parsed structured data
│   │   ├── normalized_entities.json
│   │   ├── parsing_statistics.json
│   │   └── entity_index.json
│   ├── rdf/                  # RDF Knowledge Graph
│   │   ├── tolkien_kg.ttl
│   │   ├── tolkien_kg_enriched.ttl
│   │   ├── tolkien_kg_multilingual.ttl
│   │   └── tolkien_kg_aligned.ttl (FINAL)
│   ├── shacl/                # Validation shapes
│   │   ├── tolkien_shapes.ttl
│   │   └── validation_report.ttl
│   └── external/             # External datasets
│       ├── cards.json
│       └── lotr_characters.csv
└── requirements.txt
```

## Execution Pipeline

##### Setup Apache Fuseki

```bash
# Download and extract
wget https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-5.2.0.tar.gz
tar -xzf apache-jena-fuseki-5.2.0.tar.gz
cd apache-jena-fuseki-5.2.0

# Start server

# Linux OS
./fuseki-server

# window OS
.\fuseki-server.bat --update --mem /tolkien

```

### Phase 1: Data Acquisition

Extract character data from Tolkien Gateway wiki:

```bash
python src/extract/scrape_characters_categories.py
python src/extract/scrape_all_characters.py
python src/extract/scrape_characters_json.py
```

Outputs: `data/raw/json_pages.json` with 1,728 pages

### Phase 2: Parsing & Normalization

Parse wikitext infoboxes and extract structured data:

```bash
python parsers/Step1_parse_all_pages.py
```

Outputs: `data/normalized/normalized_entities.json`

### Phase 3: RDF Generation

Convert structured data to RDF triples using schema.org:

```bash
python parsers/Step2_rdf_generator.py
```

Outputs: `data/rdf/tolkien_kg.ttl` (27,149 triples)

<!-- ##### Example output -->

```turtle
tgr:Gandalf a schema:Person ;
    schema:name "Gandalf"@en ;
    schema:gender "Male" ;
    schema:affiliation tgr:White_Council ;
    tgprop:race tgr:Maiar .
```

### Phase 4: SHACL Validation

Validate RDF against constraints:

```bash
python parsers/Step3_shacl_generator.py
```

Outputs: `data/shacl/tolkien_shapes.ttl` and validation report

### Phase 5: Enrichment

Add multilingual labels and external alignments:

```bash
python parsers/Step4_enrich_with_metw_and_csv.py
python parsers/Step5_add_multilingual_labels.py
python parsers/Step6_align_external_kgs.py
```

Final output: `data/rdf/tolkien_kg_aligned.ttl` (~27,194 triples)

### Phase 6: Triplestore & SPARQL

##### Setup Apache Fuseki

```bash
# Download and extract
wget https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-5.2.0.tar.gz
tar -xzf apache-jena-fuseki-5.2.0.tar.gz
cd apache-jena-fuseki-5.2.0

# Start server

# Linux OS
./fuseki-server

# window OS
.\fuseki-server.bat --update --mem /tolkien

```

##### Create Dataset & Load Data

1. Open Fuseki Web UI: <http://localhost:3030>
2. Create dataset:
   - Click "Manage datasets" → "Add new dataset"
   - Name: `tolkienkg`
   - Type: Persistent (TDB2)
3. Upload data:
   - Go to "tolkienkg" → "upload files"
   - Select: `data/rdf/tolkien_kg_aligned.ttl`
   - Click "upload all"

##### Verify Data Loaded

Query in Fuseki UI (<http://localhost:3030>):

```sparql
SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o . }
```

Expected: ~27,199 triples

##### Run SPARQL queries

```bash
python parsers/Step7_sparql_queries.py
```

### Phase 7: Linked Data Interface

##### Start Flask application

```bash
python web/app.py
```

Server runs on: <http://localhost:5000>

##### Features

- Content negotiation (HTML for browsers, Turtle for machines)
- Rich HTML display with entity details and relationships
- SPARQL-powered real-time queries
- External links to DBpedia

##### Example entities

- <http://localhost:5000/resource/Gandalf>
- <http://localhost:5000/resource/Frodo_Baggins>
- <http://localhost:5000/resource/Aragorn>

## Example SPARQL Queries

##### Find all children of Elrond

```sparql
PREFIX schema: <http://schema.org/>

SELECT ?child ?name WHERE {
  ?elrond schema:name "Elrond"@en ;
          schema:children ?child .
  ?child schema:name ?name .
}
```

##### Characters aligned with DBpedia

```sparql
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX schema: <http://schema.org/>

SELECT ?name ?dbpedia WHERE {
  ?character schema:name ?name ;
            owl:sameAs ?dbpedia .
  FILTER(STRSTARTS(STR(?dbpedia), "http://dbpedia.org/"))
}
```

## Final Statistics

| Metric | Count |
|--------|-------|
| Total triples | ~ 27,199 |
| Unique entities | ~ 3,458 |
| Person entities | ~ 1,203 |
| Wiki pages | ~ 1,728 |
| Multilingual labels | ~ 20 |
| DBpedia alignments | ~ 26 |
| Languages supported | 5 (en, fr, de, es, it) |

## Key Features

- Schema.org vocabulary for interoperability
- SHACL validation for data quality
- Multilingual support
- External alignments to DBpedia
- Rich relationship modeling
- Linked Data interface with content negotiation

## Technologies Used

- Python 3.8+
- RDFLib
- mwparserfromhell
- Apache Jena Fuseki
- SHACL
- Schema.org
- SPARQLWrapper
- Flask

## Troubleshooting

**Fuseki won't start:**

```bash
lsof -i :3030  # Check if port is in use
./fuseki-server --port=3031  # Use different port
```

**Flask connection errors:**

```bash
curl http://localhost:3030/$/ping  # Verify Fuseki is running
curl http://localhost:3030/$/datasets  # Check dataset exists
```

## License

Educational project for Semantic Web course.
Data sourced from [Tolkien Gateway](https://tolkiengateway.net/) (CC BY-SA 3.0).

## Useful Links

- [Tolkien Gateway](https://tolkiengateway.net/)
- [Schema.org Specification](https://schema.org/)
- [Apache Jena Fuseki](https://jena.apache.org/documentation/fuseki2/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)
- [SHACL Specification](https://www.w3.org/TR/shacl/)
- [DBpedia](https://www.dbpedia.org/)