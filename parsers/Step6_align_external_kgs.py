#!/usr/bin/env python3
"""
External Knowledge Graph Alignment
Create owl:sameAs links to DBpedia and YAGO
"""

import requests
import time
from pathlib import Path
from typing import Dict, List, Optional, Set
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, OWL
from urllib.parse import quote
from SPARQLWrapper import SPARQLWrapper, JSON


class ExternalKGAligner:
    """Align entities with DBpedia and YAGO"""
    
    def __init__(self, kg_file: Path):
        self.base_uri = "http://tolkiengateway.semanticweb.org/"
        
        # Define namespaces
        self.NS = Namespace(self.base_uri)
        self.RESOURCE = Namespace(f"{self.base_uri}resource/")
        self.SCHEMA = Namespace("http://schema.org/")
        self.DBPEDIA = Namespace("http://dbpedia.org/resource/")
        self.YAGO = Namespace("http://yago-knowledge.org/resource/")
        
        # Load existing KG
        print(f"Loading Knowledge Graph from {kg_file}...")
        self.graph = Graph()
        self.graph.parse(str(kg_file), format='turtle')
        print(f"Loaded {len(self.graph)} existing triples")
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("owl", OWL)
        self.graph.bind("dbr", self.DBPEDIA)
        self.graph.bind("yago", self.YAGO)
        
        # Statistics
        self.stats = {
            'entities_processed': 0,
            'dbpedia_matches': 0,
            'yago_matches': 0,
            'total_alignments': 0
        }
    
    def get_entity_names(self, limit: int = 100) -> List[tuple]:
        """Get entities and their English names"""
        query = """
        PREFIX schema: <http://schema.org/>
        
        SELECT DISTINCT ?entity ?name WHERE {
            ?entity a schema:Person ;
                    schema:name ?name .
            FILTER(lang(?name) = "en" || lang(?name) = "")
        }
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        results = self.graph.query(query)
        entities = [(str(row.entity), str(row.name)) for row in results]
        print(f"Found {len(entities)} Person entities to align")
        return entities
    
    def search_dbpedia(self, entity_name: str) -> Optional[str]:
        """
        Search DBpedia for an entity
        Returns DBpedia URI if found
        """
        try:
            # Try direct URI construction (works for many cases)
            dbpedia_name = entity_name.replace(' ', '_')
            dbpedia_uri = f"http://dbpedia.org/resource/{quote(dbpedia_name)}"
            
            # Check if resource exists
            response = requests.head(dbpedia_uri, timeout=5, allow_redirects=True)
            
            if response.status_code == 200:
                return dbpedia_uri
            
            return None
            
        except Exception as e:
            # Silently fail - many entities won't be in DBpedia
            return None
    
    def search_dbpedia_sparql(self, entity_name: str) -> Optional[str]:
        """
        Search DBpedia using SPARQL
        More accurate but slower
        """
        try:
            sparql = SPARQLWrapper("https://dbpedia.org/sparql")
            
            query = f"""
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dbo: <http://dbpedia.org/ontology/>
            
            SELECT DISTINCT ?entity WHERE {{
                ?entity rdfs:label "{entity_name}"@en .
                ?entity a ?type .
            }}
            LIMIT 1
            """
            
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            
            results = sparql.query().convert()
            
            if results['results']['bindings']:
                return results['results']['bindings'][0]['entity']['value']
            
            return None
            
        except Exception as e:
            return None
    
    def create_static_alignments(self) -> int:
        """
        Create alignments for well-known entities (fast, reliable)
        """
        # Major Tolkien characters that definitely exist in DBpedia
        known_alignments = {
            'Gandalf': 'http://dbpedia.org/resource/Gandalf',
            'Frodo Baggins': 'http://dbpedia.org/resource/Frodo_Baggins',
            'Aragorn': 'http://dbpedia.org/resource/Aragorn',
            'Bilbo Baggins': 'http://dbpedia.org/resource/Bilbo_Baggins',
            'Elrond': 'http://dbpedia.org/resource/Elrond',
            'Galadriel': 'http://dbpedia.org/resource/Galadriel',
            'Sauron': 'http://dbpedia.org/resource/Sauron',
            'Saruman': 'http://dbpedia.org/resource/Saruman',
            'Legolas': 'http://dbpedia.org/resource/Legolas',
            'Gimli': 'http://dbpedia.org/resource/Gimli_(Middle-earth)',
            'Boromir': 'http://dbpedia.org/resource/Boromir',
            'Samwise Gamgee': 'http://dbpedia.org/resource/Samwise_Gamgee',
            'Peregrin Took': 'http://dbpedia.org/resource/Peregrin_Took',
            'Meriadoc Brandybuck': 'http://dbpedia.org/resource/Meriadoc_Brandybuck',
            'Gollum': 'http://dbpedia.org/resource/Gollum',
            'Arwen': 'http://dbpedia.org/resource/Arwen',
            'Éowyn': 'http://dbpedia.org/resource/Éowyn',
            'Théoden': 'http://dbpedia.org/resource/Théoden',
            'Faramir': 'http://dbpedia.org/resource/Faramir',
            'Denethor': 'http://dbpedia.org/resource/Denethor',
            'Glorfindel': 'http://dbpedia.org/resource/Glorfindel',
            'Celeborn': 'http://dbpedia.org/resource/Celeborn',
            'Gil-galad': 'http://dbpedia.org/resource/Gil-galad',
            'Elendil': 'http://dbpedia.org/resource/Elendil',
            'Isildur': 'http://dbpedia.org/resource/Isildur',
            'Beren': 'http://dbpedia.org/resource/Beren',
            'Lúthien': 'http://dbpedia.org/resource/Lúthien',
            'Túrin Turambar': 'http://dbpedia.org/resource/Túrin_Turambar',
            'Morgoth': 'http://dbpedia.org/resource/Morgoth',
            'Fëanor': 'http://dbpedia.org/resource/Fëanor',
        }
        
        alignments_added = 0
        
        print("\nAdding static DBpedia alignments...")
        
        for entity_name, dbpedia_uri in known_alignments.items():
            # Find entity in our KG
            local_uri = self.RESOURCE[entity_name.replace(' ', '_')]
            
            # Check if entity exists in our KG
            if (local_uri, RDF.type, None) in self.graph:
                # Add owl:sameAs triple
                self.graph.add((local_uri, OWL.sameAs, URIRef(dbpedia_uri)))
                alignments_added += 1
                self.stats['dbpedia_matches'] += 1
        
        print(f"Added {alignments_added} static DBpedia alignments")
        return alignments_added
    
    def align_with_dbpedia(self, use_api: bool = False, limit: int = 50):
        """
        Align entities with DBpedia
        
        Args:
            use_api: Whether to use DBpedia API (slow)
            limit: Maximum entities to process
        """
        print("\nAligning with DBpedia...")
        
        # Always add static alignments first
        static_count = self.create_static_alignments()
        self.stats['total_alignments'] += static_count
        
        if not use_api:
            print("\nSkipping DBpedia API lookup (use_api=False)")
            print("To enable API lookup, set use_api=True")
            return
        
        # Get entities not yet aligned
        entities = self.get_entity_names(limit)
        
        print(f"\nSearching DBpedia for {min(limit, len(entities))} entities...")
        
        for i, (entity_uri, entity_name) in enumerate(entities[:limit]):
            if (i + 1) % 10 == 0:
                print(f"  Processed {i + 1}...")
            
            self.stats['entities_processed'] += 1
            
            # Skip if already aligned
            if (URIRef(entity_uri), OWL.sameAs, None) in self.graph:
                continue
            
            # Search DBpedia
            dbpedia_uri = self.search_dbpedia(entity_name)
            
            if dbpedia_uri:
                self.graph.add((URIRef(entity_uri), OWL.sameAs, URIRef(dbpedia_uri)))
                self.stats['dbpedia_matches'] += 1
                self.stats['total_alignments'] += 1
            
            # Be nice to DBpedia
            time.sleep(0.2)
    
    def print_statistics(self):
        """Print alignment statistics"""
        print("\n" + "="*60)
        print("ALIGNMENT STATISTICS")
        print("="*60)
        print(f"Entities processed: {self.stats['entities_processed']}")
        print(f"DBpedia alignments: {self.stats['dbpedia_matches']}")
        print(f"YAGO alignments: {self.stats['yago_matches']}")
        print(f"Total alignments: {self.stats['total_alignments']}")
    
    def save_enriched_kg(self, output_file: Path):
        """Save aligned KG"""
        print(f"\nSaving aligned KG to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.graph.serialize(destination=str(output_file), format='turtle')
        print(f"Saved {len(self.graph)} triples")


def main():
    """Main execution"""
    # File paths
    kg_file = Path('./data/rdf/tolkien_kg_multilingual.ttl')
    output_file = Path('./data/rdf/tolkien_kg_aligned.ttl')
    
    # Check file exists
    if not kg_file.exists():
        print(f"ERROR: KG file not found: {kg_file}")
        print("Run add_multilingual_labels.py first")
        return
    
    print("="*60)
    print("EXTERNAL KG ALIGNMENT")
    print("="*60)
    
    # Align
    aligner = ExternalKGAligner(kg_file)
    
    # Align with DBpedia
    # Set use_api=True to search more entities (slow)
    aligner.align_with_dbpedia(use_api=False, limit=50)
    
    # Print stats
    aligner.print_statistics()
    
    # Save
    aligner.save_enriched_kg(output_file)
    
    print("\n" + "="*60)
    print("ALIGNMENT COMPLETE")
    print("="*60)
    print(f"Output: {output_file}")


if __name__ == '__main__':
    main()