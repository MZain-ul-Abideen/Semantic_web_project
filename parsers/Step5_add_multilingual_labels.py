#!/usr/bin/env python3
"""
Multilingual Labels Enrichment
Add labels in multiple languages from external wikis
Uses Wikidata API to fetch multilingual labels
"""

import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Set
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS
from urllib.parse import quote


class MultilingualEnricher:
    """Add multilingual labels to entities"""
    
    def __init__(self, kg_file: Path):
        self.base_uri = "http://tolkiengateway.semanticweb.org/"
        
        # Define namespaces
        self.NS = Namespace(self.base_uri)
        self.RESOURCE = Namespace(f"{self.base_uri}resource/")
        self.SCHEMA = Namespace("http://schema.org/")
        
        # Load existing KG
        print(f"Loading Knowledge Graph from {kg_file}...")
        self.graph = Graph()
        self.graph.parse(str(kg_file), format='turtle')
        print(f"Loaded {len(self.graph)} existing triples")
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("schema", self.SCHEMA)
        
        # Track statistics
        self.stats = {
            'entities_processed': 0,
            'labels_added': 0,
            'wikidata_matches': 0,
            'failed_lookups': 0
        }
    
    def get_entity_names(self) -> List[tuple]:
        """Get all entities and their English names from KG"""
        query = """
        PREFIX schema: <http://schema.org/>
        
        SELECT DISTINCT ?entity ?name WHERE {
            ?entity schema:name ?name .
            FILTER(lang(?name) = "en" || lang(?name) = "")
        }
        LIMIT 100
        """
        
        results = self.graph.query(query)
        entities = [(str(row.entity), str(row.name)) for row in results]
        print(f"Found {len(entities)} entities to enrich")
        return entities
    
    def search_wikidata(self, entity_name: str) -> Optional[str]:
        """
        Search Wikidata for an entity and return its QID
        """
        try:
            # Search Wikidata
            search_url = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbsearchentities',
                'format': 'json',
                'language': 'en',
                'type': 'item',
                'search': entity_name,
                'limit': 1
            }
            
            response = requests.get(search_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('search'):
                # Get first result
                result = data['search'][0]
                return result.get('id')
            
            return None
            
        except Exception as e:
            print(f"  Error searching Wikidata for '{entity_name}': {e}")
            return None
    
    def get_wikidata_labels(self, qid: str) -> Dict[str, str]:
        """
        Fetch labels in multiple languages from Wikidata
        """
        try:
            url = "https://www.wikidata.org/w/api.php"
            params = {
                'action': 'wbgetentities',
                'format': 'json',
                'ids': qid,
                'props': 'labels',
                'languages': 'fr|de|es|it|pt|nl|pl|ru|ja|zh'  # Multiple languages
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'entities' in data and qid in data['entities']:
                entity = data['entities'][qid]
                labels = entity.get('labels', {})
                
                # Extract labels
                result = {}
                for lang_code, label_data in labels.items():
                    result[lang_code] = label_data.get('value', '')
                
                return result
            
            return {}
            
        except Exception as e:
            print(f"  Error fetching labels for {qid}: {e}")
            return {}
    
    def add_labels_from_static_data(self):
        """
        Add some common Tolkien character labels manually
        (fallback when Wikidata is not available)
        """
        # Common translations for major characters
        translations = {
            'Gandalf': {
                'fr': 'Gandalf',
                'de': 'Gandalf',
                'es': 'Gandalf',
                'it': 'Gandalf'
            },
            'Frodo': {
                'fr': 'Frodon',
                'de': 'Frodo',
                'es': 'Frodo',
                'it': 'Frodo'
            },
            'Aragorn': {
                'fr': 'Aragorn',
                'de': 'Aragorn',
                'es': 'Aragorn',
                'it': 'Aragorn'
            },
            'Elrond': {
                'fr': 'Elrond',
                'de': 'Elrond',
                'es': 'Elrond',
                'it': 'Elrond'
            },
            'Galadriel': {
                'fr': 'Galadriel',
                'de': 'Galadriel',
                'es': 'Galadriel',
                'it': 'Galadriel'
            },
            'Sauron': {
                'fr': 'Sauron',
                'de': 'Sauron',
                'es': 'Sauron',
                'it': 'Sauron'
            }
        }
        
        labels_added = 0
        
        for entity_name, langs in translations.items():
            # Find entity URI
            entity_uri = self.RESOURCE[entity_name.replace(' ', '_')]
            
            # Check if entity exists
            if (entity_uri, None, None) in self.graph:
                for lang, label in langs.items():
                    self.graph.add((entity_uri, RDFS.label, Literal(label, lang=lang)))
                    labels_added += 1
        
        print(f"Added {labels_added} static multilingual labels")
        return labels_added
    
    def enrich_with_multilingual_labels(self, use_wikidata: bool = False, limit: int = 50):
        """
        Add multilingual labels to entities
        
        Args:
            use_wikidata: Whether to query Wikidata API (slow)
            limit: Maximum entities to process with Wikidata
        """
        print("\nEnriching with multilingual labels...")
        
        # First, add static labels for major characters
        self.add_labels_from_static_data()
        
        if not use_wikidata:
            print("\nSkipping Wikidata lookup (use_wikidata=False)")
            print("To enable Wikidata lookup, set use_wikidata=True")
            return
        
        # Get entities
        entities = self.get_entity_names()[:limit]
        
        print(f"\nQuerying Wikidata for {len(entities)} entities...")
        print("(This may take a while...)")
        
        for i, (entity_uri, entity_name) in enumerate(entities):
            if (i + 1) % 10 == 0:
                print(f"  Processing {i + 1}/{len(entities)}...")
            
            self.stats['entities_processed'] += 1
            
            # Search Wikidata
            qid = self.search_wikidata(entity_name)
            
            if qid:
                self.stats['wikidata_matches'] += 1
                
                # Get multilingual labels
                labels = self.get_wikidata_labels(qid)
                
                # Add to graph
                for lang, label in labels.items():
                    if label:
                        self.graph.add((URIRef(entity_uri), RDFS.label, 
                                      Literal(label, lang=lang)))
                        self.stats['labels_added'] += 1
                
                # Be nice to Wikidata API
                time.sleep(0.5)
            else:
                self.stats['failed_lookups'] += 1
        
        print("\nMultilingual enrichment complete")
        self.print_statistics()
    
    def print_statistics(self):
        """Print enrichment statistics"""
        print("\n" + "="*60)
        print("MULTILINGUAL ENRICHMENT STATISTICS")
        print("="*60)
        print(f"Entities processed: {self.stats['entities_processed']}")
        print(f"Wikidata matches: {self.stats['wikidata_matches']}")
        print(f"Labels added: {self.stats['labels_added']}")
        print(f"Failed lookups: {self.stats['failed_lookups']}")
    
    def save_enriched_kg(self, output_file: Path):
        """Save enriched KG"""
        print(f"\nSaving enriched KG to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.graph.serialize(destination=str(output_file), format='turtle')
        print(f"Saved {len(self.graph)} triples")


def main():
    """Main execution"""
    # File paths
    kg_file = Path('./data/rdf/tolkien_kg_enriched.ttl')
    output_file = Path('./data/rdf/tolkien_kg_multilingual.ttl')
    
    # Check file exists
    if not kg_file.exists():
        print(f"ERROR: KG file not found: {kg_file}")
        print("Run enrich_with_metw.py first")
        return
    
    print("="*60)
    print("MULTILINGUAL LABELS ENRICHMENT")
    print("="*60)
    
    # Enrich
    enricher = MultilingualEnricher(kg_file)
    
    # Add multilingual labels
    # Set use_wikidata=True to query Wikidata (slow but comprehensive)
    # Set use_wikidata=False for faster execution with static data only
    enricher.enrich_with_multilingual_labels(
        use_wikidata=False,  # Change to True to use Wikidata
        limit=50
    )
    
    # Save
    enricher.save_enriched_kg(output_file)
    
    print("\n" + "="*60)
    print("ENRICHMENT COMPLETE")
    print("="*60)
    print(f"Output: {output_file}")


if __name__ == '__main__':
    main()