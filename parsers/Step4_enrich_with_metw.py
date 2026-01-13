#!/usr/bin/env python3
"""
METW Cards Enrichment
Integrate Middle Earth: The Wizards card game data into the Knowledge Graph
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, XSD
from difflib import SequenceMatcher


class METWEnricher:
    """Enrich KG with METW card data"""
    
    def __init__(self, kg_file: Path):
        self.base_uri = "http://tolkiengateway.semanticweb.org/"
        
        # Define namespaces
        self.NS = Namespace(self.base_uri)
        self.RESOURCE = Namespace(f"{self.base_uri}resource/")
        self.SCHEMA = Namespace("http://schema.org/")
        self.METW = Namespace("http://metw.org/card/")
        
        # Load existing KG
        print(f"Loading Knowledge Graph from {kg_file}...")
        self.graph = Graph()
        self.graph.parse(str(kg_file), format='turtle')
        print(f"Loaded {len(self.graph)} existing triples")
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("metw", self.METW)
        
        # Build entity index for matching
        self.entity_index = self._build_entity_index()
    
    def _build_entity_index(self) -> Dict[str, URIRef]:
        """Build index of entity names to URIs"""
        index = {}
        
        # Query for all entities with names
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?entity ?name WHERE {
            ?entity schema:name ?name .
        }
        """
        
        results = self.graph.query(query)
        for row in results:
            entity_uri = row.entity
            name = str(row.name).lower()
            # Store both normalized and original
            index[name] = entity_uri
            # Also store without spaces for fuzzy matching
            index[name.replace(' ', '')] = entity_uri
        
        print(f"Built entity index with {len(index)} entries")
        return index
    
    def similarity_score(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def find_best_match(self, card_name: str, threshold: float = 0.85) -> Optional[URIRef]:
        """Find best matching entity for a card name"""
        card_name_lower = card_name.lower()
        
        # Exact match
        if card_name_lower in self.entity_index:
            return self.entity_index[card_name_lower]
        
        # Fuzzy match
        best_score = 0.0
        best_match = None
        
        for entity_name, entity_uri in self.entity_index.items():
            score = self.similarity_score(card_name, entity_name)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = entity_uri
        
        return best_match
    
    def enrich_with_cards(self, cards_file: Path) -> int:
        """
        Enrich KG with METW card data
        
        Returns: number of cards linked
        """
        print(f"\nLoading METW cards from {cards_file}...")
        
        if not cards_file.exists():
            print(f"ERROR: Cards file not found: {cards_file}")
            print("Please download cards.json from the project resources")
            return 0
        
        with open(cards_file, 'r', encoding='utf-8') as f:
            cards_data = json.load(f)
        
        # NEW: Handle nested structure like {"AS": {"cards": {...}}, "DM": {"cards": {...}}}
        all_cards = []
        
        if isinstance(cards_data, dict):
            # Check if it's the nested set structure
            for set_key, set_data in cards_data.items():
                if isinstance(set_data, dict) and 'cards' in set_data:
                    # Extract cards from this set
                    for card_id, card in set_data['cards'].items():
                        all_cards.append(card)
            
            # Fallback: old structure
            if not all_cards:
                all_cards = cards_data.get('cards', cards_data.get('data', []))
        else:
            all_cards = cards_data
        
        print(f"Found {len(all_cards)} cards")
        
        if len(all_cards) == 0:
            print("WARNING: No cards found in file. Check JSON structure.")
            return 0
        
        linked_count = 0
        
        for card in all_cards:
            # Handle multilingual names
            card_name_obj = card.get('name', '')
            
            if isinstance(card_name_obj, dict):
                # Multilingual: try English first
                card_name = card_name_obj.get('en', 
                                              card_name_obj.get('es', 
                                              card_name_obj.get('fr', '')))
            else:
                card_name = card_name_obj
            
            if not card_name:
                continue
            
            # Find matching entity
            entity_uri = self.find_best_match(card_name)
            
            if entity_uri:
                # Create card URI
                card_id = card.get('id', card_name.replace(' ', '_'))
                card_uri = self.METW[str(card_id)]
                
                # Add card as a Thing
                self.graph.add((card_uri, RDF.type, self.SCHEMA.Thing))
                self.graph.add((card_uri, RDFS.label, Literal(card_name, lang='en')))
                
                # Link entity to card
                self.graph.add((entity_uri, self.SCHEMA.subjectOf, card_uri))
                
                # Add card properties
                if 'type' in card:
                    card_type = card['type']
                    self.graph.add((card_uri, self.SCHEMA.additionalType, 
                                  Literal(card_type, lang='en')))
                
                if 'alignment' in card:
                    self.graph.add((card_uri, self.SCHEMA.additionalProperty, 
                                  Literal(f"alignment: {card['alignment']}", lang='en')))
                
                # Add set information
                card_set = card.get('set', '')
                if card_set:
                    self.graph.add((card_uri, self.SCHEMA.isPartOf, 
                                  Literal(f"METW Set: {card_set}", lang='en')))
                
                linked_count += 1
                
                if linked_count % 50 == 0:
                    print(f"  Linked {linked_count} cards...")
        
        print(f"\nLinked {linked_count} cards to entities")
        return linked_count
    
    def save_enriched_kg(self, output_file: Path):
        """Save enriched KG"""
        print(f"\nSaving enriched KG to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.graph.serialize(destination=str(output_file), format='turtle')
        print(f"Saved {len(self.graph)} triples")


def main():
    """Main execution"""
    # File paths
    kg_file = Path('./data/rdf/tolkien_kg.ttl')
    cards_file = Path('./data/external/cards.json')  # You need to download this
    output_file = Path('./data/rdf/tolkien_kg_enriched.ttl')
    
    # Check files exist
    if not kg_file.exists():
        print(f"ERROR: KG file not found: {kg_file}")
        return
    
    print("="*60)
    print("METW CARDS ENRICHMENT")
    print("="*60)
    
    # Enrich
    enricher = METWEnricher(kg_file)
    
    if cards_file.exists():
        enricher.enrich_with_cards(cards_file)
        enricher.save_enriched_kg(output_file)
        
        print("\n" + "="*60)
        print("ENRICHMENT COMPLETE")
        print("="*60)
        print(f"Output: {output_file}")
    else:
        print(f"\nWARNING: Cards file not found at {cards_file}")
        print("Please download cards.json and place it in ./data/external/")
        print("Continuing without METW enrichment...")
        
        # Save anyway (no changes)
        enricher.save_enriched_kg(output_file)
    

if __name__ == '__main__':
    main()