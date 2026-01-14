#!/usr/bin/env python3
"""
Combined METW + CSV Enrichment (Step 4)
This single script enriches the Knowledge Graph with BOTH:
- METW card game data
- LOTR CSV character data
"""

import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, XSD
from difflib import SequenceMatcher


class CombinedEnricher:
    """Enrich KG with both METW cards and CSV character data"""
    
    def __init__(self, kg_file: Path):
        self.base_uri = "http://tolkiengateway.semanticweb.org/"
        
        # Define namespaces
        self.NS = Namespace(self.base_uri)
        self.RESOURCE = Namespace(f"{self.base_uri}resource/")
        self.SCHEMA = Namespace("http://schema.org/")
        self.PROP = Namespace(f"{self.base_uri}property/")
        self.METW = Namespace("http://metw.org/card/")
        self.LOTRCSV = Namespace("http://lotrcsv.org/character/")
        
        # Load existing KG
        print(f"Loading Knowledge Graph from {kg_file}...")
        self.graph = Graph()
        self.graph.parse(str(kg_file), format='turtle')
        print(f"Loaded {len(self.graph)} existing triples")
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("tgprop", self.PROP)
        self.graph.bind("metw", self.METW)
        self.graph.bind("lotrcsv", self.LOTRCSV)
        
        # Build entity index for matching
        self.entity_index = self._build_entity_index()
        
        # Statistics
        self.stats = {
            'metw': {
                'total_cards': 0,
                'linked_cards': 0,
                'triples_added': 0
            },
            'csv': {
                'total_characters': 0,
                'exact_matches': 0,
                'fuzzy_matches': 0,
                'no_matches': 0,
                'triples_added': 0,
                'enriched_fields': {
                    'birth': 0, 'death': 0, 'gender': 0,
                    'hair': 0, 'height': 0, 'race': 0,
                    'realm': 0, 'spouse': 0
                }
            }
        }
    
    def _build_entity_index(self) -> Dict[str, URIRef]:
        """Build index of entity names to URIs"""
        index = {}
        
        query = """
        PREFIX schema: <http://schema.org/>
        SELECT ?entity ?name WHERE {
            ?entity schema:name ?name .
        }
        """
        
        results = self.graph.query(query)
        for row in results:
            entity_uri = row.entity
            name = str(row.name).lower()
            index[name] = entity_uri
            index[name.replace(' ', '')] = entity_uri
            normalized = name.replace('-', '').replace('_', '').replace('.', '')
            index[normalized] = entity_uri
        
        print(f"Built entity index with {len(index)} entries")
        return index
    
    def similarity_score(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def find_best_match(self, name: str, threshold: float = 0.85) -> tuple:
        """Find best matching entity"""
        name_lower = name.lower()
        
        # Exact match
        if name_lower in self.entity_index:
            return self.entity_index[name_lower], 1.0
        
        # Try without spaces
        name_no_spaces = name_lower.replace(' ', '')
        if name_no_spaces in self.entity_index:
            return self.entity_index[name_no_spaces], 1.0
        
        # Fuzzy match
        best_score = 0.0
        best_match = None
        
        for entity_name, entity_uri in self.entity_index.items():
            score = self.similarity_score(name, entity_name)
            if score > best_score and score >= threshold:
                best_score = score
                best_match = entity_uri
        
        return (best_match, best_score) if best_match else (None, 0.0)
    
    # ========== METW ENRICHMENT ==========
    
    def enrich_with_metw(self, cards_file: Path, fuzzy_threshold: float = 0.85):
        """Enrich with METW card game data"""
        if not cards_file.exists():
            print(f"\nWARNING: METW cards file not found at {cards_file}")
            print("Skipping METW enrichment...")
            return
        
        print(f"\n{'='*60}")
        print("METW CARD ENRICHMENT")
        print(f"{'='*60}")
        print(f"Loading cards from {cards_file}...")
        
        with open(cards_file, 'r', encoding='utf-8') as f:
            cards_data = json.load(f)
        
        # Handle nested structure like {"AS": {"cards": {...}}, "DM": {"cards": {...}}}
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
        
        cards = all_cards
        
        self.stats['metw']['total_cards'] = len(cards)
        print(f"Found {len(cards)} cards")
        
        triples_before = len(self.graph)
        
        for i, card in enumerate(cards):
            if (i + 1) % 50 == 0:
                print(f"  Linked {i + 1} cards...")
            
            # Handle multilingual names
            card_name_obj = card.get('name', '')
            
            if isinstance(card_name_obj, dict):
                # Multilingual: try English first
                card_name = card_name_obj.get('en', 
                                              card_name_obj.get('es', 
                                              card_name_obj.get('fr', '')))
            else:
                card_name = card_name_obj
            
            card_name = str(card_name).strip()
            if not card_name:
                continue
            
            # Find matching entity
            match_result = self.find_best_match(card_name, threshold=fuzzy_threshold)
            if match_result[0] is None:
                continue
            
            entity_uri, _ = match_result
            self.stats['metw']['linked_cards'] += 1
            
            # Create card URI using card ID if available
            card_id = card.get('id', card_name.replace(' ', '_'))
            card_uri = self.METW[str(card_id)]
            self.graph.add((card_uri, RDF.type, self.SCHEMA.Thing))
            self.graph.add((card_uri, RDFS.label, Literal(card_name, lang='en')))
            
            # Link entity to card
            self.graph.add((entity_uri, self.SCHEMA.subjectOf, card_uri))
            self.graph.add((entity_uri, self.PROP.hasCard, card_uri))
            
            # Add card type
            if 'type' in card:
                card_type = card['type']
                if isinstance(card_type, dict):
                    card_type = card_type.get('en', str(card_type))
                self.graph.add((card_uri, self.SCHEMA.additionalType, 
                              Literal(str(card_type), lang='en')))
            
            # Add alignment
            if 'alignment' in card:
                self.graph.add((card_uri, self.SCHEMA.additionalProperty, 
                              Literal(f"alignment: {card['alignment']}", lang='en')))
            
            # Add other properties
            for key, value in card.items():
                if key not in ['name', 'id', 'type', 'alignment'] and value:
                    prop_uri = self.METW[f"card_{key}"]
                    if isinstance(value, dict):
                        # Handle multilingual properties
                        value = value.get('en', str(value))
                    if isinstance(value, (int, float)):
                        self.graph.add((card_uri, prop_uri, Literal(value)))
                    else:
                        self.graph.add((card_uri, prop_uri, Literal(str(value), lang='en')))
        
        self.stats['metw']['triples_added'] = len(self.graph) - triples_before
        print(f"\n✓ METW enrichment complete!")
        print(f"  Linked cards: {self.stats['metw']['linked_cards']}")
        print(f"  Triples added: {self.stats['metw']['triples_added']}")
    
    # ========== CSV ENRICHMENT ==========
    
    def clean_value(self, value: str) -> Optional[str]:
        """Clean CSV value"""
        if not value or not value.strip():
            return None
        cleaned = value.strip()
        if cleaned in [',', '-', '', 'None', 'none']:
            return None
        return cleaned
    
    def add_csv_character_triples(self, entity_uri: URIRef, csv_data: Dict[str, str], match_score: float):
        """Add triples from CSV character data"""
        triples_before = len(self.graph)
        
        # Add provenance annotation (where the data came from)
        self.graph.add((entity_uri, self.SCHEMA.isBasedOn, 
                    Literal("LOTR Characters CSV Dataset", lang='en')))
        
        if match_score < 1.0:
            self.graph.add((entity_uri, self.PROP.csvMatchScore, Literal(match_score, datatype=XSD.float)))
        
        # Birth date
        birth = self.clean_value(csv_data.get('birth'))
        if birth and not list(self.graph.objects(entity_uri, self.SCHEMA.birthDate)):
            self.graph.add((entity_uri, self.SCHEMA.birthDate, Literal(birth, lang='en')))
            self.stats['csv']['enriched_fields']['birth'] += 1
        
        # Death date
        death = self.clean_value(csv_data.get('death'))
        if death and not list(self.graph.objects(entity_uri, self.SCHEMA.deathDate)):
            self.graph.add((entity_uri, self.SCHEMA.deathDate, Literal(death, lang='en')))
            self.stats['csv']['enriched_fields']['death'] += 1
        
        # Gender
        gender = self.clean_value(csv_data.get('gender'))
        if gender and not list(self.graph.objects(entity_uri, self.SCHEMA.gender)):
            self.graph.add((entity_uri, self.SCHEMA.gender, Literal(gender, lang='en')))
            self.stats['csv']['enriched_fields']['gender'] += 1
        
        # Hair
        hair = self.clean_value(csv_data.get('hair'))
        if hair and not list(self.graph.objects(entity_uri, self.PROP.hairColor)):
            self.graph.add((entity_uri, self.PROP.hairColor, Literal(hair, lang='en')))
            self.stats['csv']['enriched_fields']['hair'] += 1
        
        # Height
        height = self.clean_value(csv_data.get('height'))
        if height and not list(self.graph.objects(entity_uri, self.SCHEMA.height)):
            self.graph.add((entity_uri, self.SCHEMA.height, Literal(height, lang='en')))
            self.stats['csv']['enriched_fields']['height'] += 1
        
        # Race
        race = self.clean_value(csv_data.get('race'))
        if race and not list(self.graph.objects(entity_uri, self.PROP.race)):
            for single_race in [r.strip() for r in race.split(',')]:
                if single_race:
                    self.graph.add((entity_uri, self.PROP.race, Literal(single_race, lang='en')))
            self.stats['csv']['enriched_fields']['race'] += 1
        
        # Realm
        realm = self.clean_value(csv_data.get('realm'))
        if realm and not list(self.graph.objects(entity_uri, self.PROP.realm)):
            for single_realm in [r.strip() for r in realm.split(',')]:
                if single_realm:
                    realm_uri = self.RESOURCE[single_realm.replace(' ', '_')]
                    self.graph.add((entity_uri, self.PROP.realm, realm_uri))
                    self.graph.add((entity_uri, self.PROP.realmName, Literal(single_realm, lang='en')))
            self.stats['csv']['enriched_fields']['realm'] += 1
        
        # Spouse
        spouse = self.clean_value(csv_data.get('spouse'))
        if spouse and not list(self.graph.objects(entity_uri, self.SCHEMA.spouse)):
            spouse_match = self.find_best_match(spouse, threshold=0.9)
            if spouse_match[0]:
                self.graph.add((entity_uri, self.SCHEMA.spouse, spouse_match[0]))
            else:
                spouse_uri = self.RESOURCE[spouse.replace(' ', '_')]
                self.graph.add((entity_uri, self.SCHEMA.spouse, spouse_uri))
            self.stats['csv']['enriched_fields']['spouse'] += 1
        
        # Source annotation
        self.graph.add((entity_uri, self.SCHEMA.isBasedOn, 
                       Literal("LOTR Characters CSV Dataset", lang='en')))
        
        return len(self.graph) - triples_before
    
    def enrich_with_csv(self, csv_file: Path, fuzzy_threshold: float = 0.85):
        """Enrich with CSV character data"""
        if not csv_file.exists():
            print(f"\nWARNING: CSV file not found at {csv_file}")
            print("Skipping CSV enrichment...")
            return
        
        print(f"\n{'='*60}")
        print("CSV CHARACTER ENRICHMENT")
        print(f"{'='*60}")
        print(f"Loading characters from {csv_file}...")
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            characters = list(reader)
        
        self.stats['csv']['total_characters'] = len(characters)
        print(f"Found {len(characters)} characters")
        
        print(f"\nMatching and enriching (threshold={fuzzy_threshold})...")
        for i, char_data in enumerate(characters):
            name = char_data.get('name', '').strip()
            if not name:
                continue
            
            if (i + 1) % 50 == 0 or (i + 1) == len(characters):
                print(f"  Processing {i + 1}/{len(characters)}: {name}")
            
            match_result = self.find_best_match(name, threshold=fuzzy_threshold)
            if match_result[0] is None:
                self.stats['csv']['no_matches'] += 1
                continue
            
            entity_uri, match_score = match_result
            
            if match_score == 1.0:
                self.stats['csv']['exact_matches'] += 1
            else:
                self.stats['csv']['fuzzy_matches'] += 1
            
            num_triples = self.add_csv_character_triples(entity_uri, char_data, match_score)
            self.stats['csv']['triples_added'] += num_triples
        
        print(f"\n✓ CSV enrichment complete!")
        print(f"  Exact matches: {self.stats['csv']['exact_matches']}")
        print(f"  Fuzzy matches: {self.stats['csv']['fuzzy_matches']}")
        print(f"  No matches: {self.stats['csv']['no_matches']}")
        print(f"  Triples added: {self.stats['csv']['triples_added']}")
    
    def save_graph(self, output_file: Path):
        """Save enriched graph"""
        print(f"\nSaving enriched Knowledge Graph to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.graph.serialize(destination=str(output_file), format='turtle')
        print(f"✓ Saved {len(self.graph)} triples")
    
    def print_summary(self):
        """Print final summary"""
        print("\n" + "="*60)
        print("COMBINED ENRICHMENT SUMMARY")
        print("="*60)
        print("\nMETW Cards:")
        print(f"  Total cards: {self.stats['metw']['total_cards']}")
        print(f"  Linked cards: {self.stats['metw']['linked_cards']}")
        print(f"  Triples added: {self.stats['metw']['triples_added']}")
        
        print("\nCSV Characters:")
        print(f"  Total characters: {self.stats['csv']['total_characters']}")
        print(f"  Exact matches: {self.stats['csv']['exact_matches']}")
        print(f"  Fuzzy matches: {self.stats['csv']['fuzzy_matches']}")
        print(f"  No matches: {self.stats['csv']['no_matches']}")
        print(f"  Triples added: {self.stats['csv']['triples_added']}")
        
        total_enrichments = (self.stats['metw']['linked_cards'] + 
                           self.stats['csv']['exact_matches'] + 
                           self.stats['csv']['fuzzy_matches'])
        total_triples = self.stats['metw']['triples_added'] + self.stats['csv']['triples_added']
        
        print("\nTotal:")
        print(f"  Entities enriched: {total_enrichments}")
        print(f"  Triples added: {total_triples}")


def main():
    """Main execution - Combined METW + CSV enrichment"""
    print("="*60)
    print("STEP 4: COMBINED ENRICHMENT (METW + CSV)")
    print("="*60)
    
    # File paths
    kg_file = Path('./data/rdf/tolkien_kg.ttl')
    cards_file = Path('./data/external/cards.json')
    csv_file = Path('./data/external/lotr_characters.csv')
    output_file = Path('./data/rdf/tolkien_kg_enriched.ttl')
    
    # Check base KG exists
    if not kg_file.exists():
        print(f"\nERROR: Base KG file not found: {kg_file}")
        print("Run Step 2 (RDF generation) first!")
        return
    
    # Initialize enricher
    print("\nInitializing combined enricher...")
    enricher = CombinedEnricher(kg_file)
    
    # Enrich with METW cards
    enricher.enrich_with_metw(cards_file, fuzzy_threshold=0.85)
    
    # Enrich with CSV data
    enricher.enrich_with_csv(csv_file, fuzzy_threshold=0.85)
    
    # Save enriched graph
    enricher.save_graph(output_file)
    
    # Print summary
    enricher.print_summary()
    
    print("\n" + "="*60)
    print("="*60)
    print(f"Enriched KG: {output_file}")
    print("\nThis file now contains:")
    print("  ✓ Base Tolkien Gateway wiki data")
    print("  ✓ METW card game enrichments")
    print("  ✓ LOTR CSV character enrichments")
    print("="*60)


if __name__ == '__main__':
    main()