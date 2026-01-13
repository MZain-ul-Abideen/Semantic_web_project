#!/usr/bin/env python3
"""
RDF Generator for Tolkien Gateway Knowledge Graph
Converts normalized JSON entities to RDF using schema.org vocabulary
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, XSD, OWL
from rdflib.namespace import FOAF, DCTERMS


class TolkienRDFGenerator:
    """Generate RDF triples from normalized Tolkien Gateway entities"""
    
    def __init__(self, base_uri: str = "http://tolkiengateway.semanticweb.org/"):
        self.base_uri = base_uri
        
        # Define namespaces
        self.NS = Namespace(base_uri)
        self.RESOURCE = Namespace(f"{base_uri}resource/")
        self.PAGE = Namespace(f"{base_uri}page/")
        self.PROP = Namespace(f"{base_uri}property/")
        self.SCHEMA = Namespace("http://schema.org/")
        
        # Initialize graph
        self.graph = Graph()
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("tgpage", self.PAGE)
        self.graph.bind("tgprop", self.PROP)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("owl", OWL)
        self.graph.bind("foaf", FOAF)
        self.graph.bind("dcterms", DCTERMS)
        
        # Property mappings: infobox field → schema.org property
        self.property_mappings = {
            'name': self.SCHEMA.name,
            'birthDate': self.SCHEMA.birthDate,
            'birthPlace': self.SCHEMA.birthPlace,
            'deathDate': self.SCHEMA.deathDate,
            'deathPlace': self.SCHEMA.deathPlace,
            'gender': self.SCHEMA.gender,
            'spouse': self.SCHEMA.spouse,
            'children': self.SCHEMA.children,
            'parent': self.SCHEMA.parent,
            'parentage': self.SCHEMA.parent,
            'siblings': self.SCHEMA.sibling,
            'occupation': self.SCHEMA.hasOccupation,
            'affiliation': self.SCHEMA.affiliation,
            'image': self.SCHEMA.image,
            'description': self.SCHEMA.description,
            'height': self.SCHEMA.height,
            'nationality': self.SCHEMA.nationality,
            
            # Custom properties (not in schema.org)
            'race': self.PROP.race,
            'titles': self.PROP.title,
            'position': self.PROP.position,
            'location': self.PROP.location,
            'language': self.PROP.language,
            'weapon': self.PROP.weapon,
            'weapons': self.PROP.weapon,
            'hair': self.PROP.hairColor,
            'eyes': self.PROP.eyeColor,
            'clothing': self.PROP.clothing,
            'house': self.PROP.house,
            'heritage': self.PROP.heritage,
            'otherNames': self.SCHEMA.alternateName,
            'realName': self.SCHEMA.alternateName,
        }
    
    def create_uri(self, name: str, uri_type: str = "resource") -> URIRef:
        """
        Create a URI for an entity
        
        Args:
            name: Entity name
            uri_type: 'resource' or 'page'
        """
        # Clean the name - replace spaces with underscores
        # Don't use quote() as it encodes Unicode characters (Æ, É, í, ó, etc.)
        # RDFLib handles Unicode IRIs correctly, so we just need to handle spaces
        clean_name = name.replace(' ', '_')
        
        if uri_type == "resource":
            return self.RESOURCE[clean_name]
        elif uri_type == "page":
            return self.PAGE[clean_name]
        else:
            return self.NS[clean_name]
    
    def determine_class(self, entity_type: str) -> URIRef:
        """Determine schema.org class based on entity type"""
        type_mappings = {
            'character': self.SCHEMA.Person,
            'infobox character': self.SCHEMA.Person,
            'place': self.SCHEMA.Place,
            'location': self.SCHEMA.Place,
            'artifact': self.SCHEMA.Thing,
            'item': self.SCHEMA.Thing,
            'event': self.SCHEMA.Event,
            'organization': self.SCHEMA.Organization,
        }
        
        # Normalize type
        if entity_type:
            entity_type_lower = entity_type.lower()
            for key, schema_class in type_mappings.items():
                if key in entity_type_lower:
                    return schema_class
        
        # Default to Thing
        return self.SCHEMA.Thing
    
    def add_basic_triples(self, entity: Dict[str, Any], resource_uri: URIRef, page_uri: URIRef):
        """Add basic triples for an entity"""
        title = entity.get('title', entity.get('entity_id'))
        
        # Type
        entity_type = entity.get('type')
        schema_class = self.determine_class(entity_type)
        self.graph.add((resource_uri, RDF.type, schema_class))
        
        # Label
        self.graph.add((resource_uri, RDFS.label, Literal(title, lang='en')))
        self.graph.add((resource_uri, self.SCHEMA.name, Literal(title, lang='en')))
        
        # Page relationship (resource is about page)
        self.graph.add((page_uri, FOAF.primaryTopic, resource_uri))
        self.graph.add((resource_uri, FOAF.isPrimaryTopicOf, page_uri))
        
        # Page type
        self.graph.add((page_uri, RDF.type, self.SCHEMA.WebPage))
        self.graph.add((page_uri, DCTERMS.title, Literal(title)))
    
    def add_infobox_triples(self, entity: Dict[str, Any], resource_uri: URIRef):
        """Add triples from infobox parameters"""
        infobox = entity.get('infobox')
        if not infobox:
            return
        
        parameters = infobox.get('parameters', {})
        
        for field, value_data in parameters.items():
            if not value_data:
                continue
            
            cleaned_value = value_data.get('cleaned', '')
            internal_links = value_data.get('internal_links', [])
            
            # Skip empty values
            if not cleaned_value or cleaned_value.strip() == '':
                continue
            
            # Get the appropriate property
            prop = self.property_mappings.get(field, self.PROP[field])
            
            # If there are internal links, create object properties
            if internal_links:
                for link in internal_links:
                    linked_resource = self.create_uri(link, 'resource')
                    self.graph.add((resource_uri, prop, linked_resource))
            else:
                # Add as literal
                # Try to determine datatype
                literal = self.create_typed_literal(cleaned_value, field)
                self.graph.add((resource_uri, prop, literal))
    
    def create_typed_literal(self, value: str, field: str) -> Literal:
        """Create a typed literal based on field and value"""
        # Date fields
        if 'date' in field.lower() or 'birth' in field.lower() or 'death' in field.lower():
            # Keep as language-tagged string
            return Literal(value, lang='en')
        
        # Numeric fields
        if field.lower() in ['age', 'height']:
            try:
                # Try integer
                return Literal(int(value), datatype=XSD.integer)
            except ValueError:
                # Keep as string
                return Literal(value, lang='en')
        
        # Gender - NO DATATYPE, just plain literal for SHACL compatibility
        if field.lower() == 'gender':
            # Normalize to match SHACL enum
            gender_normalized = value.strip()
            if gender_normalized.lower() == 'male':
                return Literal("Male")
            elif gender_normalized.lower() == 'female':
                return Literal("Female")
            else:
                return Literal("Unknown")
        
        # Default: language-tagged string
        return Literal(value, lang='en')
    
    def add_link_triples(self, entity: Dict[str, Any], resource_uri: URIRef):
        """Add triples for internal wiki links"""
        internal_links = entity.get('internal_links', [])
        
        for link in internal_links:
            linked_resource = self.create_uri(link, 'resource')
            # Use a generic "related" property
            self.graph.add((resource_uri, self.SCHEMA.relatedLink, linked_resource))
    
    def process_entity(self, entity: Dict[str, Any]):
        """Process a single entity and add all its triples"""
        entity_id = entity.get('entity_id')
        if not entity_id:
            return
        
        # Create URIs
        resource_uri = self.create_uri(entity_id, 'resource')
        page_uri = self.create_uri(entity_id, 'page')
        
        # Add basic triples
        self.add_basic_triples(entity, resource_uri, page_uri)
        
        # Add infobox triples
        self.add_infobox_triples(entity, resource_uri)
        
        # Add link triples (commented out to reduce noise - uncomment if needed)
        # self.add_link_triples(entity, resource_uri)
    
    def generate_from_file(self, input_file: Path) -> Graph:
        """Generate RDF graph from normalized entities file"""
        print(f"Loading entities from {input_file}...")
        with open(input_file, 'r', encoding='utf-8') as f:
            entities = json.load(f)
        
        print(f"Processing {len(entities)} entities...")
        for i, entity in enumerate(entities):
            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{len(entities)} entities")
            self.process_entity(entity)
        
        print(f"Generated {len(self.graph)} triples")
        return self.graph
    
    def save_graph(self, output_file: Path, format: str = 'turtle'):
        """Save RDF graph to file"""
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Saving graph to {output_file}...")
        self.graph.serialize(destination=str(output_file), format=format, encoding='utf-8')
        print(f"Saved {len(self.graph)} triples to {output_file}")


def main():
    """Main execution"""
    # File paths
    input_file = Path('./data/normalized/normalized_entities.json')
    output_file = Path('./data/rdf/tolkien_kg.ttl')
    
    # Check input file exists
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        print("Run Step1_parse_all_pages.py first to generate normalized_entities.json")
        return
    
    # Generate RDF
    generator = TolkienRDFGenerator()
    generator.generate_from_file(input_file)
    generator.save_graph(output_file, format='turtle')
    
    print("\n" + "="*60)
    print("RDF GENERATION COMPLETE")
    print("="*60)
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    print(f"Total triples: {len(generator.graph)}")


if __name__ == '__main__':
    main()