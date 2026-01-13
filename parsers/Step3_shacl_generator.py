#!/usr/bin/env python3
"""
SHACL Shapes Generator for Tolkien Gateway
Creates SHACL constraints based on infobox templates
"""

import json
from pathlib import Path
from typing import Dict, List, Set, Any
from collections import defaultdict
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, XSD, BNode
from rdflib.namespace import SH


class SHACLShapeGenerator:
    """Generate SHACL shapes from infobox templates"""
    
    def __init__(self, base_uri: str = "http://tolkiengateway.semanticweb.org/"):
        self.base_uri = base_uri
        
        # Define namespaces
        self.NS = Namespace(base_uri)
        self.RESOURCE = Namespace(f"{base_uri}resource/")
        self.PROP = Namespace(f"{base_uri}property/")
        self.SCHEMA = Namespace("http://schema.org/")
        self.SHAPE = Namespace(f"{base_uri}shape/")
        
        # Initialize graph
        self.graph = Graph()
        
        # Bind namespaces
        self.graph.bind("tg", self.NS)
        self.graph.bind("tgr", self.RESOURCE)
        self.graph.bind("tgprop", self.PROP)
        self.graph.bind("schema", self.SCHEMA)
        self.graph.bind("sh", SH)
        self.graph.bind("shape", self.SHAPE)
        
        # Track property usage per entity type
        self.property_usage = defaultdict(lambda: defaultdict(int))
        self.property_datatypes = defaultdict(set)
    
    def analyze_entities(self, entities: List[Dict[str, Any]]):
        """Analyze entities to determine property patterns"""
        print("Analyzing entity patterns...")
        
        for entity in entities:
            entity_type = entity.get('type', 'unknown')
            infobox = entity.get('infobox')
            
            if not infobox:
                continue
            
            parameters = infobox.get('parameters', {})
            
            for field, value_data in parameters.items():
                if not value_data:
                    continue
                
                # Track usage
                self.property_usage[entity_type][field] += 1
                
                # Track datatypes
                cleaned = value_data.get('cleaned', '')
                if cleaned:
                    self.property_datatypes[field].add(self.infer_datatype(cleaned, field))
        
        print(f"Found {len(self.property_usage)} entity types")
        for entity_type, props in self.property_usage.items():
            print(f"  {entity_type}: {len(props)} properties")
    
    def infer_datatype(self, value: str, field: str) -> str:
        """Infer XSD datatype from value and field name"""
        if 'date' in field.lower():
            return 'xsd:string'  # Tolkien dates are complex
        
        if field.lower() in ['age', 'height']:
            try:
                int(value)
                return 'xsd:integer'
            except ValueError:
                return 'xsd:string'
        
        if field.lower() == 'gender':
            return 'xsd:string'
        
        return 'xsd:string'
    
    def create_character_shape(self, total_characters: int) -> URIRef:
        """Create SHACL shape for Character entities"""
        shape_uri = self.SHAPE.CharacterShape
        
        # Shape declaration
        self.graph.add((shape_uri, RDF.type, SH.NodeShape))
        self.graph.add((shape_uri, SH.targetClass, self.SCHEMA.Person))
        self.graph.add((shape_uri, RDFS.label, Literal("Character Shape")))
        self.graph.add((shape_uri, RDFS.comment, 
                       Literal("Validation shape for Tolkien Gateway characters")))
        
        # Get character properties
        char_props = self.property_usage.get('infobox character', {})
        
        # Required properties (appear in >80% of characters)
        threshold = total_characters * 0.8
        
        # Name is always required
        name_prop = self.graph.resource(shape_uri)
        name_constraint = self.graph.resource(BNode())
        name_constraint.add(SH.path, self.SCHEMA.name)
        name_constraint.add(SH.minCount, Literal(1))
        # Remove maxCount to allow multiple names (different forms)
        # Remove datatype constraint to allow language tags
        name_constraint.add(SH.nodeKind, SH.Literal)
        self.graph.add((shape_uri, SH.property, name_constraint.identifier))
        
        # Gender (common property)
        if char_props.get('gender', 0) > 100:
            gender_constraint = self.graph.resource(BNode())
            gender_constraint.add(SH.path, self.SCHEMA.gender)
            gender_constraint.add(SH.datatype, XSD.string)
            gender_constraint.add(SH['in'], self.create_gender_list())
            self.graph.add((shape_uri, SH.property, gender_constraint.identifier))
        
        # Race (custom property, very common)
        if char_props.get('race', 0) > 100:
            race_constraint = self.graph.resource(BNode())
            race_constraint.add(SH.path, self.PROP.race)
            race_constraint.add(SH.nodeKind, SH.IRIOrLiteral)
            self.graph.add((shape_uri, SH.property, race_constraint.identifier))
        
        # Birth place
        if char_props.get('birthPlace', 0) > 50:
            birth_constraint = self.graph.resource(BNode())
            birth_constraint.add(SH.path, self.SCHEMA.birthPlace)
            birth_constraint.add(SH.nodeKind, SH.IRIOrLiteral)
            self.graph.add((shape_uri, SH.property, birth_constraint.identifier))
        
        return shape_uri
    
    def create_gender_list(self) -> URIRef:
        """Create RDF list of valid gender values"""
        # Create a simple list node
        list_node = BNode()
        from rdflib.collection import Collection
        collection = Collection(self.graph, list_node)
        collection += [Literal("Male"), Literal("Female"), Literal("Unknown")]
        return list_node
    
    def create_place_shape(self) -> URIRef:
        """Create SHACL shape for Place entities"""
        shape_uri = self.SHAPE.PlaceShape
        
        self.graph.add((shape_uri, RDF.type, SH.NodeShape))
        self.graph.add((shape_uri, SH.targetClass, self.SCHEMA.Place))
        self.graph.add((shape_uri, RDFS.label, Literal("Place Shape")))
        
        # Name required
        name_constraint = self.graph.resource(BNode())
        name_constraint.add(SH.path, self.SCHEMA.name)
        name_constraint.add(SH.minCount, Literal(1))
        name_constraint.add(SH.nodeKind, SH.Literal)
        self.graph.add((shape_uri, SH.property, name_constraint.identifier))
        
        return shape_uri
    
    def create_thing_shape(self) -> URIRef:
        """Create SHACL shape for generic Thing entities"""
        shape_uri = self.SHAPE.ThingShape
        
        self.graph.add((shape_uri, RDF.type, SH.NodeShape))
        self.graph.add((shape_uri, SH.targetClass, self.SCHEMA.Thing))
        self.graph.add((shape_uri, RDFS.label, Literal("Thing Shape")))
        
        # Name required
        name_constraint = self.graph.resource(BNode())
        name_constraint.add(SH.path, self.SCHEMA.name)
        name_constraint.add(SH.minCount, Literal(1))
        self.graph.add((shape_uri, SH.property, name_constraint.identifier))
        
        return shape_uri
    
    def generate_shapes(self, entities: List[Dict[str, Any]]) -> Graph:
        """Generate all SHACL shapes"""
        print("Generating SHACL shapes...")
        
        # Analyze entities first
        self.analyze_entities(entities)
        
        # Count entities by type
        character_count = sum(1 for e in entities if e.get('type') == 'infobox character')
        
        # Create shapes
        print("Creating Character shape...")
        self.create_character_shape(character_count)
        
        print("Creating Place shape...")
        self.create_place_shape()
        
        print("Creating generic Thing shape...")
        self.create_thing_shape()
        
        print(f"Generated {len(self.graph)} SHACL triples")
        return self.graph
    
    def save_shapes(self, output_file: Path):
        """Save SHACL shapes to file"""
        print(f"Saving SHACL shapes to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        self.graph.serialize(destination=str(output_file), format='turtle')
        print(f"Saved {len(self.graph)} triples")


def validate_kg(kg_file: Path, shapes_file: Path, report_file: Path):
    """Validate Knowledge Graph against SHACL shapes"""
    from pyshacl import validate
    
    print("\n" + "="*60)
    print("VALIDATING KNOWLEDGE GRAPH")
    print("="*60)
    
    print(f"Loading KG from {kg_file}...")
    data_graph = Graph()
    data_graph.parse(str(kg_file), format='turtle')
    print(f"Loaded {len(data_graph)} triples")
    
    print(f"Loading shapes from {shapes_file}...")
    shapes_graph = Graph()
    shapes_graph.parse(str(shapes_file), format='turtle')
    print(f"Loaded {len(shapes_graph)} shape triples")
    
    print("\nRunning SHACL validation...")
    conforms, results_graph, results_text = validate(
        data_graph,
        shacl_graph=shapes_graph,
        inference='rdfs',
        abort_on_first=False,
    )
    
    print("\n" + "="*60)
    print("VALIDATION RESULTS")
    print("="*60)
    print(f"Conforms: {conforms}")
    print()
    print(results_text)
    
    # Save validation report
    if results_graph:
        report_file.parent.mkdir(parents=True, exist_ok=True)
        results_graph.serialize(destination=str(report_file), format='turtle')
        print(f"\nValidation report saved to {report_file}")
    
    return conforms, results_text


def main():
    """Main execution"""
    # Paths
    entities_file = Path('./data/normalized/normalized_entities.json')
    shapes_file = Path('./data/shacl/tolkien_shapes.ttl')
    kg_file = Path('./data/rdf/tolkien_kg.ttl')
    report_file = Path('./data/shacl/validation_report.ttl')
    
    # Check input files exist
    if not entities_file.exists():
        print(f"ERROR: {entities_file} not found")
        return
    
    if not kg_file.exists():
        print(f"ERROR: {kg_file} not found. Run rdf_generator.py first")
        return
    
    print("="*60)
    print("SHACL SHAPES GENERATION")
    print("="*60)
    print()
    
    # Load entities
    with open(entities_file, 'r', encoding='utf-8') as f:
        entities = json.load(f)
    
    # Generate shapes
    generator = SHACLShapeGenerator()
    generator.generate_shapes(entities)
    generator.save_shapes(shapes_file)
    
    print()
    print("="*60)
    print("SHAPES GENERATION COMPLETE")
    print("="*60)
    print(f"Shapes file: {shapes_file}")
    
    # Validate
    try:
        print("\nAttempting validation...")
        conforms, _ = validate_kg(kg_file, shapes_file, report_file)
        
        if conforms:
            print("\n✓ Knowledge Graph is VALID!")
        else:
            print("\n✗ Knowledge Graph has validation errors")
            print(f"See {report_file} for details")
    
    except ImportError:
        print("\nNote: pyshacl not installed. To validate, run:")
        print("  pip install pyshacl")
        print(f"  Then run validation manually")
    except Exception as e:
        print(f"\nValidation error: {e}")
        print("You can validate manually using a SHACL validator")


if __name__ == '__main__':
    main()