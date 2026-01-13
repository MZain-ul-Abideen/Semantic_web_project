#!/usr/bin/env python3
"""
SPARQL Queries for Tolkien Gateway Knowledge Graph
Demonstrates reasoning and complex queries
"""

from SPARQLWrapper import SPARQLWrapper, JSON, N3
from typing import Dict, List, Any
import json


class TolkienSPARQLQueries:
    """Collection of SPARQL queries for the Tolkien KG"""
    
    def __init__(self, endpoint: str = "http://localhost:3030/tolkienkg/sparql"):
        self.endpoint = endpoint
        self.sparql = SPARQLWrapper(endpoint)
    
    def execute_query(self, query: str, format: str = "json") -> Any:
        """Execute a SPARQL query and return results"""
        self.sparql.setQuery(query)
        
        if format == "json":
            self.sparql.setReturnFormat(JSON)
            return self.sparql.query().convert()
        elif format == "turtle":
            self.sparql.setReturnFormat(N3)
            return self.sparql.query().convert()
        else:
            return self.sparql.query()
    
    def print_results(self, results: Dict, limit: int = 10):
        """Pretty print query results"""
        if 'results' in results and 'bindings' in results['results']:
            bindings = results['results']['bindings'][:limit]
            
            if not bindings:
                print("No results found")
                return
            
            # Get column names
            if bindings:
                cols = list(bindings[0].keys())
                
                # Print header
                print("\n" + " | ".join(cols))
                print("-" * (sum(len(c) for c in cols) + 3 * len(cols)))
                
                # Print rows
                for row in bindings:
                    values = [row.get(col, {}).get('value', 'N/A') for col in cols]
                    # Truncate long values
                    values = [v[:50] + '...' if len(v) > 50 else v for v in values]
                    print(" | ".join(values))
        else:
            print(results)
    
    # ============================================================
    # BASIC QUERIES
    # ============================================================
    
    def query_all_characters(self, limit: int = 20):
        """Get all characters in the KG"""
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?character ?name WHERE {{
            ?character a schema:Person ;
                      schema:name ?name .
            FILTER(lang(?name) = "en" || lang(?name) = "")
        }}
        ORDER BY ?name
        LIMIT {limit}
        """
        
        print("="*60)
        print("QUERY: All Characters")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, limit)
        return results
    
    def query_character_details(self, character_name: str):
        """Get all information about a specific character"""
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX tgprop: <http://tolkiengateway.semanticweb.org/property/>
        
        SELECT ?property ?value WHERE {{
            ?character schema:name "{character_name}"@en ;
                      ?property ?value .
        }}
        """
        
        print("="*60)
        print(f"QUERY: Details for {character_name}")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, 50)
        return results
    
    # ============================================================
    # REASONING QUERIES
    # ============================================================
    
    def query_all_classes_with_inference(self, entity_name: str):
        """
        Get all classes of an entity, including superclasses
        Uses property paths for RDFS reasoning
        """
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT DISTINCT ?class WHERE {{
            ?entity schema:name "{entity_name}"@en ;
                   rdf:type ?directType .
            ?directType rdfs:subClassOf* ?class .
        }}
        """
        
        print("="*60)
        print(f"QUERY: All Classes (with inference) for {entity_name}")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results)
        return results
    
    def query_related_via_sameas(self, character_name: str):
        """
        Find all relations considering owl:sameAs
        If X sameAs Y, then all triples with Y also apply to X
        """
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?property ?value WHERE {{
            # Find the entity
            ?entity schema:name "{character_name}"@en .
            
            # Find equivalent entities via owl:sameAs
            ?entity owl:sameAs* ?equivalent .
            
            # Get all properties of equivalent entities
            ?equivalent ?property ?value .
            
            FILTER(?property != owl:sameAs)
        }}
        LIMIT 50
        """
        
        print("="*60)
        print(f"QUERY: Relations with owl:sameAs expansion for {character_name}")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, 30)
        return results
    
    # ============================================================
    # ANALYTICAL QUERIES
    # ============================================================
    
    def query_characters_by_race(self):
        """Count characters by race"""
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX tgprop: <http://tolkiengateway.semanticweb.org/property/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?race (COUNT(?character) AS ?count) WHERE {
            ?character a schema:Person ;
                      tgprop:race ?raceEntity .
            ?raceEntity rdfs:label ?race .
            FILTER(lang(?race) = "en" || lang(?race) = "")
        }
        GROUP BY ?race
        ORDER BY DESC(?count)
        """
        
        print("="*60)
        print("QUERY: Characters by Race")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, 20)
        return results
    
    def query_characters_with_dbpedia(self):
        """Find characters aligned with DBpedia"""
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?name ?dbpedia WHERE {
            ?character a schema:Person ;
                      schema:name ?name ;
                      owl:sameAs ?dbpedia .
            
            FILTER(lang(?name) = "en" || lang(?name) = "")
            FILTER(STRSTARTS(STR(?dbpedia), "http://dbpedia.org/"))
        }
        ORDER BY ?name
        """
        
        print("="*60)
        print("QUERY: Characters Aligned with DBpedia")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, 30)
        return results
    
    def query_multilingual_labels(self, character_name: str):
        """Get all language labels for a character"""
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT ?language ?label WHERE {{
            ?character schema:name "{character_name}"@en ;
                      rdfs:label ?label .
            
            BIND(LANG(?label) AS ?language)
        }}
        ORDER BY ?language
        """
        
        print("="*60)
        print(f"QUERY: Multilingual Labels for {character_name}")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results)
        return results
    
    def query_relationship_network(self, character_name: str):
        """Find all directly related characters"""
        query = f"""
        PREFIX schema: <http://schema.org/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        
        SELECT DISTINCT ?relationType ?relatedCharacter ?relatedName WHERE {{
            ?character schema:name "{character_name}"@en .
            
            # Outgoing relations
            {{
                ?character ?relationType ?related .
                ?related a schema:Person ;
                        schema:name ?relatedName .
                FILTER(?relationType != schema:name)
                FILTER(lang(?relatedName) = "en" || lang(?relatedName) = "")
            }}
            UNION
            {{
                # Incoming relations
                ?related ?relationType ?character .
                ?related a schema:Person ;
                        schema:name ?relatedName .
                FILTER(?relationType != schema:name)
                FILTER(lang(?relatedName) = "en" || lang(?relatedName) = "")
            }}
        }}
        LIMIT 50
        """
        
        print("="*60)
        print(f"QUERY: Relationship Network for {character_name}")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results, 30)
        return results
    
    # ============================================================
    # STATISTICS
    # ============================================================
    
    def query_statistics(self):
        """Get overall KG statistics"""
        query = """
        PREFIX schema: <http://schema.org/>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT 
            (COUNT(DISTINCT ?s) AS ?total_subjects)
            (COUNT(DISTINCT ?p) AS ?total_properties)
            (COUNT(*) AS ?total_triples)
        WHERE {
            ?s ?p ?o .
        }
        """
        
        print("="*60)
        print("QUERY: Knowledge Graph Statistics")
        print("="*60)
        results = self.execute_query(query)
        self.print_results(results)
        
        # Also count by type
        type_query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        
        SELECT ?type (COUNT(?entity) AS ?count) WHERE {
            ?entity rdf:type ?type .
        }
        GROUP BY ?type
        ORDER BY DESC(?count)
        """
        
        print("\nEntities by Type:")
        type_results = self.execute_query(type_query)
        self.print_results(type_results, 10)
        
        return results


def main():
    """Run demonstration queries"""
    print("="*60)
    print("TOLKIEN GATEWAY SPARQL QUERIES")
    print("="*60)
    print("\nMake sure Fuseki is running at http://localhost:3030")
    print("and the 'tolkien' dataset is loaded\n")
    
    try:
        queries = TolkienSPARQLQueries()
        
        # Run example queries
        queries.query_statistics()
        print("\n")
        
        queries.query_all_characters(limit=10)
        print("\n")
        
        queries.query_characters_with_dbpedia()
        print("\n")
        
        queries.query_character_details("Gandalf")
        print("\n")
        
        queries.query_multilingual_labels("Gandalf")
        print("\n")
        
        queries.query_all_classes_with_inference("Gandalf")
        print("\n")
        
        queries.query_relationship_network("Elrond")
        print("\n")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        print("\nMake sure:")
        print("1. Fuseki is running")
        print("2. Dataset 'tolkien' exists")
        print("3. Data is loaded")
        print("4. SPARQLWrapper is installed: pip install SPARQLWrapper")


if __name__ == '__main__':
    main()