from flask import Flask, request, abort, Response, render_template
from SPARQLWrapper import SPARQLWrapper, TURTLE, JSON
from rdflib import Graph, Namespace, RDF, RDFS
import logging
from urllib.parse import unquote

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# SPARQL endpoint
FUSEKI_URL = "http://localhost:3030/tolkienkg/sparql"
BASE_URI = "http://tolkiengateway.semanticweb.org/resource/"

# Namespaces
SCHEMA = Namespace("http://schema.org/")
TGPROP = Namespace("http://tolkiengateway.semanticweb.org/property/")
FOAF = Namespace("http://xmlns.com/foaf/0.1/")  # Add FOAF at module level

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/sparql")
def sparql_interface():
    """SPARQL query interface"""
    return render_template('sparql.html')

@app.route("/sparql/query", methods=['POST'])
def sparql_query():
    """Execute SPARQL query and return results"""
    query = request.form.get('query', '')
    output_format = request.form.get('format', 'json')
    
    if not query:
        return {"error": "No query provided"}, 400
    
    sparql = SPARQLWrapper(FUSEKI_URL)
    sparql.setQuery(query)
    
    try:
        # Determine query type - need to find the actual query keyword after PREFIX declarations
        query_normalized = ' '.join(query.lower().split())  # Normalize whitespace
        
        # Find the query type keyword
        query_type = None
        for keyword in ['select', 'construct', 'describe', 'ask']:
            if keyword in query_normalized:
                # Make sure it's not part of a URI or PREFIX
                pattern = r'\b' + keyword + r'\b'
                import re
                if re.search(pattern, query_normalized):
                    query_type = keyword
                    break
        
        if not query_type:
            logging.error(f"Could not determine query type for: {query[:100]}")
            return {"error": "Could not determine query type. Please use SELECT, CONSTRUCT, DESCRIBE, or ASK."}, 400
        
        logging.info(f"Executing {query_type.upper()} query")
        
        if query_type in ['select', 'ask']:
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            return results
                
        elif query_type in ['construct', 'describe']:
            if output_format == 'turtle':
                sparql.setReturnFormat(TURTLE)
                results = sparql.query().convert()
                return Response(results, mimetype="text/turtle")
            elif output_format == 'json':
                # For CONSTRUCT/DESCRIBE, we need to parse the graph and return JSON-LD or custom JSON
                sparql.setReturnFormat(TURTLE)
                turtle_data = sparql.query().convert()
                
                # Parse into RDFLib graph and serialize as JSON-LD
                from rdflib import Graph
                g = Graph()
                g.parse(data=turtle_data, format='turtle')
                
                # Serialize as JSON-LD
                json_ld = g.serialize(format='json-ld')
                return Response(json_ld, mimetype="application/ld+json")
            else:
                # Default to table format - return as JSON with bindings
                sparql.setReturnFormat(JSON)
                results = sparql.query().convert()
                return results
            
    except Exception as e:
        logging.error(f"SPARQL query error: {e}")
        return {"error": str(e)}, 500

@app.route("/resource/<path:entity>")
def resource(entity):
    """Handle entity requests with content negotiation"""
    # Build entity URI
    entity_uri = f"<{BASE_URI}{entity}>"
    
    # Enhanced SPARQL query - add more image-related predicates
    construct_query = f"""
    PREFIX schema: <http://schema.org/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX tgprop: <http://tolkiengateway.semanticweb.org/property/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dbp: <http://dbpedia.org/property/>
    
    CONSTRUCT {{
        {entity_uri} ?p ?o .
        ?s ?p2 {entity_uri} .
        ?equivalent ?ep ?eo .
    }}
    WHERE {{
        {{
            {entity_uri} ?p ?o .
        }}
        UNION
        {{
            ?s ?p2 {entity_uri} .
        }}
        UNION
        {{
            {entity_uri} owl:sameAs ?equivalent .
            ?equivalent ?ep ?eo .
        }}
    }}
    """
    
    # Get all classes including superclasses
    classes_query = f"""
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    
    SELECT DISTINCT ?class WHERE {{
        {entity_uri} rdf:type ?directClass .
        ?directClass rdfs:subClassOf* ?class .
    }}
    """
    
    # Content negotiation - check query parameter first, then Accept header
    format_param = request.args.get('format', '').lower()
    accept_header = request.headers.get('Accept', 'text/html')
    
    # Return Turtle if requested via query parameter or Accept header
    if format_param == 'turtle' or 'text/turtle' in accept_header or 'application/rdf+xml' in accept_header:
        return serve_turtle(construct_query, entity)
    else:
        return serve_html(entity_uri, entity, construct_query, classes_query)

def serve_turtle(query, entity):
    """Return Turtle serialization"""
    sparql = SPARQLWrapper(FUSEKI_URL)
    sparql.setQuery(query)
    sparql.setReturnFormat(TURTLE)
    
    try:
        results = sparql.query().convert()
        
        if not results or len(results) == 0:
            return Response(
                f"# No data found for entity: {entity}\n",
                status=404,
                mimetype="text/turtle"
            )
        
        return Response(results, mimetype="text/turtle")
        
    except Exception as e:
        logging.error(f"Error querying entity {entity}: {e}")
        return Response(
            f"# Error querying entity: {str(e)}\n",
            status=500,
            mimetype="text/turtle"
        )

def serve_html(entity_uri, entity_name, construct_query, classes_query):
    """Return HTML representation"""
    sparql = SPARQLWrapper(FUSEKI_URL)
    sparql.setQuery(construct_query)
    sparql.setReturnFormat(TURTLE)
    
    try:
        turtle_data = sparql.query().convert()
        
        if not turtle_data or len(turtle_data) == 0:
            return render_template('404.html', entity_name=entity_name), 404
        
        # Parse Turtle into RDFLib graph
        g = Graph()
        g.parse(data=turtle_data, format='turtle')
        
        # Bind namespaces for better querying
        g.bind("schema", SCHEMA)
        g.bind("tgprop", TGPROP)
        g.bind("foaf", FOAF)
        
        # Check if entity actually exists
        entity_full_uri = f"{BASE_URI}{entity_name}"
        if len(list(g.triples((None, None, None)))) == 0:
            return render_template('404.html', entity_name=entity_name), 404
        
        # Get classes (with reasoning)
        classes = get_classes_with_reasoning(entity_uri, classes_query)
        
        # Extract label
        label = unquote(entity_name.replace('_', ' '))
        for obj in g.objects(subject=None, predicate=RDFS.label):
            if obj.language == 'en' or not obj.language:
                label = str(obj)
                break
        
        # Extract image - IMPROVED LOGIC
        image_url = get_image_url(g, entity_full_uri, entity_name)
                    
        # Get description
        description = None
        for obj in g.objects(subject=None, predicate=SCHEMA.description):
            description = str(obj)
            break
        
        # Get all properties
        direct_properties = []
        inverse_properties = []
        same_as_links = []
        
        for s, p, o in g.triples((None, None, None)):
            prop_name = format_uri(str(p))
            
            if str(s) == entity_full_uri:
                if 'sameAs' in str(p):
                    same_as_links.append({
                        'property': prop_name,
                        'value': format_value(o, g)
                    })
                elif 'label' not in str(p) and 'type' not in str(p):
                    direct_properties.append({
                        'property': prop_name,
                        'value': format_value(o, g)
                    })
            else:
                inverse_properties.append({
                    'subject': format_value(s, g),
                    'property': prop_name
                })
        
        return render_template('entity.html', 
                             entity_name=label,
                             entity_path=entity_name,
                             entity_uri=entity_full_uri,
                             description=description,
                             classes=classes,
                             image_url=image_url,
                             properties=direct_properties,
                             inverse_properties=inverse_properties,
                             same_as_links=same_as_links)
        
    except Exception as e:
        logging.error(f"Error rendering HTML for {entity_name}: {e}")
        return f"<h1>Error</h1><p>{str(e)}</p>", 500

def get_image_url(graph, entity_uri, entity_name):
    """
    Extract image URL from graph with multiple fallback strategies
    """
    DBP = Namespace("http://dbpedia.org/property/")
    DBO = Namespace("http://dbpedia.org/ontology/")
    
    # Strategy 1: Check standard image predicates in RDF
    image_predicates = [
        SCHEMA.image,
        FOAF.img, 
        FOAF.depiction,
        DBP.image,
        DBO.thumbnail
    ]
    
    for pred in image_predicates:
        for obj in graph.objects(subject=None, predicate=pred):
            # Handle both URIs and Literals (filenames)
            url = str(obj)
            logging.info(f"Found image via {pred}: {url} (type: {type(obj).__name__})")
            
            # The image might be a Literal with language tag
            # Extract just the string value
            if hasattr(obj, 'value'):
                url = str(obj.value)
            
            return construct_image_url(url)
    
    # Strategy 2: Check owl:sameAs links for images
    OWL = Namespace("http://www.w3.org/2002/07/owl#")
    for same_as in graph.objects(subject=None, predicate=OWL.sameAs):
        for pred in image_predicates:
            for obj in graph.objects(subject=same_as, predicate=pred):
                url = str(obj)
                if hasattr(obj, 'value'):
                    url = str(obj.value)
                logging.info(f"Found image via sameAs link {same_as}: {url}")
                return construct_image_url(url)
    
    # Strategy 3: Dynamically fetch from Tolkien Gateway wiki page
    try:
        wiki_image = fetch_wiki_image(entity_name)
        if wiki_image:
            logging.info(f"Fetched image from wiki for {entity_name}: {wiki_image}")
            return wiki_image
    except Exception as e:
        logging.warning(f"Could not fetch wiki image for {entity_name}: {e}")
    
    return None

def fetch_wiki_image(entity_name):
    """
    Fetch image URL from Tolkien Gateway wiki page
    """
    import requests
    from bs4 import BeautifulSoup
    
    wiki_url = f"https://tolkiengateway.net/wiki/{entity_name}"
    
    try:
        response = requests.get(wiki_url, timeout=3)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for image in infobox
        infobox = soup.find('table', class_='infobox')
        if infobox:
            img = infobox.find('img')
            if img and img.get('src'):
                img_src = img['src']
                if img_src.startswith('//'):
                    return 'https:' + img_src
                elif img_src.startswith('/'):
                    return 'https://tolkiengateway.net' + img_src
                return img_src
        
        # Look for first substantial image
        content = soup.find('div', id='mw-content-text')
        if content:
            for img in content.find_all('img'):
                width = img.get('width', '0')
                try:
                    if int(width) > 100:
                        img_src = img['src']
                        if img_src.startswith('//'):
                            return 'https:' + img_src
                        elif img_src.startswith('/'):
                            return 'https://tolkiengateway.net' + img_src
                        return img_src
                except ValueError:
                    continue
        
        return None
        
    except Exception as e:
        logging.error(f"Error fetching wiki page {wiki_url}: {e}")
        return None

def construct_image_url(image_value):
    """
    Convert image value to a proper URL
    """
    # If already a full URL, return it
    if image_value.startswith('http://') or image_value.startswith('https://'):
        return image_value
    
    # If it's a filename (stored in RDF as just the filename)
    # We need to construct the full Tolkien Gateway image URL
    
    # The image filename might have URL encoding or special characters
    # Tolkien Gateway uses MediaWiki which stores images in /w/images/ with specific naming
    
    # Clean the filename - remove any quotes or whitespace
    filename = image_value.strip().strip('"')
    
    # URL encode the filename properly for MediaWiki
    # MediaWiki converts spaces to underscores and encodes special chars
    from urllib.parse import quote
    
    # Replace spaces with underscores (MediaWiki convention)
    filename = filename.replace(' ', '_')
    
    # Encode the filename but keep underscores, hyphens, dots
    encoded_filename = quote(filename, safe='-_./')
    
    # Construct the full URL
    # MediaWiki stores images in subdirectories based on MD5 hash
    # But we can try the direct path first which often works
    full_url = f"https://tolkiengateway.net/w/images/{encoded_filename}"
    
    logging.info(f"Constructed image URL: {full_url}")
    return full_url

def get_classes_with_reasoning(entity_uri, classes_query):
    """Get all classes including superclasses"""
    sparql = SPARQLWrapper(FUSEKI_URL)
    sparql.setQuery(classes_query)
    sparql.setReturnFormat(JSON)
    
    try:
        results = sparql.query().convert()
        classes = []
        for result in results["results"]["bindings"]:
            class_uri = result["class"]["value"]
            classes.append(format_uri(class_uri))
        return classes
    except Exception as e:
        logging.error(f"Error getting classes: {e}")
        return []

def format_uri(uri):
    """Extract readable name from URI and decode URL encoding"""
    if '#' in uri:
        name = uri.split('#')[-1].replace('_', ' ')
    else:
        name = uri.split('/')[-1].replace('_', ' ')
    return unquote(name)

def format_value(value, graph):
    """Format RDF value for HTML display"""
    value_str = str(value)
    
    # If it's a URI, make it clickable
    if value_str.startswith('http://'):
        display_name = format_uri(value_str)
        
        # Check if it's an internal resource
        if BASE_URI in value_str:
            entity_name = value_str.replace(BASE_URI, '')
            return f'<a href="/resource/{entity_name}">{display_name}</a>'
        else:
            return f'<a href="{value_str}" target="_blank">{display_name}</a> 🔗'
    
    # Handle language tags
    if hasattr(value, 'language') and value.language:
        return f'{value_str} <span class="lang-tag">@{value.language}</span>'
    
    return value_str

@app.errorhandler(404)
def not_found(e):
    return render_template('404.html', entity_name="Unknown"), 404

@app.errorhandler(500)
def server_error(e):
    return "<h1>500 Internal Server Error</h1><p>Something went wrong.</p>", 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)