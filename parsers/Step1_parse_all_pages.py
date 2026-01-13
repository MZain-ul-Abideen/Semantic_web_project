#!/usr/bin/env python3
"""
Batch parser for all Tolkien Gateway pages
Processes json_pages.json and normalizes all entities
"""

import json
import sys
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict

# Import InfoboxParser - will be defined below
# (Inline to avoid import issues)

import re
import mwparserfromhell


class InfoboxParser:
    """Parser for MediaWiki infobox templates - INLINE VERSION"""
    
    def __init__(self):
        self.key_normalizations = {
            'birth_date': 'birthDate', 'birth_place': 'birthPlace',
            'death_date': 'deathDate', 'death_place': 'deathPlace',
            'real_name': 'realName', 'other_names': 'otherNames',
            'pronunciation': 'pronunciation', 'sail_date': 'sailDate',
            'sail_ship': 'sailShip', 'lifespan': 'lifespan',
            'realm': 'realm', 'title': 'title', 'titles': 'titles',
            'position': 'position', 'affiliation': 'affiliation',
            'language': 'language', 'height': 'height',
            'hair': 'hair', 'eyes': 'eyes', 'clothing': 'clothing',
            'weapons': 'weapons', 'steed': 'steed', 'gallery': 'gallery',
            'actor': 'actor', 'voice': 'voice', 'culture': 'culture',
            'gender': 'gender', 'race': 'race', 'spouse': 'spouse',
            'children': 'children', 'parentage': 'parentage',
            'siblings': 'siblings', 'physical_description': 'physicalDescription',
            'notable_for': 'notableFor',
        }
    
    def normalize_key(self, key: str) -> str:
        key = key.strip().lower().replace(' ', '_')
        if key in self.key_normalizations:
            return self.key_normalizations[key]
        parts = key.split('_')
        if len(parts) > 1:
            return parts[0] + ''.join(word.capitalize() for word in parts[1:])
        return key
    
    def extract_internal_links(self, text: str) -> List[str]:
        links = []
        pattern = r'\[\[([^\]|]+)(?:\|[^\]]+)?\]\]'
        matches = re.findall(pattern, text)
        for match in matches:
            link = match.strip()
            if ':' in link and not link.startswith('http'):
                continue
            links.append(link)
        return links
    
    def clean_wikitext_value(self, value: str) -> str:
        if not isinstance(value, str):
            return str(value)
        try:
            wikicode = mwparserfromhell.parse(value)
            cleaned = wikicode.strip_code()
            cleaned = re.sub(r'<ref[^>]*>.*?</ref>', '', cleaned, flags=re.DOTALL)
            cleaned = re.sub(r'<ref[^>]*/?>', '', cleaned)
            cleaned = re.sub(r'\s+', ' ', cleaned)
            return cleaned.strip()
        except:
            return value.strip()
    
    def parse_infobox_template(self, wikitext: str):
        try:
            wikicode = mwparserfromhell.parse(wikitext)
            templates = wikicode.filter_templates()
            infobox_template = None
            for template in templates:
                if str(template.name).strip().lower().startswith('infobox'):
                    infobox_template = template
                    break
            if not infobox_template:
                return None
            template_name = str(infobox_template.name).strip()
            params = {}
            for param in infobox_template.params:
                key = str(param.name).strip()
                value = str(param.value).strip()
                if not value:
                    continue
                normalized_key = self.normalize_key(key)
                params[normalized_key] = {
                    'raw': value,
                    'cleaned': self.clean_wikitext_value(value),
                    'internal_links': self.extract_internal_links(value)
                }
            return {
                'template_name': template_name,
                'template_type': template_name.replace('Infobox ', '').strip(),
                'parameters': params
            }
        except:
            return None
    
    def extract_all_internal_links(self, wikitext: str) -> List[str]:
        try:
            wikicode = mwparserfromhell.parse(wikitext)
            wikilinks = wikicode.filter_wikilinks()
            links = []
            for link in wikilinks:
                title = str(link.title).strip()
                if ':' in title and not title.startswith('http'):
                    continue
                links.append(title)
            return list(set(links))
        except:
            return []
    
    def parse_page(self, page_data: Dict[str, Any]) -> Dict[str, Any]:
        title = page_data.get('title', 'Unknown')
        if 'json' in page_data:
            page_data = page_data['json']
        if title == 'Unknown':
            title = page_data.get('title', page_data.get('parse', {}).get('title', 'Unknown'))
        wikitext = None
        if 'wikitext' in page_data:
            wt = page_data['wikitext']
            wikitext = wt.get('*', wt) if isinstance(wt, dict) else wt
        elif 'parse' in page_data:
            parse_obj = page_data['parse']
            if 'wikitext' in parse_obj:
                wt = parse_obj['wikitext']
                wikitext = wt.get('*', wt) if isinstance(wt, dict) else wt
        elif 'text' in page_data:
            wikitext = page_data['text']
        if not wikitext:
            return None
        infobox = self.parse_infobox_template(wikitext)
        all_links = self.extract_all_internal_links(wikitext)
        pageid = page_data.get('pageid', page_data.get('parse', {}).get('pageid'))
        entity = {
            'entity_id': title,
            'page_id': pageid,
            'title': title,
            'type': None,
            'infobox': None,
            'internal_links': all_links,
            'categories': page_data.get('categories', page_data.get('parse', {}).get('categories', [])),
            'raw_wikitext_length': len(wikitext)
        }
        if infobox:
            entity['type'] = infobox['template_type']
            entity['infobox'] = {
                'template_name': infobox['template_name'],
                'parameters': infobox['parameters']
            }
        return entity


def parse_all_pages(input_file: Path, output_file: Path) -> Dict[str, Any]:
    """
    Parse all pages from json_pages.json
    
    Args:
        input_file: Path to json_pages.json
        output_file: Path to save normalized entities
    
    Returns:
        Statistics about the parsing process
    """
    parser = InfoboxParser()
    
    # Read all pages
    print(f"Reading pages from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        pages_data = json.load(f)
    
    # Handle different possible structures
    if isinstance(pages_data, dict):
        # If it's a dict with a key like 'pages', extract that
        if 'pages' in pages_data:
            pages_list = pages_data['pages']
        else:
            # Single page
            pages_list = [pages_data]
    else:
        # Already a list
        pages_list = pages_data
    
    print(f"Found {len(pages_list)} pages to process")
    
    # Statistics tracking
    stats = {
        'total_pages': len(pages_list),
        'successful': 0,
        'failed': 0,
        'with_infobox': 0,
        'without_infobox': 0,
        'infobox_types': defaultdict(int),
        'failed_pages': []
    }
    
    # Parse all pages
    normalized_entities = []
    
    for i, page_data in enumerate(pages_list):
        title = page_data.get('title', f'Unknown_{i}')
        
        # Progress indicator
        if (i + 1) % 100 == 0 or (i + 1) == len(pages_list):
            print(f"Processing {i + 1}/{len(pages_list)}: {title}")
        
        try:
            entity = parser.parse_page(page_data)
            
            if entity:
                normalized_entities.append(entity)
                stats['successful'] += 1
                
                # Track infobox statistics
                if entity.get('infobox'):
                    stats['with_infobox'] += 1
                    infobox_type = entity['type']
                    stats['infobox_types'][infobox_type] += 1
                else:
                    stats['without_infobox'] += 1
            else:
                stats['failed'] += 1
                stats['failed_pages'].append(title)
                
        except Exception as e:
            print(f"ERROR processing {title}: {e}")
            stats['failed'] += 1
            stats['failed_pages'].append(title)
    
    # Save normalized entities
    print(f"\nSaving {len(normalized_entities)} entities to {output_file}...")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_entities, f, indent=2, ensure_ascii=False)
    
    # Convert defaultdict to regular dict for JSON serialization
    stats['infobox_types'] = dict(stats['infobox_types'])
    
    return stats


def save_statistics(stats: Dict[str, Any], stats_file: Path):
    """Save parsing statistics to a file"""
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    
    # Also print to console
    print("\n" + "="*60)
    print("PARSING STATISTICS")
    print("="*60)
    print(f"Total pages: {stats['total_pages']}")
    print(f"Successfully parsed: {stats['successful']}")
    print(f"Failed: {stats['failed']}")
    print(f"\nWith infobox: {stats['with_infobox']}")
    print(f"Without infobox: {stats['without_infobox']}")
    print(f"\nInfobox types found:")
    for infobox_type, count in sorted(stats['infobox_types'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {infobox_type}: {count}")
    
    if stats['failed_pages']:
        print(f"\nFirst 10 failed pages:")
        for page in stats['failed_pages'][:10]:
            print(f"  - {page}")


def create_entity_index(normalized_entities: List[Dict], index_file: Path):
    """
    Create an index of entities by type for easy lookup
    """
    index = defaultdict(list)
    
    for entity in normalized_entities:
        entity_type = entity.get('type', 'unknown')
        index[entity_type].append({
            'entity_id': entity['entity_id'],
            'title': entity['title'],
            'page_id': entity.get('page_id')
        })
    
    # Save index
    index_file.parent.mkdir(parents=True, exist_ok=True)
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(dict(index), f, indent=2, ensure_ascii=False)
    
    print(f"\nEntity index saved to {index_file}")


def main():
    """Main execution"""
    # File paths
    input_file = Path('./data/raw/json_pages.json')
    output_file = Path('./data/normalized/normalized_entities.json')
    stats_file = Path('./data/normalized/parsing_statistics.json')
    index_file = Path('./data/normalized/entity_index.json')
    
    # Check input file exists
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        print("Make sure json_pages.json exists in ./data/raw/")
        return
    
    # Parse all pages
    print("Starting batch parsing...\n")
    stats = parse_all_pages(input_file, output_file)
    
    # Save statistics
    save_statistics(stats, stats_file)
    
    # Load normalized entities and create index
    print("\nCreating entity index...")
    with open(output_file, 'r', encoding='utf-8') as f:
        normalized_entities = json.load(f)
    
    create_entity_index(normalized_entities, index_file)
    
    print("\n" + "="*60)
    print("PARSING COMPLETE!")
    print("="*60)
    print(f"Normalized entities: {output_file}")
    print(f"Statistics: {stats_file}")
    print(f"Entity index: {index_file}")


if __name__ == '__main__':
    main()