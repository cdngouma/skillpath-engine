import yaml

def get_noc_lookup(yaml_path: str) -> dict:
    """
    Loads role mapping from YAML and inverts it for fast lookup.
    Returns: { "Search Term": "NOC Code" }
    """
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    lookup = {}
    # Access the specific 'role_mapping' key in your YAML
    for noc, terms in config.get('role_mapping', {}).items():
        for term in terms:
            clean_term = term.replace('"', '').strip()
            lookup[clean_term] = str(noc)
    return lookup