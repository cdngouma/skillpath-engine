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

def cip_to_noc(cip):
    """
    Maps Classification of Instructional Programs (CIP) to primary tech NOCs.
    Focuses on 11.xx (Computer Science) and 27.xx (Math/Stats).
    """
    cip_str = str(cip).lower().strip()
    if "computer science" in cip_str or "11." in cip_str:
        return "2122"
    elif "mathematics" in cip_str or "statistics" in cip_str or "27." in cip_str:
        return "2121"
    return "Unknown"

def occupation_to_noc(occupation):
    """
    Maps an official occupation title or broad category to its 5-digit NOC 2021 code.
    This is a strict mapper with no heuristic fallback.
    """
    if not occupation:
        return None

    occ_clean = str(occupation).strip().lower()

    official_map = {
        # Broad & Sub-Major Categories
        "professional occupations in applied sciences (except engineering)": "212",
        "mathematicians, statisticians, actuaries and data scientists": "2121",
        "computer and information systems professionals": "2122",
        # Unit Groups (5-digit)
        "computer and information systems managers": "20012",
        "mathematicians, statisticians and actuaries": "21210",
        "data scientists": "21211",
        "cybersecurity specialists": "21220",
        "business systems specialists": "21221",
        "information systems specialists": "21222",
        "database analysts and data administrators": "21223",
        "computer systems developers and programmers": "21230",
        "software engineers and designers": "21231",
        "software developers and programmers": "21232",
        "web designers": "21233",
        "web developers and programmers": "21234"
    }
    
    return official_map.get(occ_clean, "Unknown")