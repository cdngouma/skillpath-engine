from bs4 import BeautifulSoup, NavigableString, Tag
import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Header fragments / concepts, not full exact titles
SKILL_HEADER_PATTERNS = [
    r"\brequirements?\b",
    r"\bqualifications?\b",
    r"\brequired qualifications?\b",
    r"\bminimum qualifications?\b",
    r"\bpreferred qualifications?\b",
    r"\bpreferred experience\b",
    r"\bother qualifications?\b",
    r"\bbasic qualifications?\b",
    r"\bmust[- ]?haves?\b",
    r"\bmust have\b",
    r"\bnice[- ]to[- ]have\b",
    r"\btechnical (skills|expertise)\b",
    r"\bskills?\b",
    r"\bcore engineering skills?\b",
    r"\bexperience\b",
    r"\bexperience\s*&\s*qualifications?\b",
    r"\bqualifications?\s*&\s*experience\b",
    r"\bwhat you('ll| will) bring\b",
    r"\bhere('?s| is) what you('ll| will) bring\b",
    r"\byou bring\b",
    r"\bwhat we are looking for\b",
    r"\bwhat we're looking for\b",
    r"\bwhat we looking for\b",
    r"\bwhat you need to have\b",
    r"\bwhat do we expect\b",
    r"\bour ideal candidate\b",
    r"\byou('?re| are) our ideal candidate\b",
    r"\bideal candidate\b",
    r"\bideal candidate for this position will have\b",
    r"\bwhat you may have\b",
    r"\bhow you will succeed\b",
    r"\byour responsibilities\b",
    r"\bwhat you've done\b",
    r"\bwho you are\b",
    r"\btechnical expertise\b",
    r"\bprerequisites\b"
]

HEADER_RX = re.compile("|".join(SKILL_HEADER_PATTERNS), re.IGNORECASE)

BULLET_LINE_RX = re.compile(r"^\s*(?:•|·|⁃|∙|‣|◦|\*|-|▪|■|\.|\d+[.)])\s*")


def _normalize_text(text: str) -> str:
    if not text: return ""
    # Standardize spaces and common HTML entities
    text = text.replace("\xa0", " ").replace("&amp;", "&")

    apostrophe_pattern = r"[\u2018\u2019\u201a\u201b\u00b4]"
    text = re.sub(apostrophe_pattern, "'", text)
    
    # Collapse multiple whitespaces but keep a single leading space if it exists
    # because our regex handles leading whitespace
    return re.sub(r"\s+", " ", text).strip()


def _nearest_preceding_text(element: Tag) -> str:
    curr = element.previous_sibling
    for _ in range(10):
        if not curr:
            break

        if isinstance(curr, Tag):
            if curr.name in ["ul", "ol"]:
                curr = curr.previous_sibling
                continue
            txt = _normalize_text(curr.get_text())
        else:
            txt = _normalize_text(str(curr))

        if txt:
            return txt

        curr = curr.previous_sibling

    prev = element.find_previous(["h1", "h2", "h3", "h4", "p", "strong", "b"])
    return _normalize_text(prev.get_text()) if prev else ""


def extract_requirements(html_content: str) -> str:
    if not html_content: return ""
    
    # Pre-process line breaks
    html_content = html_content.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    soup = BeautifulSoup(html_content, "html.parser")
    
    targeted_items = []
    all_items_fallback = []

    for tag in soup.find_all(["ul", "ol", "p", "div"]):
        is_real_list = tag.name in ["ul", "ol"]
        
        # Check for internal headers
        # Get the first bit of text in the tag to see if it's a header
        tag_text = tag.get_text("\n")
        first_line = tag_text.split("\n")[0]
        
        # Check context (external header) OR if the tag itself starts with a header
        external_context = _nearest_preceding_text(tag)
        is_skill_section = bool(HEADER_RX.search(external_context)) or bool(HEADER_RX.search(first_line))
        
        current_items = []
        
        if is_real_list:
            current_items = [_normalize_text(li.get_text()) for li in tag.find_all("li")]
        else:
            lines = tag_text.split("\n")
            for line in lines:
                clean_line = _normalize_text(line)
                if not clean_line: continue
                
                # If the line is the header itself, skip it but mark the section as targeted
                if HEADER_RX.search(clean_line) and not BULLET_LINE_RX.match(clean_line):
                    is_skill_section = True
                    continue

                if BULLET_LINE_RX.match(clean_line):
                    item_text = BULLET_LINE_RX.sub("", clean_line).strip()
                    current_items.append(item_text)
                elif current_items and is_skill_section:
                    # If no bullet, but we are in a skill section, append to previous item
                    # This fixes the "similar technologies" line in Case 1
                    current_items[-1] = f"{current_items[-1]} {clean_line}"
        
        current_items = [i for i in current_items if i]

        if is_skill_section:
            targeted_items.extend(current_items)
        else:
            all_items_fallback.extend(current_items)

    # Waterfall logic
    if targeted_items:
        return "\n".join(targeted_items).strip()
    if all_items_fallback:
        return "\n".join(all_items_fallback).strip()
    
    return _normalize_text(soup.get_text(" "))