from bs4 import BeautifulSoup, NavigableString, Tag
import re

# Header fragments / concepts, not full exact titles
SKILL_HEADER_PATTERNS = [
    r"\brequirements?\b",
    r"\bqualifications?\b",
    r"\brequired qualifications?\b",
    r"\bminimum qualifications?\b",
    r"\bpreferred qualifications?\b",
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
    r"\bwhat you need to have\b",
    r"\bour ideal candidate\b",
    r"\byou('?re| are) our ideal candidate\b",
    r"\bideal candidate\b",
    r"\bhow you will succeed\b",
    r"\byour responsibilities\b",
    r"\btechnical expertise\b",
]

HEADER_RX = re.compile("|".join(SKILL_HEADER_PATTERNS), re.IGNORECASE)

BULLET_LINE_RX = re.compile(r"^\s*(?:•|‣|◦|\*|-)\s+")

STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with",
    "by", "at", "from", "as", "is", "are", "be", "will", "you", "your",
    "our", "we", "their", "they", "this", "that", "these", "those"
}


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"&amp;", "&", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_stopwords(text: str) -> str:
    words = re.findall(r"\b[\w.+#/-]+\b|[^\w\s]", text, flags=re.UNICODE)
    cleaned = []
    for w in words:
        if re.match(r"\b[\w.+#/-]+\b", w):
            if w.lower() not in STOPWORDS:
                cleaned.append(w)
        else:
            cleaned.append(w)

    out = " ".join(cleaned)
    out = re.sub(r"\s+([,.;:])", r"\1", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _clean_item_text(text: str) -> str:
    text = _normalize_text(text)
    text = BULLET_LINE_RX.sub("", text)
    return _strip_stopwords(text)


def _text_matches_header(text: str) -> bool:
    text = _normalize_text(text)
    return bool(text and HEADER_RX.search(text))


def _extract_text_from_candidate(node) -> str:
    if node is None:
        return ""

    if isinstance(node, NavigableString):
        return _normalize_text(str(node))

    if isinstance(node, Tag):
        return _normalize_text(node.get_text(" ", strip=True))

    return ""


def _nearest_preceding_text(list_tag: Tag, max_steps: int = 5) -> str:
    """
    Look at nearby previous siblings first.
    This is more precise than a broad find_previous().
    """
    current = list_tag
    steps = 0

    while steps < max_steps:
        current = current.previous_sibling
        if current is None:
            break

        text = _extract_text_from_candidate(current)
        if text:
            return text

        steps += 1

    # fallback: broader structural search upward
    prev = list_tag.find_previous(["p", "div", "strong", "b", "h1", "h2", "h3", "h4"])
    return _extract_text_from_candidate(prev)


def _extract_list_items(list_tag: Tag) -> list[str]:
    items = []
    for li in list_tag.find_all("li"):
        txt = _clean_item_text(li.get_text(" ", strip=True))
        if txt:
            items.append(txt)
    return items


def _extract_all_lists(soup: BeautifulSoup) -> list[str]:
    items = []
    for list_tag in soup.find_all(["ul", "ol"]):
        items.extend(_extract_list_items(list_tag))
    return items


def _extract_bullet_points_with_context(soup: BeautifulSoup) -> list[str]:
    extracted = []

    for element in soup.find_all(["p", "div"]):
        text = _normalize_text(element.get_text(" ", strip=True))
        if not text or not BULLET_LINE_RX.match(text):
            continue

        context = ""
        sibling = element.previous_sibling
        hops = 0
        while sibling is not None and hops < 5:
            candidate = _extract_text_from_candidate(sibling)
            if candidate and not BULLET_LINE_RX.match(candidate):
                context = candidate
                break
            sibling = getattr(sibling, "previous_sibling", None)
            hops += 1

        if not context:
            prev = element.find_previous(["p", "div", "strong", "b", "h1", "h2", "h3", "h4"])
            context = _extract_text_from_candidate(prev)

        if _text_matches_header(context):
            cleaned = _clean_item_text(text)
            if cleaned:
                extracted.append(cleaned)

    return extracted


def _extract_all_bullet_points(soup: BeautifulSoup) -> list[str]:
    text = soup.get_text("\n", strip=True)
    if not text:
        return []

    items = []

    for line in text.split("\n"):
        line = _normalize_text(line)
        
        if not line or not BULLET_LINE_RX.match(line):
            continue
        
        clean = BULLET_LINE_RX.sub("", line).strip()
        
        if clean:
            items.append(clean)
    
    return items


def extract_skills_section(html_content: str) -> str:
    """
    Strategy:
    1. Find UL/OL lists whose nearest preceding text matches a skill/requirements header.
    2. If none found, return all UL/OL text content.
    3. If no lists exist, use visual bullet-point fallback with contextual header matching.
    4. If still nothing, return full text content.
    5. Remove stopwords before returning.
    """
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, "html.parser")

    # 1) Targeted list extraction
    extracted = []
    for list_tag in soup.find_all(["ul", "ol"]):
        context_text = _nearest_preceding_text(list_tag)
        if _text_matches_header(context_text):
            extracted.extend(_extract_list_items(list_tag))

    if extracted:
        return "\n".join(extracted).strip()

    # 2) Fallback: all UL/OL items
    all_list_items = _extract_all_lists(soup)
    if all_list_items:
        return "\n".join(all_list_items).strip()

    # 3) Fallback: visual bullet points with contextual header matching
    bullet_items = _extract_bullet_points_with_context(soup)
    if bullet_items:
        return "\n".join(bullet_items).strip()

    # 4) Fallback: all bullet points
    all_bullet_points = _extract_all_bullet_points(soup)
    if all_bullet_points:
        return "\n".join(all_bullet_points).strip()

    # 5) Final fallback: full text content
    full_text = _normalize_text(soup.get_text("\n", strip=True))
    return _strip_stopwords(full_text)