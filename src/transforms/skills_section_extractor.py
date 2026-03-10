import re

SKILL_SECTION_HEADERS = [
    # Most common in job posts (top → lower frequency)
    "requirements",
    "qualifications",
    "what you'll bring",
    "what you will bring",
    "what we're looking for",
    "what we are looking for",
    "what you need",
    "what you bring",
    "skills",
    "technical skills",
    "core experience",
    "must have",
    "nice to have",
    "preferred qualifications",
    "preferred skills",
    "bonus points",
    "our tech stack",
    "who we're looking for",
    "who we are looking for",
    "how to be successful",
]

# Headers that often indicate the start of a NEW section (end extraction here).
STOP_SECTION_HEADERS = [
    "how we hire",
    "additional job details",
    "learn more",
    "why join",
    "equal opportunity",
    "eeoc",
    "eoe",
    "benefits",
    "about us",
    "who we are",
    "company",
    "our values",
    "how we work",
    "compensation",
    "salary",
    "privacy",
    "our team culture"
]

APOS = r"['’‘ʼ]"  # common apostrophe variants
LETTER = r"A-Za-zÀ-ÖØ-öø-ÿ"

HEADER_LINE_PATTERN = (
    r"(?m)^\s*"
    r"(?:[" + LETTER + r"& ]|" + APOS + r")*?"
    r"\b({alts})\b"
    r"(?:[" + LETTER + r"& ]|" + APOS + r"|:)*"
    r"\s*$" 
)

header_regex = re.compile(
    HEADER_LINE_PATTERN.format(alts="|".join(re.escape(h) for h in SKILL_SECTION_HEADERS)),
    flags=re.IGNORECASE,
)

# Stop headers: must appear at beginning of the line.
# Allow trailing header text but keep it "header-like".
stop_header_regex = re.compile(
    r"(?mi)^\s*(?:"
    + "|".join(re.escape(h) for h in STOP_SECTION_HEADERS)
    + r")\b[^\n]*$"
)

# Generic fallback: a line that looks like a section header (single line, 2–60 chars)
# (We use case-insensitive and don't require caps to avoid Task Manager / lowercasing issues.)
generic_next_header_regex = re.compile(
    r"(?m)^\s*[" + LETTER + r"][" + LETTER + r" &" + APOS + r"]{1,60}\s*$"
)


def _find_earliest_match(rx: re.Pattern, text):
    """Return the leftmost match among all matches, or None."""
    text_norm = text.replace("\u00A0", " ")
    matches = list(rx.finditer(text_norm))
    if not matches:
        return None
    return min(matches, key=lambda m: m.start())


def extract_skills_section(text):
    if not text:
        return text

    # 1) Find earliest skill header occurrence among ALL possible header matches
    match = _find_earliest_match(header_regex, text)
    if not match:
        return text

    start = match.end()

    tail = text[start:]

    # 2) Determine end:
    #   a) First: explicit STOP headers at beginning-of-line
    stop = _find_earliest_match(stop_header_regex, tail)

    #   b) Else: generic header-looking line
    generic = _find_earliest_match(generic_next_header_regex, tail)

    # Choose earliest end if present
    candidates = [m for m in [stop, generic] if m is not None]
    if candidates:
        end = start + min(candidates, key=lambda m: m.start()).start()
    else:
        end = len(text)

    return text[start:end].strip()