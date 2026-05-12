from __future__ import annotations

from typing import Optional

import instructor
from pydantic import BaseModel, Field, field_validator


MODEL_NAME = "ollama/gemma3:4b"

PLACEHOLDER_VALUES = {
    "none",
    "n/a",
    "na",
    "not specified",
    "not mentioned",
    "not provided",
    "unknown",
    "null",
}


class JobRequirements(BaseModel):
    technical_tools: list[str] = Field(
        default_factory=list,
        description="Named software, programming languages, frameworks, libraries, platforms, and cloud tools.",
    )
    technical_concepts: list[str] = Field(
        default_factory=list,
        description="Conceptual domains, methodologies, and technical areas.",
    )
    min_years: Optional[int] = Field(
        default=None,
        description="Minimum years of experience required.",
    )
    max_years: Optional[int] = Field(
        default=None,
        description="Maximum years of experience required.",
    )
    certifications: list[str] = Field(
        default_factory=list,
        description="Certifications.",
    )

    @field_validator(
        "technical_tools",
        "technical_concepts",
        "certifications",
        mode="before",
    )
    @classmethod
    def clean_list_fields(cls, value):
        if value is None:
            return []

        if isinstance(value, list):
            return [
                item for item in value
                if item is not None and str(item).strip().lower() not in {"none", "null", "n/a", ""}
            ]
        
        return value


SYSTEM_PROMPT = """
Extract job requirements from the text.

Return valid JSON only with exactly these keys:
- technical_tools
- technical_concepts
- min_years
- max_years
- certifications

Rules:
- technical_tools: named software, programming languages, frameworks, libraries, platforms, and cloud tools.
- technical_concepts: non-tool technical domains, methods, and practices such as machine learning, data modeling, ETL, data governance, MLOps, statistics, experimentation, NLP, computer vision, API design, or distributed systems. Do not include software tools, programming languages, cloud platforms, or generic job responsibilities.
- min_years and max_years must be integers or null.
- certifications must be a list of explicitly stated certifications.
- Use empty lists for missing list fields.
- Use null for missing year fields.
- Never return ["None"], ["N/A"], ["not specified"], ["not mentioned"], or similar placeholder strings.
- If no certification is stated, return [].
- Do not invent requirements not present in the text.
- Do not include explanations.

Before answering, verify that every extracted item appears in or is directly supported by the text.
""".strip()


def build_user_prompt(description: str) -> str:
    return f"""
Extract the structured job requirements from the text below.

<job_requirements_text>
{description}
</job_requirements_text>
""".strip()


def get_llm_client(model=None):
    model = model if model else MODEL_NAME
    return instructor.from_provider(model)


def extract_requirements(
    description: str,
    client = None,
) -> dict:
    if not description or not description.strip():
        return {
            "extraction_status": "skipped",
            "parsed_requirements": None,
            "error": "Missing requirement text",
        }

    client = client or get_llm_client()

    try:
        result = client.chat.completions.create(
            response_model=JobRequirements,
            max_retries=2,
            temperature=0,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_user_prompt(description)},
            ],
        )

        return {
            "extraction_status": "validated",
            "parsed_requirements": result.model_dump(),
            "error": None,
        }

    except Exception as e:
        return {
            "extraction_status": "failed",
            "parsed_requirements": None,
            "error": str(e),
        }