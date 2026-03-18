import instructor
from pydantic import BaseModel, Field
from typing import List, Optional
from pathlib import Path

PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "tech_skills_extraction.txt"
SYSTEM_PROMPT = PROMPT_PATH.read_text(encoding="utf-8").strip()

class TechRequirement(BaseModel):
    skill_name: str = Field(description="The canonical name of the technology")

class JobSkills(BaseModel):
    technical_skills: List[TechRequirement] = Field(default_factory=list)

client = instructor.from_provider(
    "ollama/gemma3:4b",
    base_url="http://localhost:11434/v1"
)

def extract_tech_skills(description: str) -> JobSkills:
    PROMPT = f"""
    Extract explicit hard technical skills from the job description below:

    ```
    {description}
    ```
    """
    return client.chat.completions.create(
        model="gemma3:4b",
        temperature=0,
        max_retries=2,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": PROMPT}
        ],
        response_model=JobSkills,
    )