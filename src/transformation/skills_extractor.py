import instructor
from pydantic import BaseModel, Field, ConfigDict
from typing import List
from ollama import Client
from pathlib import Path

PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "tech_skills_extraction.txt"

class TechRequirement(BaseModel):
    skill_name: str = Field(description="The name of the technology, e.g., 'React' or 'Kubernetes'")
    category: str = Field(description="One of: Programming Language, Framework, Cloud/Infra, Database, Tool")

class JobSkills(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    technical_skills: List[TechRequirement] = Field(default_factory=list, validation_alias="TechRequirement", description="List of extracted technical skills")

client = instructor.from_provider(
    "ollama/gemma3:4b", 
    base_url="http://localhost:11434/v1"
)

def extract_tech_skills(description):
    system_prompt = PROMPT_PATH.read_text(encoding="utf-8").strip()
    return client.chat.completions.create(
        model="gemma3:4b",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": description}
        ],
        response_model=JobSkills,
    )