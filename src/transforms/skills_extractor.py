from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from typing import List
from pathlib import Path

# Keep your existing Pydantic models
class TechRequirement(BaseModel):
    skill_name: str = Field(description="The canonical name of the extracted hard technical skill")

class JobSkills(BaseModel):
    technical_skills: List[TechRequirement] = Field(default_factory=list)

# Load System Prompt
PROMPT_PATH = Path(__file__).resolve().parents[1] / "prompts" / "tech_skills_extraction.txt"
SYSTEM_PROMPT = PROMPT_PATH.read_text(encoding="utf-8").strip()

# Initialize LangChain Model
llm = ChatOllama(
    model="gemma3:4b",
    temperature=0,
    format="json",
    base_url="http://localhost:11434"
).with_structured_output(JobSkills)

# Define the Template
prompt_template = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_PROMPT),
    ("user", "<<<<>>>>\n{description}\n<<<<>>>>")
])

# Define the function using a Chain
def extract_tech_skills(description: str) -> JobSkills:
    chain = prompt_template | llm
    return chain.invoke({"description": description})