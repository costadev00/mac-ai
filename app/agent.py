from langchain.llms import OpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

prompt = PromptTemplate(
    input_variables=["schema", "question"],
    template="""Você é um assistente que gera consultas SQL para PostgreSQL.\n\
Use apenas SELECT e sempre inclua LIMIT 100.\n\
Esquema relevante: {schema}\n\
Pergunta: {question}\nSQL:""",
)

llm = OpenAI(temperature=0, model_name="gpt-3.5-turbo")

chain = LLMChain(llm=llm, prompt=prompt)

def generate_sql(question: str, schema_context: str) -> str:
    sql = chain.run(schema=schema_context, question=question)
    sql = sql.strip().splitlines()[0]
    if sql.startswith("```"):
        sql = sql.strip("`")
    return sql
