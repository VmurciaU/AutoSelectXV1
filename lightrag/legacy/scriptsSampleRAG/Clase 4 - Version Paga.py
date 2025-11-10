import os
from langchain_chroma import Chroma
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate


from dotenv import load_dotenv
import os

# Cargar variables del archivo .env
load_dotenv()

# Leer la API key desde el entorno
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")


llm = ChatOpenAI(model='gpt-3.5-turbo-0125', temperature=0)
chroma_local = Chroma(persist_directory="./vectordb", embedding_function=OpenAIEmbeddings())
    

def prompt(texto):
    system_prompt = (
    texto+
    "\n\n"
    "{context}"
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            ("human", "{input}"),
        ])
    return prompt


def respuesta(pregunta, llm, chroma_db, prompt):
    retriever = chroma_db.as_retriever()

    chain = create_stuff_documents_chain(llm, prompt)
    rag = create_retrieval_chain(retriever, chain)
    
    results = rag.invoke({"input": pregunta})
    return results



texto = """Tú eres un asistente para tareas de respuesta a preguntas."
    "Usa los siguientes fragmentos de contexto recuperado para responder "
    "la pregunta. Si no sabes la respuesta, di que no "
    "sabes. Usa un máximo de tres oraciones y mantén la "
    "respuesta concisa."""
    
    

if __name__ == '__main__':
    print(respuesta(input('Hacé tu pregunta: '), llm, chroma_local, prompt(texto))['answer'])