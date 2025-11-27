# raggrafo/scripts/rag_service.py

from raggrafo.pipelines.rag_case_query_finetune import run_case_query_finetune

async def run_rag_case(
    case_id: int,
    question: str,
    mode: str = "engineering",
):
    """Wrapper oficial para consultas RAG desde el chat."""
    
    # Llamada directa al pipeline finetune
    r = await run_case_query_finetune(
        case_id=case_id,
        mode=mode,
        question=question,
    )

    return {
        "answer": r.get("final") or "",
        "error": r.get("error")
    }
