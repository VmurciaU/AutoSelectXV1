# raggrafo/scripts/ask_case_cli.py
# -*- coding: utf-8 -*-

"""
CLI para hacer preguntas al RAG de un caso.

Uso:

  python -m raggrafo.scripts.ask_case_cli --case-id 14 \
      --question "¿Cuáles son las condiciones de diseño del paquete de inyección de químicos?"

O modo interactivo:

  python -m raggrafo.scripts.ask_case_cli --case-id 14
  (luego te pide que escribas la pregunta)
"""

from __future__ import annotations

import argparse
from typing import List

# Import correcto: el módulo está en raggrafo/pipelines/rag_case_query.py
from raggrafo.pipelines.rag_case_query import ask_rag


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Hacer una pregunta al RAG/KG de un caso."
    )
    parser.add_argument(
        "--case-id",
        type=int,
        required=True,
        help="ID del caso (ej. 14)",
    )
    parser.add_argument(
        "--question",
        type=str,
        default=None,
        help="Pregunta a realizar. Si se omite, se solicitará por consola.",
    )

    args = parser.parse_args(argv)

    case_id = args.case_id
    question = args.question

    if not question:
        try:
            question = input(
                f"[ASK] Escribe tu pregunta para el caso {case_id}: "
            ).strip()
        except KeyboardInterrupt:
            print("\n[ASK] Cancelado por el usuario.")
            return

    if not question:
        print("[ASK] No se proporcionó ninguna pregunta.")
        return

    print(f"[ASK] case_id={case_id}")
    print(f"[ASK] pregunta: {question}\n")

    # Delegamos en tu función de RAG
    answer = ask_rag(case_id=case_id, question=question)

    print("────────────────────────────────────────")
    print("Respuesta:")
    print(answer)
    print("────────────────────────────────────────")


if __name__ == "__main__":
    main()
