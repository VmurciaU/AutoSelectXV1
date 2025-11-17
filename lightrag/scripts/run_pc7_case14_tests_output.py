# lightrag/scripts/run_pc7_case14_tests_output.py
# -*- coding: utf-8 -*-

from __future__ import annotations

from pathlib import Path

from pipelines.raggrafo_case_runner import run_raggrafo_for_case


def main() -> None:
    """
    Prueba PC7 (run_raggrafo_for_case) usando las salidas
    del pipeline de pruebas en lightrag/tests/output.
    """
    case_id = 14

    # Ojo: usamos tests/output porque el run_raggrafo_pipeline.py
    # que acabas de correr escribe allí, no en outputs/.
    root = Path(__file__).resolve().parents[1]  # .../lightrag
    pc2_dir = root / "tests" / "output" / "pc2_clean_pages"
    pc4_dir = root / "tests" / "output" / "pc4_consolidated"
    pc5_dir = root / "tests" / "output" / "pc5_graph"

    print(f"[TEST14] case_id = {case_id}")
    print(f"[TEST14] pc2_dir = {pc2_dir}")
    print(f"[TEST14] pc4_dir = {pc4_dir}")
    print(f"[TEST14] pc5_dir = {pc5_dir}")

    if not pc2_dir.exists():
        raise FileNotFoundError(f"[TEST14] pc2_dir no existe: {pc2_dir}")
    if not pc4_dir.exists():
        raise FileNotFoundError(f"[TEST14] pc4_dir no existe: {pc4_dir}")
    if not pc5_dir.exists():
        raise FileNotFoundError(f"[TEST14] pc5_dir no existe: {pc5_dir}")

    run_raggrafo_for_case(
        case_id=case_id,
        pc2_dir=pc2_dir,
        pc4_dir=pc4_dir,
        pc5_dir=pc5_dir,
        use_core=True,
        api_url=None,
        api_key=None,
        push_kg_api=False,
    )


if __name__ == "__main__":
    main()
