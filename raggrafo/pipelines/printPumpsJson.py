# -*- coding: utf-8 -*-
"""
printPumpsJson.py – AutoSelect-X
================================

Genera HTML amigable para:
1) Resumen rápido (normalizado)
2) RAW en vista expandible
3) Normalizado en vista expandible

Se usa desde el endpoint /cases/{id}/assistant

Autor: Victor + Asistente técnico
"""

import html


# ============================================================
# 1) RESUMEN RÁPIDO (NORMALIZADO)
# ============================================================

def build_summary_html(normalized_json: dict) -> str:
    """Genera una tabla resumida de bombas normalizadas."""

    pumps = normalized_json.get("pumps", [])
    if not pumps:
        return "<p><b>No se detectaron bombas en el resultado.</b></p>"

    out = []
    out.append("<h2>Resumen rápido – Bombas detectadas</h2>")
    out.append("""
    <table border="1" cellpadding="6" cellspacing="0"
           style="border-collapse: collapse; font-size:14px; width:100%;">
        <tr style="background:#f0f0f0;">
            <th>TAG</th>
            <th>Fluido</th>
            <th>Caudal nominal (GPH)</th>
            <th>Caudal máximo (GPH)</th>
            <th>Presión (PSI)</th>
            <th>Viscosidad (cP)</th>
        </tr>
    """)

    for p in pumps:
        opt = p.get("optional", {}) or {}

        tag = html.escape(str(opt.get("tag") or "—"))
        fluid = html.escape(str(p.get("fluid") or "—"))

        fnom = p.get("flow_nominal_std") or "—"
        fmax = p.get("flow_max_std") or "—"

        dpress = p.get("discharge_pressure_std") or "—"
        visc = p.get("viscosity_std") or "—"

        out.append(f"""
        <tr>
            <td>{tag}</td>
            <td>{fluid}</td>
            <td>{fnom}</td>
            <td>{fmax}</td>
            <td>{dpress}</td>
            <td>{visc}</td>
        </tr>
        """)

    out.append("</table>")
    return "\n".join(out)


# ============================================================
# 2) RENDER JSON COMPLETO (RAW O NORMALIZADO)
# ============================================================

def build_json_html(pumps_list: list, title: str = "JSON") -> str:
    """
    Genera un bloque <details> para el JSON (RAW o Normalizado).
    Cada bomba aparece como sección expandible.
    """

    if not pumps_list:
        return f"<h2>{title}</h2><p>No hay bombas para mostrar.</p>"

    out = []
    out.append(f"<h2>{title}</h2>")
    out.append(f"<details open><summary><b>{title} – Ver/Ocultar</b></summary>")

    for idx, pump in enumerate(pumps_list, start=1):

        out.append(f"<details><summary><b>Bomba {idx}</b></summary>")
        out.append("""
            <table border='1' cellpadding='6' cellspacing='0'
                   style='border-collapse: collapse; margin:10px 0; width:100%;'>
        """)

        # Campos principales
        for key, value in pump.items():

            # Caso dict (subtabla)
            if isinstance(value, dict):
                out.append(
                    f"<tr><td colspan='2' style='background:#fafafa;'><b>{html.escape(key)}</b></td></tr>"
                )
                for sub_k, sub_v in value.items():
                    sub_v = "—" if sub_v in (None, "", []) else sub_v
                    out.append(f"""
                        <tr>
                            <td style='padding-left:25px;'>{html.escape(str(sub_k))}</td>
                            <td>{html.escape(str(sub_v))}</td>
                        </tr>
                    """)
                continue

            # Campo simple
            value = "—" if value in (None, "", []) else value
            out.append(f"""
                <tr>
                    <td><b>{html.escape(str(key))}</b></td>
                    <td>{html.escape(str(value))}</td>
                </tr>
            """)

        out.append("</table>")
        out.append("</details>")

    out.append("</details>")
    return "\n".join(out)


# ============================================================
# 3) WRAPPER – Devuelve los 3 mensajes HTML
# ============================================================

def build_all_messages(raw_json: dict, normalized_json: dict) -> dict:
    """
    Construye los 3 mensajes HTML:

    - summary → Vista rápida (normalizado)
    - raw → JSON RAW completo
    - normalized → JSON normalizado completo
    """

    summary_html = build_summary_html(normalized_json)
    raw_html = build_json_html(raw_json.get("pumps", []), "JSON RAW")
    norm_html = build_json_html(normalized_json.get("pumps", []), "JSON Normalizado")

    return {
        "summary": summary_html,
        "raw": raw_html,
        "normalized": norm_html
    }
