from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List

import httpx
import pandas as pd

from app.utils.json_safe import sanitizar_json_safe

SUPABASE_TIMEOUT_SECONDS = 20.0
SUPABASE_INSERT_CHUNK_SIZE = 500

COLUNAS_BASE_REFERENCIA = {
    "id_linha_pipeline",
    "manifesto_id",
    "pedido_id",
    "cliente_id",
    "cnpj",
    "cidade",
    "uf",
    "bairro",
    "cep",
    "latitude",
    "longitude",
    "peso",
    "peso_calculo",
    "volume",
}

PREFIXOS_CALCULADOS = (
    "score_",
    "dist_",
    "km_",
    "tempo_",
    "flag_",
    "calc_",
    "capacidade_",
    "ocupacao_",
)


def _normalizar_dataframe_para_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return []

    df2 = df.copy()
    for col in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[col]):
            df2[col] = df2[col].astype(str)
    df2 = df2.where(pd.notnull(df2), None)

    records = df2.to_dict(orient="records")
    records_sanitizados, _ = sanitizar_json_safe(records)
    return records_sanitizados


def _pick_primeiro_valor(row: Dict[str, Any], nomes: List[str]) -> Any:
    for nome in nomes:
        if nome in row and row.get(nome) is not None:
            return row.get(nome)
    return None


def _extrair_campos_base(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in row.items() if k in COLUNAS_BASE_REFERENCIA}


def _extrair_campos_calculados(row: Dict[str, Any]) -> Dict[str, Any]:
    calculados: Dict[str, Any] = {}
    for k, v in row.items():
        if k in COLUNAS_BASE_REFERENCIA:
            continue
        if any(k.startswith(prefixo) for prefixo in PREFIXOS_CALCULADOS):
            calculados[k] = v
    return calculados


def _config_supabase() -> tuple[str | None, str | None]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")
    return url, key


def persistir_snapshot_modulo_auditoria(
    *,
    teste_id: str,
    rodada_id: str | None,
    upload_id: str | None,
    modulo: str,
    ordem_modulo: int,
    df_etapa: pd.DataFrame | None,
    contexto: Dict[str, Any] | None = None,
) -> int:
    contexto = contexto or {}
    rows = _normalizar_dataframe_para_records(df_etapa if isinstance(df_etapa, pd.DataFrame) else pd.DataFrame())
    total_linhas = len(rows)

    if total_linhas == 0:
        print(f"[AUDITORIA] snapshot modulo={modulo} sem linhas para persistir")
        return 0

    supabase_url, supabase_key = _config_supabase()
    if not supabase_url or not supabase_key:
        print(f"[AUDITORIA] supabase não configurado. modulo={modulo} linhas={total_linhas}")
        return 0

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/auditoria_pipeline_modular"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    payload_insert: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        id_linha_pipeline = _pick_primeiro_valor(row, ["id_linha_pipeline", "id_linha", "linha_id", "pedido_id"])
        manifesto_id = _pick_primeiro_valor(row, ["manifesto_id"])
        status_linha = _pick_primeiro_valor(row, ["status_linha", "status"])
        status_triagem = _pick_primeiro_valor(row, ["status_triagem"])
        grupo_logico = _pick_primeiro_valor(row, ["grupo_logico", "grupo"])
        etapa_origem = _pick_primeiro_valor(row, ["etapa_origem", "origem_manifesto_modulo"])

        payload_insert.append(
            {
                "teste_id": teste_id,
                "rodada_id": rodada_id,
                "upload_id": upload_id,
                "modulo": modulo,
                "ordem_modulo": ordem_modulo,
                "tipo_roteirizacao": contexto.get("tipo_roteirizacao"),
                "filial_id": contexto.get("filial_id"),
                "usuario_id": contexto.get("usuario_id"),
                "data_base_roteirizacao": contexto.get("data_base_roteirizacao"),
                "id_linha_pipeline": str(id_linha_pipeline) if id_linha_pipeline is not None else f"{modulo}-{idx}",
                "manifesto_id": str(manifesto_id) if manifesto_id is not None else None,
                "status_linha": str(status_linha) if status_linha is not None else None,
                "status_triagem": str(status_triagem) if status_triagem is not None else None,
                "grupo_logico": str(grupo_logico) if grupo_logico is not None else None,
                "etapa_origem": str(etapa_origem) if etapa_origem is not None else modulo,
                "payload_linha_json": row,
                "campos_base_json": _extrair_campos_base(row),
                "campos_calculados_json": _extrair_campos_calculados(row),
                "campos_auditoria_json": {
                    "modulo": modulo,
                    "ordem_modulo": ordem_modulo,
                    "snapshot_em": datetime.utcnow().isoformat() + "Z",
                },
            }
        )

    try:
        with httpx.Client(timeout=SUPABASE_TIMEOUT_SECONDS) as client:
            for i in range(0, len(payload_insert), SUPABASE_INSERT_CHUNK_SIZE):
                chunk = payload_insert[i : i + SUPABASE_INSERT_CHUNK_SIZE]
                response = client.post(endpoint, headers=headers, json=chunk)
                response.raise_for_status()
    except Exception as exc:
        print(f"[AUDITORIA] falha ao persistir modulo={modulo}: {exc}")
        return 0

    return total_linhas
