from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Tuple

import pandas as pd


VALORES_VAZIOS_OPERACIONAIS = {"", "-", "nenhum", "none", "null", "nan", "nat"}


def is_valor_vazio_operacional(valor: Any) -> bool:
    if valor is None:
        return True
    try:
        if pd.isna(valor):
            return True
    except Exception:
        pass
    if isinstance(valor, str):
        return valor.strip().lower() in VALORES_VAZIOS_OPERACIONAIS
    return False


def _valor_coluna(row: pd.Series, candidatos: List[str]) -> Any:
    for c in candidatos:
        if c in row.index:
            return row[c]
    return None


def _todos_vazios(row: pd.Series, candidatos: List[str]) -> bool:
    return all(is_valor_vazio_operacional(_valor_coluna(row, [c])) for c in candidatos)


def validar_campos_criticos_vazios_pre_m1(df_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    if df_raw is None or df_raw.empty:
        auditoria = {
            "total_linhas_entrada": 0,
            "total_linhas_validas_pre_m1": 0,
            "total_linhas_rejeitadas_pre_m1": 0,
            "total_por_motivo": {},
        }
        return pd.DataFrame(columns=[]), pd.DataFrame(columns=[]), auditoria

    carteira = df_raw.copy()
    rejeitadas: List[Dict[str, Any]] = []
    validas_idx: List[int] = []
    total_por_motivo: Counter[str] = Counter()

    for idx, row in carteira.iterrows():
        motivos: List[str] = []

        if _todos_vazios(row, ["id_linha_pipeline", "nro_documento", "Nro Doc.", "Nro Doc", "Nro Do", "chave_linha_dataset"]):
            motivos.append("identificador_linha_ausente")
        if _todos_vazios(row, ["destinatario", "Destina"]):
            motivos.append("destinatario_ausente")
        if _todos_vazios(row, ["cidade", "Cida", "Cidade Dest."]):
            motivos.append("cidade_ausente")
        if _todos_vazios(row, ["uf", "UF"]):
            motivos.append("uf_ausente")
        if _todos_vazios(row, ["peso_calculado", "Peso Calculo", "Peso Cálculo", "Peso C"]):
            motivos.append("peso_calculado_ausente")
        if _todos_vazios(row, ["data_leadtime", "D.L.E.", "DLE", "dle", "data_limite"]):
            motivos.append("dle_ausente")
        if _todos_vazios(row, ["latitude_destinatario", "Latitude", "latitude"]):
            motivos.append("latitude_destinatario_ausente")
        if _todos_vazios(row, ["longitude_destinatario", "Longitude", "longitude"]):
            motivos.append("longitude_destinatario_ausente")

        if motivos:
            total_por_motivo.update(motivos)
            row_dict = row.to_dict()
            row_dict["status_triagem"] = "excecao"
            row_dict["grupo_saida"] = "excecao_triagem"
            row_dict["status_linha_pipeline"] = "erro_dado_critico_pre_m1"
            row_dict["origem_excecao"] = "validacao_campos_criticos_pre_m1"
            row_dict["motivos_dados_criticos"] = motivos
            texto_motivos = "|".join(motivos)
            row_dict["motivo_triagem"] = f"dados_criticos_ausentes_pre_m1: {texto_motivos}"
            row_dict["motivo_nao_roteirizavel"] = row_dict["motivo_triagem"]
            rejeitadas.append(row_dict)
        else:
            validas_idx.append(idx)

    df_validos = carteira.loc[validas_idx].copy().reset_index(drop=True)
    df_rejeitadas = pd.DataFrame(rejeitadas)
    auditoria = {
        "total_linhas_entrada": int(len(carteira)),
        "total_linhas_validas_pre_m1": int(len(df_validos)),
        "total_linhas_rejeitadas_pre_m1": int(len(df_rejeitadas)),
        "total_por_motivo": dict(total_por_motivo),
    }
    return df_validos, df_rejeitadas, auditoria
