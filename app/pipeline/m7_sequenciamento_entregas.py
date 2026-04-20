from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import math
import numpy as np
import pandas as pd


TIME_LIMIT_SECONDS_PADRAO = 5
FATOR_KM_RODOVIARIO_M7_PADRAO = 1.20
VERSAO_M7 = "NOVA_VERSAO_FILIAL_PRIMEIRA"


# =========================================================================================
# HELPERS
# =========================================================================================
def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if pd.isna(value):
            return default
        return int(float(value))
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except Exception:
        pass
    txt = str(value).strip().lower()
    return txt in {"1", "true", "sim", "s", "yes", "y", "verdadeiro"}


def _resolver_coluna_existente(
    df: pd.DataFrame,
    candidatos: List[str],
    nome_logico: str,
    obrigatoria: bool = True,
) -> str:
    for c in candidatos:
        if c in df.columns:
            return c
    if obrigatoria:
        raise Exception(
            f"M7 não encontrou a coluna obrigatória '{nome_logico}'. "
            f"Esperado um destes nomes: {candidatos}."
        )
    return ""


def _garantir_colunas(df: pd.DataFrame, colunas: List[str]) -> pd.DataFrame:
    out = df.copy()
    for col in colunas:
        if col not in out.columns:
            out[col] = None
    return out


def _validar_colunas(df: pd.DataFrame, obrigatorias: List[str], nome_df: str) -> None:
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        raise Exception(f"M7 encontrou colunas obrigatórias ausentes em {nome_df}: {faltando}")


# =========================================================================================
# DISTÂNCIA
# =========================================================================================
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    if any(pd.isna(x) for x in [lat1, lon1, lat2, lon2]):
        return 999999.0

    r = 6371.0
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))

    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def _distancia_operacional_km(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    fator_km_rodoviario: float,
) -> float:
    dist_hav = _haversine_km(lat1, lon1, lat2, lon2)
    if pd.isna(dist_hav):
        return 999999.0

    fator = _safe_float(fator_km_rodoviario, FATOR_KM_RODOVIARIO_M7_PADRAO)
    if fator <= 0:
        fator = FATOR_KM_RODOVIARIO_M7_PADRAO

    return float(dist_hav) * fator


def _inferir_fator_rodoviario_real_manifesto(
    df_manifesto: pd.DataFrame,
    fallback: float,
) -> float:
    if "distancia_rodoviaria_est_km" not in df_manifesto.columns:
        return float(fallback)

    ratios: List[float] = []

    for _, row in df_manifesto.iterrows():
        lat_o = pd.to_numeric(row.get("latitude_filial_m7"), errors="coerce")
        lon_o = pd.to_numeric(row.get("longitude_filial_m7"), errors="coerce")
        lat_d = pd.to_numeric(row.get("latitude_dest_m7"), errors="coerce")
        lon_d = pd.to_numeric(row.get("longitude_dest_m7"), errors="coerce")
        dist_est = pd.to_numeric(row.get("distancia_rodoviaria_est_km"), errors="coerce")

        dist_hav = _haversine_km(lat_o, lon_o, lat_d, lon_d)
        if pd.isna(dist_est) or pd.isna(dist_hav) or dist_est <= 0 or dist_hav <= 0:
            continue

        ratio = float(dist_est) / float(dist_hav)
        if 0.8 <= ratio <= 3.0:
            ratios.append(ratio)

    if not ratios:
        return float(fallback)

    return float(np.median(ratios))


# =========================================================================================
# PRIORIDADE
# =========================================================================================
def _classificar_prioridade_negocio(row: pd.Series) -> Tuple[int, float, float]:
    agendada = bool(row.get("agendada_norm", False))
    folga = row.get("folga_dias_norm", np.nan)
    peso = row.get("peso_seq_m7", 0.0)

    if pd.isna(folga):
        folga = 9999.0
    if pd.isna(peso):
        peso = 0.0

    if agendada:
        if folga <= 0:
            bucket = 0
        elif folga <= 1:
            bucket = 1
        else:
            bucket = 2
    else:
        if folga <= 0:
            bucket = 3
        elif folga <= 1:
            bucket = 4
        else:
            bucket = 5

    return (bucket, float(folga), -float(peso))


def _calcular_score_parada(df_parada: pd.DataFrame) -> Dict[str, Any]:
    buckets: List[int] = []
    folgas: List[float] = []
    pesos: List[float] = []

    for _, row in df_parada.iterrows():
        b, f, pneg = _classificar_prioridade_negocio(row)
        buckets.append(b)
        folgas.append(f)
        pesos.append(-pneg)

    return {
        "bucket_prioridade": min(buckets) if buckets else 9,
        "folga_min": min(folgas) if folgas else 9999.0,
        "peso_total": sum(pesos) if pesos else 0.0,
    }


def _montar_justificativa_doc(row: pd.Series) -> str:
    bucket, folga, _ = _classificar_prioridade_negocio(row)

    if bucket == 0:
        prioridade_txt = "Agendada com folga vencida/zero"
    elif bucket == 1:
        prioridade_txt = "Agendada com folga de 1 dia"
    elif bucket == 2:
        prioridade_txt = "Agendada com folga acima de 1 dia"
    elif bucket == 3:
        prioridade_txt = "Não agendada urgente"
    elif bucket == 4:
        prioridade_txt = "Não agendada com folga de 1 dia"
    else:
        prioridade_txt = "Não agendada normal"

    return (
        f"{prioridade_txt}; "
        f"folga={folga if not pd.isna(folga) else 'NA'}; "
        f"peso={_safe_float(row.get('peso_seq_m7', 0.0), 0.0):.2f}kg"
    )


def _ordenar_docs_por_prioridade(df_docs: pd.DataFrame, col_doc: str) -> pd.DataFrame:
    dfp = df_docs.copy()

    prioridades = dfp.apply(_classificar_prioridade_negocio, axis=1)
    dfp["bucket_prioridade_doc_m7"] = [x[0] for x in prioridades]
    dfp["folga_prioridade_doc_m7"] = [x[1] for x in prioridades]
    dfp["peso_prioridade_doc_m7"] = [(-x[2]) for x in prioridades]

    dfp = dfp.sort_values(
        by=[
            "bucket_prioridade_doc_m7",
            "folga_prioridade_doc_m7",
            "peso_prioridade_doc_m7",
            col_doc,
        ],
        ascending=[True, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    # IMPORTANTE: manter as colunas auxiliares, porque o restante do M7 usa elas
    return dfp


# =========================================================================================
# NORMALIZAÇÃO
# =========================================================================================
def _normalizar_manifestos(df_manifestos_m6_2: pd.DataFrame) -> pd.DataFrame:
    out = df_manifestos_m6_2.copy()
    _validar_colunas(out, ["manifesto_id"], "df_manifestos_m6_2")
    out["manifesto_id"] = out["manifesto_id"].astype(str).str.strip()
    out = out[out["manifesto_id"] != ""].copy()
    return out.reset_index(drop=True)


def _normalizar_itens(df_itens_m6_2: pd.DataFrame) -> pd.DataFrame:
    out = df_itens_m6_2.copy()

    colunas_minimas = [
        "manifesto_id",
        "id_linha_pipeline",
        "nro_documento",
        "destinatario",
        "cidade",
        "uf",
        "peso_kg",
        "peso_calculado",
        "agendada",
        "folga_dias",
        "distancia_rodoviaria_est_km",
    ]
    out = _garantir_colunas(out, colunas_minimas)

    _validar_colunas(
        out,
        ["manifesto_id", "id_linha_pipeline", "destinatario", "cidade", "uf"],
        "df_itens_manifestos_m6_2",
    )

    col_lat_filial = _resolver_coluna_existente(
        out,
        ["latitude_filial", "origem_latitude"],
        "latitude_filial",
        obrigatoria=False,
    )
    if col_lat_filial == "":
        out["latitude_filial"] = np.nan
        col_lat_filial = "latitude_filial"

    col_lon_filial = _resolver_coluna_existente(
        out,
        ["longitude_filial", "origem_longitude"],
        "longitude_filial",
        obrigatoria=False,
    )
    if col_lon_filial == "":
        out["longitude_filial"] = np.nan
        col_lon_filial = "longitude_filial"

    col_lat_dest = _resolver_coluna_existente(
        out,
        ["latitude_destinatario", "latitude_destino", "latitude"],
        "latitude_destinatario",
        obrigatoria=False,
    )
    if col_lat_dest == "":
        out["latitude_destinatario"] = np.nan
        col_lat_dest = "latitude_destinatario"

    col_lon_dest = _resolver_coluna_existente(
        out,
        ["longitude_destinatario", "longitude_destino", "longitude"],
        "longitude_destinatario",
        obrigatoria=False,
    )
    if col_lon_dest == "":
        out["longitude_destinatario"] = np.nan
        col_lon_dest = "longitude_destinatario"

    out["manifesto_id"] = out["manifesto_id"].fillna("").astype(str).str.strip()
    out["id_linha_pipeline"] = out["id_linha_pipeline"].fillna("").astype(str).str.strip()
    out["nro_documento"] = out["nro_documento"].fillna("").astype(str).str.strip()
    out["destinatario"] = out["destinatario"].fillna("").astype(str).str.strip()
    out["cidade"] = out["cidade"].fillna("").astype(str).str.strip()
    out["uf"] = out["uf"].fillna("").astype(str).str.strip()

    for c in [
        "peso_kg",
        "peso_calculado",
        "folga_dias",
        "distancia_rodoviaria_est_km",
        col_lat_filial,
        col_lon_filial,
        col_lat_dest,
        col_lon_dest,
    ]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["agendada_norm"] = out["agendada"].apply(_to_bool)
    out["folga_dias_norm"] = pd.to_numeric(out["folga_dias"], errors="coerce")

    out["peso_seq_m7"] = pd.to_numeric(out["peso_calculado"], errors="coerce").fillna(
        pd.to_numeric(out["peso_kg"], errors="coerce")
    )

    out["latitude_filial_m7"] = out[col_lat_filial]
    out["longitude_filial_m7"] = out[col_lon_filial]
    out["latitude_dest_m7"] = out[col_lat_dest]
    out["longitude_dest_m7"] = out[col_lon_dest]

    col_origem_cidade = _resolver_coluna_existente(
        out,
        ["origem_cidade", "filial_cidade", "cidade_filial", "cidade_origem"],
        "origem_cidade",
        obrigatoria=False,
    )
    if col_origem_cidade == "":
        out["origem_cidade"] = None
        col_origem_cidade = "origem_cidade"

    col_origem_uf = _resolver_coluna_existente(
        out,
        ["origem_uf", "filial_uf", "uf_filial", "uf_origem"],
        "origem_uf",
        obrigatoria=False,
    )
    if col_origem_uf == "":
        out["origem_uf"] = None
        col_origem_uf = "origem_uf"

    out["origem_cidade"] = out[col_origem_cidade].fillna("").astype(str).str.strip()
    out["origem_uf"] = out[col_origem_uf].fillna("").astype(str).str.strip()

    out = out[(out["manifesto_id"] != "") & (out["id_linha_pipeline"] != "")].copy()

    if out["id_linha_pipeline"].duplicated().any():
        duplicados = out.loc[out["id_linha_pipeline"].duplicated(), "id_linha_pipeline"].astype(str).tolist()[:20]
        raise Exception(
            f"M7 recebeu id_linha_pipeline duplicado em df_itens_manifestos_m6_2: {duplicados}"
        )

    return out.reset_index(drop=True)


# =========================================================================================
# PREPARAÇÃO GEO
# =========================================================================================
def _preparar_coordenadas_contrato(df_itens: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    out = df_itens.copy()

    out["status_coord_filial_m7"] = np.where(
        out["latitude_filial_m7"].notna() & out["longitude_filial_m7"].notna(),
        "ok",
        "sem_coordenada_filial",
    )

    out["status_coord_dest_m7"] = np.where(
        out["latitude_dest_m7"].notna() & out["longitude_dest_m7"].notna(),
        "ok",
        "sem_coordenada_destino",
    )

    out["coord_dest_origem_m7"] = np.where(
        out["latitude_dest_m7"].notna() & out["longitude_dest_m7"].notna(),
        "contrato_carteira",
        "ausente_no_contrato_recebido",
    )

    diagnostico = pd.DataFrame(
        [
            {"indicador": "linhas_filial_ok", "valor": int((out["status_coord_filial_m7"] == "ok").sum())},
            {"indicador": "linhas_filial_nula", "valor": int((out["status_coord_filial_m7"] != "ok").sum())},
            {"indicador": "linhas_destino_ok", "valor": int((out["status_coord_dest_m7"] == "ok").sum())},
            {"indicador": "linhas_destino_nula", "valor": int((out["status_coord_dest_m7"] != "ok").sum())},
        ]
    )

    return out.reset_index(drop=True), diagnostico.reset_index(drop=True)


# =========================================================================================
# GEOMETRIA / EIXO
# =========================================================================================
def _geo_para_xy_km(
    lat_base: float,
    lon_base: float,
    lat: float,
    lon: float,
) -> Tuple[float, float]:
    lat_base_rad = math.radians(float(lat_base))
    x = (float(lon) - float(lon_base)) * 111.320 * math.cos(lat_base_rad)
    y = (float(lat) - float(lat_base)) * 110.574
    return float(x), float(y)


def _norma_xy(x: float, y: float) -> float:
    return float(math.sqrt((x * x) + (y * y)))


def _projecao_no_eixo(
    lat_a: float,
    lon_a: float,
    lat_b: float,
    lon_b: float,
    lat_p: float,
    lon_p: float,
) -> float:
    ax, ay = _geo_para_xy_km(lat_a, lon_a, lat_b, lon_b)
    px, py = _geo_para_xy_km(lat_a, lon_a, lat_p, lon_p)

    norma_a = _norma_xy(ax, ay)
    if norma_a <= 1e-9:
        return 0.0

    ux = ax / norma_a
    uy = ay / norma_a

    return float((px * ux) + (py * uy))


# =========================================================================================
# AGRUPAMENTOS
# =========================================================================================
def _agrupar_paradas(grupo: pd.DataFrame, fator_km_rodoviario_m7: float) -> pd.DataFrame:
    registros: List[Dict[str, Any]] = []

    lat_o = pd.to_numeric(grupo["latitude_filial_m7"], errors="coerce").dropna()
    lon_o = pd.to_numeric(grupo["longitude_filial_m7"], errors="coerce").dropna()
    if len(lat_o) == 0 or len(lon_o) == 0:
        raise Exception(
            f"Manifesto {grupo['manifesto_id'].iloc[0]} sem coordenada de filial no contrato."
        )

    origem = (float(lat_o.iloc[0]), float(lon_o.iloc[0]))

    grupo["chave_parada_seq_m7"] = (
        grupo["destinatario"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["cidade"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["uf"].fillna("").astype(str).str.strip()
    )

    for chave_parada, gpar in grupo.groupby("chave_parada_seq_m7", dropna=False):
        score = _calcular_score_parada(gpar)
        lat_ref = pd.to_numeric(gpar["latitude_dest_m7"], errors="coerce").mean()
        lon_ref = pd.to_numeric(gpar["longitude_dest_m7"], errors="coerce").mean()

        if pd.isna(lat_ref) or pd.isna(lon_ref):
            raise Exception(
                f"Manifesto {grupo['manifesto_id'].iloc[0]} possui parada sem coordenada de destino."
            )

        dist_origem = _distancia_operacional_km(
            origem[0], origem[1], float(lat_ref), float(lon_ref), fator_km_rodoviario_m7
        )

        registros.append(
            {
                "chave_parada_seq_m7": chave_parada,
                "destinatario_ref_m7": _safe_text(gpar["destinatario"].iloc[0]),
                "cidade_ref_m7": _safe_text(gpar["cidade"].iloc[0]),
                "uf_ref_m7": _safe_text(gpar["uf"].iloc[0]),
                "lat_ref_m7": float(lat_ref),
                "lon_ref_m7": float(lon_ref),
                "bucket_prioridade_m7": score["bucket_prioridade"],
                "folga_min_m7": score["folga_min"],
                "peso_total_m7": score["peso_total"],
                "qtd_docs_parada_m7": int(len(gpar)),
                "distancia_origem_parada_km_m7": float(dist_origem),
            }
        )

    return pd.DataFrame(registros).reset_index(drop=True)


def _agrupar_cidades(grupo: pd.DataFrame, fator_km_rodoviario_m7: float) -> pd.DataFrame:
    registros: List[Dict[str, Any]] = []

    lat_o = pd.to_numeric(grupo["latitude_filial_m7"], errors="coerce").dropna()
    lon_o = pd.to_numeric(grupo["longitude_filial_m7"], errors="coerce").dropna()
    if len(lat_o) == 0 or len(lon_o) == 0:
        raise Exception(
            f"Manifesto {grupo['manifesto_id'].iloc[0]} sem coordenada de filial no contrato."
        )

    origem = (float(lat_o.iloc[0]), float(lon_o.iloc[0]))

    grupo["chave_cidade_seq_m7"] = (
        grupo["cidade"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["uf"].fillna("").astype(str).str.strip()
    )

    for chave_cidade, gcid in grupo.groupby("chave_cidade_seq_m7", dropna=False):
        lat_ref = pd.to_numeric(gcid["latitude_dest_m7"], errors="coerce").mean()
        lon_ref = pd.to_numeric(gcid["longitude_dest_m7"], errors="coerce").mean()

        if pd.isna(lat_ref) or pd.isna(lon_ref):
            raise Exception(
                f"Manifesto {grupo['manifesto_id'].iloc[0]} possui cidade sem coordenada válida."
            )

        dist_origem = _distancia_operacional_km(
            origem[0], origem[1], float(lat_ref), float(lon_ref), fator_km_rodoviario_m7
        )

        pesos = pd.to_numeric(gcid["peso_seq_m7"], errors="coerce").fillna(0.0)
        buckets = gcid.apply(lambda r: _classificar_prioridade_negocio(r)[0], axis=1).tolist()
        folgas = gcid.apply(lambda r: _classificar_prioridade_negocio(r)[1], axis=1).tolist()

        registros.append(
            {
                "chave_cidade_seq_m7": chave_cidade,
                "cidade_ref_m7": _safe_text(gcid["cidade"].iloc[0]),
                "uf_ref_m7": _safe_text(gcid["uf"].iloc[0]),
                "lat_ref_cidade_m7": float(lat_ref),
                "lon_ref_cidade_m7": float(lon_ref),
                "qtd_docs_cidade_m7": int(len(gcid)),
                "qtd_paradas_cidade_m7": int(gcid["chave_parada_seq_m7"].nunique()),
                "peso_total_cidade_m7": float(pesos.sum()),
                "bucket_prioridade_cidade_m7": min(buckets) if buckets else 9,
                "folga_min_cidade_m7": min(folgas) if folgas else 9999.0,
                "distancia_origem_cidade_km_m7": float(dist_origem),
            }
        )

    return pd.DataFrame(registros).reset_index(drop=True)


# =========================================================================================
# SEQUÊNCIA DE CIDADES POR VARREDURA ENTRE EXTREMOS
# =========================================================================================
def _calcular_km_ordem_cidades(
    df_cidades: pd.DataFrame,
    ordem_chaves: List[str],
    origem_lat: float,
    origem_lon: float,
    fator_km_rodoviario_m7: float,
) -> Tuple[List[Dict[str, Any]], float]:
    idx_por_chave = {
        str(row["chave_cidade_seq_m7"]): i for i, row in df_cidades.iterrows()
    }

    trilha: List[Dict[str, Any]] = []
    km_total = 0.0

    atual_lat = float(origem_lat)
    atual_lon = float(origem_lon)
    origem_label = "ORIGEM"

    for pos, chave in enumerate(ordem_chaves, start=1):
        row = df_cidades.iloc[idx_por_chave[chave]]
        dist = _distancia_operacional_km(
            atual_lat,
            atual_lon,
            float(row["lat_ref_cidade_m7"]),
            float(row["lon_ref_cidade_m7"]),
            fator_km_rodoviario_m7,
        )
        km_total += float(dist)

        trilha.append(
            {
                "ordem_cidade_m7": int(pos),
                "chave_cidade_seq_m7": chave,
                "cidade_ref_m7": _safe_text(row["cidade_ref_m7"]),
                "uf_ref_m7": _safe_text(row["uf_ref_m7"]),
                "origem_anterior_cidade_m7": origem_label,
                "distancia_no_anterior_km_m7": float(dist),
            }
        )

        atual_lat = float(row["lat_ref_cidade_m7"])
        atual_lon = float(row["lon_ref_cidade_m7"])
        origem_label = chave

    return trilha, float(km_total)



def _identificar_cidade_filial_no_manifesto(
    df_cidades: pd.DataFrame,
    filial_cidade: Optional[str],
    filial_uf: Optional[str],
) -> pd.Series:
    cidade = _safe_text(filial_cidade).upper()
    uf = _safe_text(filial_uf).upper()

    if cidade and uf:
        return (
            df_cidades["cidade_ref_m7"].fillna("").astype(str).str.strip().str.upper().eq(cidade)
            & df_cidades["uf_ref_m7"].fillna("").astype(str).str.strip().str.upper().eq(uf)
        )

    if cidade:
        return df_cidades["cidade_ref_m7"].fillna("").astype(str).str.strip().str.upper().eq(cidade)

    return pd.Series([False] * len(df_cidades), index=df_cidades.index)


def _sequenciar_cidades(
    df_cidades: pd.DataFrame,
    origem_lat: float,
    origem_lon: float,
    fator_km_rodoviario_m7: float,
    filial_cidade: Optional[str] = None,
    filial_uf: Optional[str] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], float]:
    if df_cidades.empty:
        return df_cidades.copy(), [], 0.0

    work = df_cidades.copy().reset_index(drop=True)

    work["dist_origem_tmp_m7"] = work.apply(
        lambda r: _distancia_operacional_km(
            origem_lat,
            origem_lon,
            float(r["lat_ref_cidade_m7"]),
            float(r["lon_ref_cidade_m7"]),
            fator_km_rodoviario_m7,
        ),
        axis=1,
    )

    cidade_filial_flag = _identificar_cidade_filial_no_manifesto(
        df_cidades=work,
        filial_cidade=filial_cidade,
        filial_uf=filial_uf,
    )

    if bool(cidade_filial_flag.any()):
        work["cidade_origem_flag_m7"] = cidade_filial_flag.astype(bool)
        criterio_base = "cidade_da_filial_primeira"
    else:
        tolerancia_origem_km = 0.50
        work["cidade_origem_flag_m7"] = work["dist_origem_tmp_m7"] <= tolerancia_origem_km
        criterio_base = "fallback_proximidade_origem"

    if len(work) == 1:
        row = work.iloc[0]
        dist = float(row["dist_origem_tmp_m7"])
        criterio = (
            f"{criterio_base}__cidade_unica_origem"
            if bool(row["cidade_origem_flag_m7"])
            else "cidade_unica"
        )

        work["ordem_cidade_m7"] = 1
        work["origem_anterior_cidade_m7"] = "ORIGEM"
        work["distancia_no_anterior_km_m7"] = float(dist)
        work["criterio_escolha_cidade_m7"] = criterio
        work["metodo_sequenciamento_cidade_m7"] = "varredura_extremos_por_cidade_mais_entregas_internas- nova versão"
        work["chave_proxima_cidade_m7"] = ""
        work["distancia_proxima_cidade_km_m7"] = np.nan

        trilha = [
            {
                "ordem_cidade_m7": 1,
                "chave_cidade_seq_m7": str(row["chave_cidade_seq_m7"]),
                "cidade_ref_m7": _safe_text(row["cidade_ref_m7"]),
                "uf_ref_m7": _safe_text(row["uf_ref_m7"]),
                "origem_anterior_cidade_m7": "ORIGEM",
                "distancia_no_anterior_km_m7": float(dist),
                "criterio_escolha_cidade_m7": criterio,
            }
        ]
        return work.reset_index(drop=True), trilha, float(dist)

    cidades_origem = work.loc[work["cidade_origem_flag_m7"]].copy().reset_index(drop=True)
    cidades_macro = work.loc[~work["cidade_origem_flag_m7"]].copy().reset_index(drop=True)

    # Se todas as cidades do manifesto são da própria filial, mantém só ordenação local
    if cidades_macro.empty:
        base = cidades_origem.sort_values(
            by=[
                "bucket_prioridade_cidade_m7",
                "folga_min_cidade_m7",
                "dist_origem_tmp_m7",
                "cidade_ref_m7",
                "uf_ref_m7",
            ],
            ascending=[True, True, True, True, True],
            kind="mergesort",
        ).reset_index(drop=True)

        ordem_escolhida = base["chave_cidade_seq_m7"].astype(str).tolist()
        trilha_escolhida, km_total = _calcular_km_ordem_cidades(
            df_cidades=base,
            ordem_chaves=ordem_escolhida,
            origem_lat=origem_lat,
            origem_lon=origem_lon,
            fator_km_rodoviario_m7=fator_km_rodoviario_m7,
        )

        ordem_map = {ch: i + 1 for i, ch in enumerate(ordem_escolhida)}
        proximo_map: Dict[str, str] = {}
        dist_proximo_map: Dict[str, float] = {}
        idx_por_chave = {str(row["chave_cidade_seq_m7"]): i for i, row in base.iterrows()}

        for pos, chave in enumerate(ordem_escolhida):
            if pos == len(ordem_escolhida) - 1:
                proximo_map[chave] = ""
                dist_proximo_map[chave] = np.nan
            else:
                prox = ordem_escolhida[pos + 1]
                row_a2 = base.iloc[idx_por_chave[chave]]
                row_b2 = base.iloc[idx_por_chave[prox]]
                dist_ab = _distancia_operacional_km(
                    float(row_a2["lat_ref_cidade_m7"]),
                    float(row_a2["lon_ref_cidade_m7"]),
                    float(row_b2["lat_ref_cidade_m7"]),
                    float(row_b2["lon_ref_cidade_m7"]),
                    fator_km_rodoviario_m7,
                )
                proximo_map[chave] = prox
                dist_proximo_map[chave] = float(dist_ab)

        out = base.copy()
        out["ordem_cidade_m7"] = out["chave_cidade_seq_m7"].map(ordem_map)
        out["chave_proxima_cidade_m7"] = out["chave_cidade_seq_m7"].map(proximo_map)
        out["distancia_proxima_cidade_km_m7"] = out["chave_cidade_seq_m7"].map(dist_proximo_map)
        out["metodo_sequenciamento_cidade_m7"] = "varredura_origem_local- nova versão"
        out["criterio_escolha_cidade_m7"] = f"{criterio_base}__todas_as_cidades_origem"

        df_trilha = pd.DataFrame(trilha_escolhida)
        if not df_trilha.empty:
            df_trilha["criterio_escolha_cidade_m7"] = f"{criterio_base}__todas_as_cidades_origem"

        out = out.merge(
            df_trilha[
                [
                    "chave_cidade_seq_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                    "origem_anterior_cidade_m7",
                    "distancia_no_anterior_km_m7",
                    "criterio_escolha_cidade_m7",
                ]
            ],
            on=["chave_cidade_seq_m7", "cidade_ref_m7", "uf_ref_m7"],
            how="left",
        )

        out = out.sort_values(by=["ordem_cidade_m7"], ascending=[True], kind="mergesort").reset_index(drop=True)
        return out, df_trilha.to_dict(orient="records"), float(km_total)

    # Quando existe cidade da filial no manifesto, ela vira obrigatoriamente a primeira cidade.
    # O restante é sequenciado a partir dela, sem excluir as entregas dessa cidade.
    if criterio_base == "cidade_da_filial_primeira":
        cidade_filial = cidades_origem.sort_values(
            by=[
                "bucket_prioridade_cidade_m7",
                "folga_min_cidade_m7",
                "dist_origem_tmp_m7",
                "cidade_ref_m7",
                "uf_ref_m7",
            ],
            ascending=[True, True, True, True, True],
            kind="mergesort",
        ).reset_index(drop=True)

        ancora_row = cidade_filial.iloc[0]
        ancora_chave = str(ancora_row["chave_cidade_seq_m7"])
        ancora_lat = float(ancora_row["lat_ref_cidade_m7"])
        ancora_lon = float(ancora_row["lon_ref_cidade_m7"])

        cidades_macro = cidades_macro.copy().reset_index(drop=True)
        cidades_macro["dist_ancora_tmp_m7"] = cidades_macro.apply(
            lambda r: _distancia_operacional_km(
                ancora_lat,
                ancora_lon,
                float(r["lat_ref_cidade_m7"]),
                float(r["lon_ref_cidade_m7"]),
                fator_km_rodoviario_m7,
            ),
            axis=1,
        )

        if len(cidades_macro) == 1:
            ordem_macro = cidades_macro["chave_cidade_seq_m7"].astype(str).tolist()
            candidatos_ordem: List[Tuple[List[str], str]] = [
                ([ancora_chave] + ordem_macro, "cidade_da_filial_primeira__restante_unico"),
            ]
        else:
            cidades_macro = cidades_macro.sort_values(
                by=[
                    "dist_ancora_tmp_m7",
                    "bucket_prioridade_cidade_m7",
                    "folga_min_cidade_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                ],
                ascending=[False, True, True, True, True],
                kind="mergesort",
            ).reset_index(drop=True)

            extremo_a = cidades_macro.iloc[0]
            cidades_macro["dist_extremo_a_tmp_m7"] = cidades_macro.apply(
                lambda r: _distancia_operacional_km(
                    float(extremo_a["lat_ref_cidade_m7"]),
                    float(extremo_a["lon_ref_cidade_m7"]),
                    float(r["lat_ref_cidade_m7"]),
                    float(r["lon_ref_cidade_m7"]),
                    fator_km_rodoviario_m7,
                ),
                axis=1,
            )
            cidades_macro = cidades_macro.sort_values(
                by=[
                    "dist_extremo_a_tmp_m7",
                    "bucket_prioridade_cidade_m7",
                    "folga_min_cidade_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                ],
                ascending=[False, True, True, True, True],
                kind="mergesort",
            ).reset_index(drop=True)
            extremo_b = cidades_macro.iloc[0]

            lat_a = float(extremo_a["lat_ref_cidade_m7"])
            lon_a = float(extremo_a["lon_ref_cidade_m7"])
            lat_b = float(extremo_b["lat_ref_cidade_m7"])
            lon_b = float(extremo_b["lon_ref_cidade_m7"])

            base_macro = cidades_macro.copy().reset_index(drop=True)
            base_macro["projecao_eixo_ab_m7"] = base_macro.apply(
                lambda r: _projecao_no_eixo(
                    lat_a,
                    lon_a,
                    lat_b,
                    lon_b,
                    float(r["lat_ref_cidade_m7"]),
                    float(r["lon_ref_cidade_m7"]),
                ),
                axis=1,
            )

            c1 = base_macro.sort_values(
                by=[
                    "projecao_eixo_ab_m7",
                    "bucket_prioridade_cidade_m7",
                    "folga_min_cidade_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                ],
                ascending=[False, True, True, True, True],
                kind="mergesort",
            ).reset_index(drop=True)
            c2 = base_macro.sort_values(
                by=[
                    "projecao_eixo_ab_m7",
                    "bucket_prioridade_cidade_m7",
                    "folga_min_cidade_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                ],
                ascending=[True, True, True, True, True],
                kind="mergesort",
            ).reset_index(drop=True)

            ordem_macro_c1 = c1["chave_cidade_seq_m7"].astype(str).tolist()
            ordem_macro_c2 = c2["chave_cidade_seq_m7"].astype(str).tolist()
            candidatos_ordem = [
                ([ancora_chave] + ordem_macro_c1, "cidade_da_filial_primeira__varredura_eixo_extremos_a_para_b"),
                ([ancora_chave] + ordem_macro_c2, "cidade_da_filial_primeira__varredura_eixo_extremos_b_para_a"),
            ]

        base_total = pd.concat([cidade_filial, cidades_macro], ignore_index=True)
        melhor_ordem: List[str] = []
        melhor_trilha: List[Dict[str, Any]] = []
        melhor_km: Optional[float] = None
        melhor_criterio = ""

        for ordem_teste, criterio_teste in candidatos_ordem:
            trilha_teste, km_teste = _calcular_km_ordem_cidades(
                df_cidades=base_total,
                ordem_chaves=ordem_teste,
                origem_lat=origem_lat,
                origem_lon=origem_lon,
                fator_km_rodoviario_m7=fator_km_rodoviario_m7,
            )
            if melhor_km is None or float(km_teste) < float(melhor_km):
                melhor_ordem = ordem_teste
                melhor_trilha = trilha_teste
                melhor_km = float(km_teste)
                melhor_criterio = criterio_teste

        ordem_map = {ch: i + 1 for i, ch in enumerate(melhor_ordem)}
        proximo_map: Dict[str, str] = {}
        dist_proximo_map: Dict[str, float] = {}
        idx_por_chave = {str(row["chave_cidade_seq_m7"]): i for i, row in base_total.iterrows()}

        for pos, chave in enumerate(melhor_ordem):
            if pos == len(melhor_ordem) - 1:
                proximo_map[chave] = ""
                dist_proximo_map[chave] = np.nan
            else:
                prox = melhor_ordem[pos + 1]
                row_a2 = base_total.iloc[idx_por_chave[chave]]
                row_b2 = base_total.iloc[idx_por_chave[prox]]
                dist_ab = _distancia_operacional_km(
                    float(row_a2["lat_ref_cidade_m7"]),
                    float(row_a2["lon_ref_cidade_m7"]),
                    float(row_b2["lat_ref_cidade_m7"]),
                    float(row_b2["lon_ref_cidade_m7"]),
                    fator_km_rodoviario_m7,
                )
                proximo_map[chave] = prox
                dist_proximo_map[chave] = float(dist_ab)

        out = base_total.copy()
        out["ordem_cidade_m7"] = out["chave_cidade_seq_m7"].map(ordem_map)
        out["chave_proxima_cidade_m7"] = out["chave_cidade_seq_m7"].map(proximo_map)
        out["distancia_proxima_cidade_km_m7"] = out["chave_cidade_seq_m7"].map(dist_proximo_map)
        out["metodo_sequenciamento_cidade_m7"] = "varredura_extremos_por_cidade_mais_entregas_internas- nova versão"

        df_trilha = pd.DataFrame(melhor_trilha)
        if not df_trilha.empty:
            df_trilha["criterio_escolha_cidade_m7"] = melhor_criterio

        out = out.merge(
            df_trilha[
                [
                    "chave_cidade_seq_m7",
                    "cidade_ref_m7",
                    "uf_ref_m7",
                    "origem_anterior_cidade_m7",
                    "distancia_no_anterior_km_m7",
                    "criterio_escolha_cidade_m7",
                ]
            ],
            on=["chave_cidade_seq_m7", "cidade_ref_m7", "uf_ref_m7"],
            how="left",
        )

        out = out.sort_values(by=["ordem_cidade_m7"], ascending=[True], kind="mergesort").reset_index(drop=True)
        return out, df_trilha.to_dict(orient="records"), float(melhor_km if melhor_km is not None else 0.0)

    # fallback legado por proximidade/origem
    cidades_macro = cidades_macro.sort_values(
        by=[
            "dist_origem_tmp_m7",
            "bucket_prioridade_cidade_m7",
            "folga_min_cidade_m7",
            "cidade_ref_m7",
            "uf_ref_m7",
        ],
        ascending=[False, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    extremo_a = cidades_macro.iloc[0]
    cidades_macro["dist_extremo_a_tmp_m7"] = cidades_macro.apply(
        lambda r: _distancia_operacional_km(
            float(extremo_a["lat_ref_cidade_m7"]),
            float(extremo_a["lon_ref_cidade_m7"]),
            float(r["lat_ref_cidade_m7"]),
            float(r["lon_ref_cidade_m7"]),
            fator_km_rodoviario_m7,
        ),
        axis=1,
    )
    cidades_macro = cidades_macro.sort_values(
        by=[
            "dist_extremo_a_tmp_m7",
            "bucket_prioridade_cidade_m7",
            "folga_min_cidade_m7",
            "cidade_ref_m7",
            "uf_ref_m7",
        ],
        ascending=[False, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    extremo_b = cidades_macro.iloc[0]

    lat_a = float(extremo_a["lat_ref_cidade_m7"])
    lon_a = float(extremo_a["lon_ref_cidade_m7"])
    lat_b = float(extremo_b["lat_ref_cidade_m7"])
    lon_b = float(extremo_b["lon_ref_cidade_m7"])

    base_macro = cidades_macro.copy().reset_index(drop=True)
    base_macro["projecao_eixo_ab_m7"] = base_macro.apply(
        lambda r: _projecao_no_eixo(
            lat_a,
            lon_a,
            lat_b,
            lon_b,
            float(r["lat_ref_cidade_m7"]),
            float(r["lon_ref_cidade_m7"]),
        ),
        axis=1,
    )

    c1 = base_macro.sort_values(
        by=[
            "projecao_eixo_ab_m7",
            "bucket_prioridade_cidade_m7",
            "folga_min_cidade_m7",
            "cidade_ref_m7",
            "uf_ref_m7",
        ],
        ascending=[False, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
    c2 = base_macro.sort_values(
        by=[
            "projecao_eixo_ab_m7",
            "bucket_prioridade_cidade_m7",
            "folga_min_cidade_m7",
            "cidade_ref_m7",
            "uf_ref_m7",
        ],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    ordem_macro_c1 = c1["chave_cidade_seq_m7"].astype(str).tolist()
    ordem_macro_c2 = c2["chave_cidade_seq_m7"].astype(str).tolist()
    ordem_origem = cidades_origem.sort_values(
        by=[
            "bucket_prioridade_cidade_m7",
            "folga_min_cidade_m7",
            "dist_origem_tmp_m7",
            "cidade_ref_m7",
            "uf_ref_m7",
        ],
        ascending=[True, True, True, True, True],
        kind="mergesort",
    )["chave_cidade_seq_m7"].astype(str).tolist()

    candidatos_ordem: List[Tuple[List[str], str]] = [
        (ordem_origem + ordem_macro_c1, "origem_no_inicio__varredura_eixo_extremos_a_para_b"),
        (ordem_macro_c1 + ordem_origem, "origem_no_fim__varredura_eixo_extremos_a_para_b"),
        (ordem_origem + ordem_macro_c2, "origem_no_inicio__varredura_eixo_extremos_b_para_a"),
        (ordem_macro_c2 + ordem_origem, "origem_no_fim__varredura_eixo_extremos_b_para_a"),
    ]

    base_total = work.copy().reset_index(drop=True)
    melhor_ordem: List[str] = []
    melhor_trilha: List[Dict[str, Any]] = []
    melhor_km: Optional[float] = None
    melhor_criterio = ""

    for ordem_teste, criterio_teste in candidatos_ordem:
        trilha_teste, km_teste = _calcular_km_ordem_cidades(
            df_cidades=base_total,
            ordem_chaves=ordem_teste,
            origem_lat=origem_lat,
            origem_lon=origem_lon,
            fator_km_rodoviario_m7=fator_km_rodoviario_m7,
        )
        if melhor_km is None or float(km_teste) < float(melhor_km):
            melhor_ordem = ordem_teste
            melhor_trilha = trilha_teste
            melhor_km = float(km_teste)
            melhor_criterio = criterio_teste

    ordem_map = {ch: i + 1 for i, ch in enumerate(melhor_ordem)}
    proximo_map: Dict[str, str] = {}
    dist_proximo_map: Dict[str, float] = {}
    idx_por_chave = {str(row["chave_cidade_seq_m7"]): i for i, row in base_total.iterrows()}

    for pos, chave in enumerate(melhor_ordem):
        if pos == len(melhor_ordem) - 1:
            proximo_map[chave] = ""
            dist_proximo_map[chave] = np.nan
        else:
            prox = melhor_ordem[pos + 1]
            row_a2 = base_total.iloc[idx_por_chave[chave]]
            row_b2 = base_total.iloc[idx_por_chave[prox]]
            dist_ab = _distancia_operacional_km(
                float(row_a2["lat_ref_cidade_m7"]),
                float(row_a2["lon_ref_cidade_m7"]),
                float(row_b2["lat_ref_cidade_m7"]),
                float(row_b2["lon_ref_cidade_m7"]),
                fator_km_rodoviario_m7,
            )
            proximo_map[chave] = prox
            dist_proximo_map[chave] = float(dist_ab)

    out = base_total.copy()
    out["ordem_cidade_m7"] = out["chave_cidade_seq_m7"].map(ordem_map)
    out["chave_proxima_cidade_m7"] = out["chave_cidade_seq_m7"].map(proximo_map)
    out["distancia_proxima_cidade_km_m7"] = out["chave_cidade_seq_m7"].map(dist_proximo_map)
    out["metodo_sequenciamento_cidade_m7"] = "varredura_extremos_por_cidade_mais_entregas_internas- nova versão"

    df_trilha = pd.DataFrame(melhor_trilha)
    if not df_trilha.empty:
        df_trilha["criterio_escolha_cidade_m7"] = melhor_criterio

    out = out.merge(
        df_trilha[
            [
                "chave_cidade_seq_m7",
                "cidade_ref_m7",
                "uf_ref_m7",
                "origem_anterior_cidade_m7",
                "distancia_no_anterior_km_m7",
                "criterio_escolha_cidade_m7",
            ]
        ],
        on=["chave_cidade_seq_m7", "cidade_ref_m7", "uf_ref_m7"],
        how="left",
    )

    out = out.sort_values(by=["ordem_cidade_m7"], ascending=[True], kind="mergesort").reset_index(drop=True)
    return out, df_trilha.to_dict(orient="records"), float(melhor_km if melhor_km is not None else 0.0)



# =========================================================================================
# SEQUÊNCIA DE DOCS DENTRO DA CIDADE
# =========================================================================================
def _ordenar_docs_dentro_cidade(
    df_cidade_docs: pd.DataFrame,
    entrada_lat: float,
    entrada_lon: float,
    saida_lat: Optional[float],
    saida_lon: Optional[float],
    fator_km_rodoviario_m7: float,
    col_doc: str,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]], float]:
    work = df_cidade_docs.copy().reset_index(drop=True)
    work = _ordenar_docs_por_prioridade(work, col_doc)

    if work.empty:
        return work.copy(), [], 0.0

    work["chave_doc_m7"] = work[col_doc].astype(str).str.strip()
    restantes = work["chave_doc_m7"].astype(str).tolist()
    idx_por_chave = {str(row["chave_doc_m7"]): i for i, row in work.iterrows()}

    trilha: List[Dict[str, Any]] = []
    km_total_interno = 0.0

    candidatos = work.loc[[idx_por_chave[ch] for ch in restantes]].copy().reset_index(drop=True)
    candidatos["dist_entrada_tmp_m7"] = candidatos.apply(
        lambda r: _distancia_operacional_km(
            entrada_lat,
            entrada_lon,
            float(r["latitude_dest_m7"]),
            float(r["longitude_dest_m7"]),
            fator_km_rodoviario_m7,
        ),
        axis=1,
    )

    candidatos = candidatos.sort_values(
        by=[
            "dist_entrada_tmp_m7",
            "bucket_prioridade_doc_m7",
            "folga_prioridade_doc_m7",
            "peso_prioridade_doc_m7",
            col_doc,
        ],
        ascending=[True, True, True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    primeiro = candidatos.iloc[0]
    chave_primeiro = str(primeiro["chave_doc_m7"])
    restantes.remove(chave_primeiro)
    km_primeiro = float(primeiro["dist_entrada_tmp_m7"])
    km_total_interno += km_primeiro

    trilha.append(
        {
            "chave_doc_m7": chave_primeiro,
            "ordem_doc_na_cidade_m7": 1,
            "tipo_posicao_cidade_m7": "primeira_da_cidade",
            "dist_no_anterior_km_m7": float(km_primeiro),
            "criterio_escolha_doc_cidade_m7": "mais_proxima_do_ponto_de_entrada",
        }
    )

    chave_ultima_reservada: Optional[str] = None
    if saida_lat is not None and saida_lon is not None and len(restantes) >= 1:
        candidatos_saida = work.loc[[idx_por_chave[ch] for ch in restantes]].copy().reset_index(drop=True)
        candidatos_saida["dist_saida_tmp_m7"] = candidatos_saida.apply(
            lambda r: _distancia_operacional_km(
                float(r["latitude_dest_m7"]),
                float(r["longitude_dest_m7"]),
                float(saida_lat),
                float(saida_lon),
                fator_km_rodoviario_m7,
            ),
            axis=1,
        )

        candidatos_saida = candidatos_saida.sort_values(
            by=[
                "dist_saida_tmp_m7",
                "bucket_prioridade_doc_m7",
                "folga_prioridade_doc_m7",
                "peso_prioridade_doc_m7",
                col_doc,
            ],
            ascending=[True, True, True, False, True],
            kind="mergesort",
        ).reset_index(drop=True)

        chave_ultima_reservada = str(candidatos_saida.iloc[0]["chave_doc_m7"])

    atual_lat = float(work.loc[work["chave_doc_m7"] == chave_primeiro, "latitude_dest_m7"].iloc[0])
    atual_lon = float(work.loc[work["chave_doc_m7"] == chave_primeiro, "longitude_dest_m7"].iloc[0])

    ordem_local = 2
    while restantes:
        candidatos_chaves = restantes.copy()

        if chave_ultima_reservada is not None and len(restantes) > 1 and chave_ultima_reservada in candidatos_chaves:
            candidatos_chaves.remove(chave_ultima_reservada)

        candidatos_nn = work.loc[[idx_por_chave[ch] for ch in candidatos_chaves]].copy().reset_index(drop=True)
        candidatos_nn["dist_tmp_m7"] = candidatos_nn.apply(
            lambda r: _distancia_operacional_km(
                atual_lat,
                atual_lon,
                float(r["latitude_dest_m7"]),
                float(r["longitude_dest_m7"]),
                fator_km_rodoviario_m7,
            ),
            axis=1,
        )

        candidatos_nn = candidatos_nn.sort_values(
            by=[
                "dist_tmp_m7",
                "bucket_prioridade_doc_m7",
                "folga_prioridade_doc_m7",
                "peso_prioridade_doc_m7",
                col_doc,
            ],
            ascending=[True, True, True, False, True],
            kind="mergesort",
        ).reset_index(drop=True)

        escolhido = candidatos_nn.iloc[0]
        chave_escolhida = str(escolhido["chave_doc_m7"])

        if chave_ultima_reservada is not None and len(restantes) == 1 and chave_escolhida == chave_ultima_reservada:
            tipo_posicao = "ultima_da_cidade"
            criterio = "mais_proxima_da_cidade_posterior"
        else:
            tipo_posicao = "intermediaria_da_cidade"
            criterio = "vizinho_mais_proximo_dentro_da_cidade"

        restantes.remove(chave_escolhida)

        dist_trecho = float(escolhido["dist_tmp_m7"])
        km_total_interno += dist_trecho

        trilha.append(
            {
                "chave_doc_m7": chave_escolhida,
                "ordem_doc_na_cidade_m7": int(ordem_local),
                "tipo_posicao_cidade_m7": tipo_posicao,
                "dist_no_anterior_km_m7": float(dist_trecho),
                "criterio_escolha_doc_cidade_m7": criterio,
            }
        )

        atual_lat = float(escolhido["latitude_dest_m7"])
        atual_lon = float(escolhido["longitude_dest_m7"])
        ordem_local += 1

    df_trilha = pd.DataFrame(trilha)
    out = work.merge(df_trilha, on="chave_doc_m7", how="left")
    out["ordem_doc_na_cidade_m7"] = pd.to_numeric(out["ordem_doc_na_cidade_m7"], errors="coerce").astype(int)

    out = out.sort_values(
        by=["ordem_doc_na_cidade_m7", "bucket_prioridade_doc_m7", "folga_prioridade_doc_m7", col_doc],
        ascending=[True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    out = out.drop(columns=["chave_doc_m7"], errors="ignore")
    return out, trilha, float(km_total_interno)


# =========================================================================================
# SEQUENCIAMENTO DE UM MANIFESTO
# =========================================================================================
def _sequenciar_manifesto(
    df_manifesto: pd.DataFrame,
    col_doc: str,
    fator_km_rodoviario_m7: float,
    filial_cidade: Optional[str] = None,
    filial_uf: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    grupo = df_manifesto.copy().reset_index(drop=True)

    lat_origem = pd.to_numeric(grupo["latitude_filial_m7"], errors="coerce").dropna()
    lon_origem = pd.to_numeric(grupo["longitude_filial_m7"], errors="coerce").dropna()

    if len(lat_origem) == 0 or len(lon_origem) == 0:
        raise Exception(
            f"Manifesto {grupo['manifesto_id'].iloc[0]} sem coordenada de filial no contrato."
        )

    origem_lat = float(lat_origem.iloc[0])
    origem_lon = float(lon_origem.iloc[0])

    grupo["chave_parada_seq_m7"] = (
        grupo["destinatario"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["cidade"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["uf"].fillna("").astype(str).str.strip()
    )
    grupo["chave_cidade_seq_m7"] = (
        grupo["cidade"].fillna("").astype(str).str.strip()
        + "|"
        + grupo["uf"].fillna("").astype(str).str.strip()
    )

    grupo = _ordenar_docs_por_prioridade(grupo, col_doc)

    df_paradas = _agrupar_paradas(grupo, fator_km_rodoviario_m7)
    df_cidades = _agrupar_cidades(grupo, fator_km_rodoviario_m7)

    if df_cidades.empty:
        raise Exception(f"Manifesto {grupo['manifesto_id'].iloc[0]} sem cidades para sequenciar.")

    df_cidades_seq, trilha_cidades, km_total_cidades = _sequenciar_cidades(
        df_cidades=df_cidades,
        origem_lat=origem_lat,
        origem_lon=origem_lon,
        fator_km_rodoviario_m7=fator_km_rodoviario_m7,
        filial_cidade=filial_cidade,
        filial_uf=filial_uf,
    )

    partes: List[pd.DataFrame] = []
    trilha_docs_total: List[Dict[str, Any]] = []
    km_total_docs = 0.0

    entrada_lat = origem_lat
    entrada_lon = origem_lon

    for idx_cid, row_cid in df_cidades_seq.iterrows():
        chave_cidade = str(row_cid["chave_cidade_seq_m7"])
        ordem_cidade = int(row_cid["ordem_cidade_m7"])

        if idx_cid < len(df_cidades_seq) - 1:
            prox_cidade = df_cidades_seq.iloc[idx_cid + 1]
            saida_lat = float(prox_cidade["lat_ref_cidade_m7"])
            saida_lon = float(prox_cidade["lon_ref_cidade_m7"])
            chave_prox_cidade = str(prox_cidade["chave_cidade_seq_m7"])
        else:
            saida_lat = None
            saida_lon = None
            chave_prox_cidade = ""

        df_cidade_docs = grupo.loc[grupo["chave_cidade_seq_m7"] == chave_cidade].copy().reset_index(drop=True)

        df_docs_ord, trilha_docs_cidade, km_docs_cidade = _ordenar_docs_dentro_cidade(
            df_cidade_docs=df_cidade_docs,
            entrada_lat=float(entrada_lat),
            entrada_lon=float(entrada_lon),
            saida_lat=saida_lat,
            saida_lon=saida_lon,
            fator_km_rodoviario_m7=fator_km_rodoviario_m7,
            col_doc=col_doc,
        )

        df_docs_ord["ordem_cidade_m7"] = ordem_cidade
        df_docs_ord["criterio_escolha_cidade_m7"] = _safe_text(row_cid.get("criterio_escolha_cidade_m7"))
        df_docs_ord["chave_proxima_cidade_m7"] = chave_prox_cidade
        df_docs_ord["metodo_sequenciamento_cidade_m7"] = "varredura_extremos_por_cidade- nova versão"
        df_docs_ord["cidade_ref_m7"] = row_cid["cidade_ref_m7"]
        df_docs_ord["uf_ref_m7"] = row_cid["uf_ref_m7"]
        df_docs_ord["distancia_no_anterior_cidade_km_m7"] = float(row_cid["distancia_no_anterior_km_m7"])
        df_docs_ord["distancia_origem_cidade_km_m7"] = float(row_cid["distancia_origem_cidade_km_m7"])

        df_docs_ord = df_docs_ord.merge(
            df_paradas[
                [
                    "chave_parada_seq_m7",
                    "bucket_prioridade_m7",
                    "folga_min_m7",
                    "peso_total_m7",
                    "distancia_origem_parada_km_m7",
                    "lat_ref_m7",
                    "lon_ref_m7",
                ]
            ],
            on="chave_parada_seq_m7",
            how="left",
        )

        partes.append(df_docs_ord)

        for item in trilha_docs_cidade:
            trilha_docs_total.append(
                {
                    "chave_cidade_seq_m7": chave_cidade,
                    "ordem_cidade_m7": ordem_cidade,
                    "cidade_ref_m7": row_cid["cidade_ref_m7"],
                    "uf_ref_m7": row_cid["uf_ref_m7"],
                    **item,
                }
            )

        km_total_docs += float(km_docs_cidade)

        ult = df_docs_ord.sort_values("ordem_doc_na_cidade_m7").iloc[-1]
        entrada_lat = float(ult["latitude_dest_m7"])
        entrada_lon = float(ult["longitude_dest_m7"])

    if not partes:
        raise Exception(f"Manifesto {grupo['manifesto_id'].iloc[0]} não produziu sequência.")

    grupo_seq = pd.concat(partes, ignore_index=True)

    grupo_seq = grupo_seq.sort_values(
        by=["ordem_cidade_m7", "ordem_doc_na_cidade_m7", "bucket_prioridade_doc_m7", col_doc],
        ascending=[True, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    grupo_seq["ordem_entrega_doc_m7"] = np.arange(1, len(grupo_seq) + 1)
    grupo_seq["ordem_carregamento_doc_m7"] = (
        grupo_seq["ordem_entrega_doc_m7"].max() - grupo_seq["ordem_entrega_doc_m7"] + 1
    )

    primeira_ordem_parada = (
        grupo_seq.groupby("chave_parada_seq_m7", dropna=False)["ordem_entrega_doc_m7"]
        .min()
        .reset_index()
        .sort_values(by=["ordem_entrega_doc_m7", "chave_parada_seq_m7"], ascending=[True, True], kind="mergesort")
        .reset_index(drop=True)
    )
    primeira_ordem_parada["ordem_entrega_parada_m7"] = np.arange(1, len(primeira_ordem_parada) + 1)

    grupo_seq = grupo_seq.merge(
        primeira_ordem_parada[["chave_parada_seq_m7", "ordem_entrega_parada_m7"]],
        on="chave_parada_seq_m7",
        how="left",
    )

    grupo_seq["metodo_sequenciamento_parada_m7"] = "varredura_extremos_por_cidade_mais_entregas_internas- nova versão"

    grupo_seq["justificativa_ordem_entrega_m7"] = grupo_seq.apply(
        lambda row: (
            f"Cidade={int(_safe_int(row.get('ordem_cidade_m7'), 0))}; "
            f"metodo_cidade={_safe_text(row.get('metodo_sequenciamento_cidade_m7'))}; "
            f"criterio_cidade={_safe_text(row.get('criterio_escolha_cidade_m7'))}; "
            f"cidade_ref={_safe_text(row.get('cidade_ref_m7'))}|{_safe_text(row.get('uf_ref_m7'))}; "
            f"dist_no_anterior_cidade_km={_safe_float(row.get('distancia_no_anterior_cidade_km_m7'), 999999.0):.2f}; "
            f"ordem_doc_na_cidade={int(_safe_int(row.get('ordem_doc_na_cidade_m7'), 0))}; "
            f"tipo_posicao_cidade={_safe_text(row.get('tipo_posicao_cidade_m7'))}; "
            f"criterio_doc_cidade={_safe_text(row.get('criterio_escolha_doc_cidade_m7'))}; "
            f"parada={int(_safe_int(row.get('ordem_entrega_parada_m7'), 0))}; "
            f"destinatario={_safe_text(row.get('destinatario'))}; "
            f"dist_origem_parada_km={_safe_float(row.get('distancia_origem_parada_km_m7'), 999999.0):.2f}; "
            f"prioridade_parada_bucket={_safe_int(row.get('bucket_prioridade_m7'), 9)}; "
            f"folga_min_parada={_safe_float(row.get('folga_min_m7'), 9999.0):.2f}; "
            f"peso_total_parada={_safe_float(row.get('peso_total_m7'), 0.0):.2f}; "
            f"criterio_doc={_montar_justificativa_doc(row)}"
        ),
        axis=1,
    )

    df_paradas_final = (
        grupo_seq.groupby("chave_parada_seq_m7", dropna=False)
        .agg(
            ordem_entrega_parada_m7=("ordem_entrega_parada_m7", "min"),
            cidade_ref_m7=("cidade_ref_m7", "first"),
            uf_ref_m7=("uf_ref_m7", "first"),
            destinatario_ref_m7=("destinatario", "first"),
            bucket_prioridade_m7=("bucket_prioridade_m7", "first"),
            folga_min_m7=("folga_min_m7", "first"),
            peso_total_m7=("peso_total_m7", "first"),
            distancia_origem_parada_km_m7=("distancia_origem_parada_km_m7", "first"),
            lat_ref_m7=("lat_ref_m7", "first"),
            lon_ref_m7=("lon_ref_m7", "first"),
        )
        .reset_index()
        .sort_values(by=["ordem_entrega_parada_m7", "chave_parada_seq_m7"], ascending=[True, True], kind="mergesort")
        .reset_index(drop=True)
    )

    auditoria_local = {
        "trilha_sequenciamento_cidades_m7": trilha_cidades,
        "trilha_sequenciamento_docs_m7": trilha_docs_total,
        "km_total_sequencia_cidades_m7": float(km_total_cidades),
        "km_total_sequencia_docs_intra_cidade_m7": float(km_total_docs),
        "km_total_sequencia_paradas_m7": float(km_total_docs),
    }

    # limpar colunas auxiliares antes de devolver
    grupo_seq = grupo_seq.drop(
        columns=[
            "bucket_prioridade_doc_m7",
            "folga_prioridade_doc_m7",
            "peso_prioridade_doc_m7",
        ],
        errors="ignore",
    )

    return grupo_seq.reset_index(drop=True), df_paradas_final, df_cidades_seq.reset_index(drop=True), auditoria_local


# =========================================================================================
# FUNÇÃO PRINCIPAL
# =========================================================================================
def executar_m7_sequenciamento_entregas(
    df_manifestos_m6_2: pd.DataFrame,
    df_itens_manifestos_m6_2: pd.DataFrame,
    df_geo_tratado: Optional[pd.DataFrame] = None,
    df_geo_raw: Optional[pd.DataFrame] = None,
    data_base_roteirizacao: Optional[datetime] = None,
    tipo_roteirizacao: str = "carteira",
    caminhos_pipeline: Optional[Dict[str, Any]] = None,
    time_limit_seconds: int = TIME_LIMIT_SECONDS_PADRAO,
    fator_km_rodoviario_m7: float = FATOR_KM_RODOVIARIO_M7_PADRAO,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del df_geo_tratado
    del df_geo_raw
    del time_limit_seconds

    if not isinstance(df_manifestos_m6_2, pd.DataFrame) or df_manifestos_m6_2.empty:
        raise Exception("M7 recebeu df_manifestos_m6_2 vazio.")

    if not isinstance(df_itens_manifestos_m6_2, pd.DataFrame) or df_itens_manifestos_m6_2.empty:
        raise Exception("M7 recebeu df_itens_manifestos_m6_2 vazio.")

    df_manifestos = _normalizar_manifestos(df_manifestos_m6_2)
    df_itens = _normalizar_itens(df_itens_manifestos_m6_2)

    manifestos_validos = set(df_manifestos["manifesto_id"].astype(str))
    df_itens = df_itens.loc[df_itens["manifesto_id"].astype(str).isin(manifestos_validos)].copy()

    df_itens, df_diagnostico_recuperacao_coordenadas_m7 = _preparar_coordenadas_contrato(df_itens)

    filial_cidade_global: Optional[str] = None
    filial_uf_global: Optional[str] = None

    candidatos_cidade_filial = [
        "origem_cidade",
        "filial_cidade",
        "cidade_origem",
        "cidade_filial",
    ]
    candidatos_uf_filial = [
        "origem_uf",
        "filial_uf",
        "uf_origem",
        "uf_filial",
    ]

    for col in candidatos_cidade_filial:
        if col in df_itens.columns:
            serie = df_itens[col].dropna().astype(str).str.strip()
            if not serie.empty:
                valor = serie.iloc[0]
                if valor:
                    filial_cidade_global = valor
                    break

    for col in candidatos_uf_filial:
        if col in df_itens.columns:
            serie = df_itens[col].dropna().astype(str).str.strip()
            if not serie.empty:
                valor = serie.iloc[0]
                if valor:
                    filial_uf_global = valor
                    break

    resultados: List[pd.DataFrame] = []
    resumos_manifestos: List[Dict[str, Any]] = []
    tentativas: List[Dict[str, Any]] = []
    auditorias_manifestos: List[Dict[str, Any]] = []
    cidades_resumo_manifestos: List[pd.DataFrame] = []

    for manifesto_id, grupo in df_itens.groupby("manifesto_id", dropna=False):
        grupo = grupo.copy().reset_index(drop=True)

        try:
            if grupo["latitude_dest_m7"].isna().any() or grupo["longitude_dest_m7"].isna().any():
                raise Exception(
                    f"Manifesto {manifesto_id} ainda possui coordenada de destino nula no contrato recebido."
                )

            if grupo["latitude_filial_m7"].isna().any() or grupo["longitude_filial_m7"].isna().any():
                raise Exception(
                    f"Manifesto {manifesto_id} ainda possui coordenada de filial nula no contrato recebido."
                )

            fator_real_manifesto = _inferir_fator_rodoviario_real_manifesto(
                df_manifesto=grupo,
                fallback=fator_km_rodoviario_m7,
            )

            grupo_seq, df_paradas_seq, df_cidades_seq, auditoria_local = _sequenciar_manifesto(
                df_manifesto=grupo,
                col_doc="id_linha_pipeline",
                fator_km_rodoviario_m7=float(fator_real_manifesto),
                filial_cidade=filial_cidade_global,
                filial_uf=filial_uf_global,
            )

            grupo_seq["status_sequenciamento_m7"] = "ok"
            grupo_seq["motivo_status_sequenciamento_m7"] = "sequenciamento_realizado"

            resultados.append(grupo_seq)

            df_cidades_seq = df_cidades_seq.copy()
            df_cidades_seq["manifesto_id"] = manifesto_id
            cidades_resumo_manifestos.append(df_cidades_seq)

            resumos_manifestos.append(
                {
                    "manifesto_id": manifesto_id,
                    "qtd_docs_manifesto_m7": int(len(grupo_seq)),
                    "qtd_paradas_manifesto_m7": int(grupo_seq["chave_parada_seq_m7"].nunique()),
                    "qtd_cidades_manifesto_m7": int(grupo_seq["chave_cidade_seq_m7"].nunique()),
                    "primeira_entrega_parada_m7": grupo_seq.sort_values("ordem_entrega_doc_m7")["chave_parada_seq_m7"].iloc[0],
                    "ultima_entrega_parada_m7": grupo_seq.sort_values("ordem_entrega_doc_m7")["chave_parada_seq_m7"].iloc[-1],
                    "primeira_cidade_m7": grupo_seq.sort_values("ordem_entrega_doc_m7")["chave_cidade_seq_m7"].iloc[0],
                    "ultima_cidade_m7": grupo_seq.sort_values("ordem_entrega_doc_m7")["chave_cidade_seq_m7"].iloc[-1],
                    "status_sequenciamento_m7": "ok",
                    "metodo_predominante_m7": "varredura_extremos_por_cidade_mais_entregas_internas- nova versão",
                    "fator_km_rodoviario_real_m7": float(fator_real_manifesto),
                    "km_total_sequencia_paradas_m7": float(auditoria_local["km_total_sequencia_paradas_m7"]),
                    "km_total_sequencia_cidades_m7": float(auditoria_local["km_total_sequencia_cidades_m7"]),
                }
            )

            tentativas.append(
                {
                    "manifesto_id": manifesto_id,
                    "resultado": "ok",
                    "motivo": "sequenciamento_realizado",
                    "qtd_docs": int(len(grupo_seq)),
                    "qtd_paradas": int(df_paradas_seq.shape[0]),
                    "qtd_cidades": int(df_cidades_seq.shape[0]),
                    "km_total_sequencia_paradas_m7": float(auditoria_local["km_total_sequencia_paradas_m7"]),
                }
            )

            auditorias_manifestos.append(
                {
                    "manifesto_id": manifesto_id,
                    **auditoria_local,
                }
            )

        except Exception as e:
            grupo_fallback = grupo.copy()

            grupo_fallback["chave_parada_seq_m7"] = (
                grupo_fallback["destinatario"].fillna("").astype(str).str.strip()
                + "|"
                + grupo_fallback["cidade"].fillna("").astype(str).str.strip()
                + "|"
                + grupo_fallback["uf"].fillna("").astype(str).str.strip()
            )
            grupo_fallback["chave_cidade_seq_m7"] = (
                grupo_fallback["cidade"].fillna("").astype(str).str.strip()
                + "|"
                + grupo_fallback["uf"].fillna("").astype(str).str.strip()
            )

            grupo_fallback = _ordenar_docs_por_prioridade(grupo_fallback, "id_linha_pipeline")
            grupo_fallback = grupo_fallback.sort_values(
                by=[
                    "cidade",
                    "uf",
                    "bucket_prioridade_doc_m7",
                    "folga_prioridade_doc_m7",
                    "peso_prioridade_doc_m7",
                    "id_linha_pipeline",
                ],
                ascending=[True, True, True, True, False, True],
                kind="mergesort",
            ).reset_index(drop=True)

            grupo_fallback["ordem_entrega_parada_m7"] = np.nan
            grupo_fallback["ordem_entrega_doc_m7"] = np.arange(1, len(grupo_fallback) + 1)
            grupo_fallback["ordem_carregamento_doc_m7"] = (
                grupo_fallback["ordem_entrega_doc_m7"].max() - grupo_fallback["ordem_entrega_doc_m7"] + 1
            )
            grupo_fallback["status_sequenciamento_m7"] = "fallback"
            grupo_fallback["motivo_status_sequenciamento_m7"] = str(e)
            grupo_fallback["metodo_sequenciamento_parada_m7"] = "fallback_regra"
            grupo_fallback["metodo_sequenciamento_cidade_m7"] = "fallback_regra"
            grupo_fallback["justificativa_ordem_entrega_m7"] = grupo_fallback.apply(
                lambda row: f"Fallback por exceção; criterio_doc={_montar_justificativa_doc(row)}; motivo={str(e)}",
                axis=1,
            )

            grupo_fallback = grupo_fallback.drop(
                columns=[
                    "bucket_prioridade_doc_m7",
                    "folga_prioridade_doc_m7",
                    "peso_prioridade_doc_m7",
                ],
                errors="ignore",
            )

            resultados.append(grupo_fallback)

            resumos_manifestos.append(
                {
                    "manifesto_id": manifesto_id,
                    "qtd_docs_manifesto_m7": int(len(grupo_fallback)),
                    "qtd_paradas_manifesto_m7": int(grupo_fallback["chave_parada_seq_m7"].nunique()),
                    "qtd_cidades_manifesto_m7": int(grupo_fallback["chave_cidade_seq_m7"].nunique()),
                    "primeira_entrega_parada_m7": "",
                    "ultima_entrega_parada_m7": "",
                    "primeira_cidade_m7": "",
                    "ultima_cidade_m7": "",
                    "status_sequenciamento_m7": "fallback",
                    "metodo_predominante_m7": "fallback_regra",
                    "fator_km_rodoviario_real_m7": None,
                    "km_total_sequencia_paradas_m7": None,
                    "km_total_sequencia_cidades_m7": None,
                }
            )

            tentativas.append(
                {
                    "manifesto_id": manifesto_id,
                    "resultado": "fallback",
                    "motivo": str(e),
                    "qtd_docs": int(len(grupo_fallback)),
                    "qtd_paradas": int(grupo_fallback["chave_parada_seq_m7"].nunique()),
                    "qtd_cidades": int(grupo_fallback["chave_cidade_seq_m7"].nunique()),
                    "km_total_sequencia_paradas_m7": None,
                }
            )

    df_itens_manifestos_sequenciados_m7 = (
        pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()
    )
    df_manifestos_sequenciamento_resumo_m7 = pd.DataFrame(resumos_manifestos)
    df_tentativas_sequenciamento_m7 = pd.DataFrame(tentativas)
    df_cidades_sequenciamento_resumo_m7 = (
        pd.concat(cidades_resumo_manifestos, ignore_index=True) if cidades_resumo_manifestos else pd.DataFrame()
    )

    if not df_itens_manifestos_sequenciados_m7.empty:
        df_manifestos_m7 = df_manifestos.merge(
            df_manifestos_sequenciamento_resumo_m7,
            on="manifesto_id",
            how="left",
        )
    else:
        df_manifestos_m7 = df_manifestos.copy()

    resumo_m7 = {
        "modulo": "M7",
        "data_base_roteirizacao": (
            data_base_roteirizacao.isoformat()
            if isinstance(data_base_roteirizacao, datetime)
            else str(data_base_roteirizacao)
            if data_base_roteirizacao is not None
            else None
        ),
        "tipo_roteirizacao": tipo_roteirizacao,
        "fonte_geo_m7": "contrato_itens_e_filial",
        "metodo_m7": "varredura_extremos_por_cidade_mais_entregas_internas- nova versão",
        "fator_km_rodoviario_param_m7": float(fator_km_rodoviario_m7),
        "manifestos_entrada_m7": int(df_manifestos["manifesto_id"].nunique()),
        "itens_entrada_m7": int(len(df_itens)),
        "manifestos_saida_m7": int(df_itens_manifestos_sequenciados_m7["manifesto_id"].nunique())
        if not df_itens_manifestos_sequenciados_m7.empty
        else 0,
        "itens_saida_m7": int(len(df_itens_manifestos_sequenciados_m7)),
        "fallbacks_m7": int(
            (df_tentativas_sequenciamento_m7["resultado"] == "fallback").sum()
        ) if not df_tentativas_sequenciamento_m7.empty else 0,
        "linhas_filial_nula_m7": int(
            (df_itens_manifestos_sequenciados_m7["status_coord_filial_m7"] != "ok").sum()
        ) if not df_itens_manifestos_sequenciados_m7.empty else 0,
        "linhas_destino_nula_m7": int(
            (df_itens_manifestos_sequenciados_m7["status_coord_dest_m7"] != "ok").sum()
        ) if not df_itens_manifestos_sequenciados_m7.empty else 0,
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    auditoria_m7 = {
        "manifestos_fallback_m7": (
            df_tentativas_sequenciamento_m7.loc[
                df_tentativas_sequenciamento_m7["resultado"] == "fallback", "manifesto_id"
            ].astype(str).tolist()
            if not df_tentativas_sequenciamento_m7.empty
            else []
        ),
        "auditoria_manifestos_m7": auditorias_manifestos,
        "amostra_justificativas_ordem_m7": (
            df_itens_manifestos_sequenciados_m7[
                [
                    "manifesto_id",
                    "id_linha_pipeline",
                    "ordem_entrega_doc_m7",
                    "ordem_carregamento_doc_m7",
                    "justificativa_ordem_entrega_m7",
                ]
            ]
            .head(50)
            .to_dict(orient="records")
            if not df_itens_manifestos_sequenciados_m7.empty
            else []
        ),
    }

    outputs = {
        "df_manifestos_m7": df_manifestos_m7.reset_index(drop=True),
        "df_itens_manifestos_sequenciados_m7": df_itens_manifestos_sequenciados_m7.reset_index(drop=True),
        "df_manifestos_sequenciamento_resumo_m7": df_manifestos_sequenciamento_resumo_m7.reset_index(drop=True),
        "df_cidades_sequenciamento_resumo_m7": df_cidades_sequenciamento_resumo_m7.reset_index(drop=True),
        "df_tentativas_sequenciamento_m7": df_tentativas_sequenciamento_m7.reset_index(drop=True),
        "df_diagnostico_recuperacao_coordenadas_m7": df_diagnostico_recuperacao_coordenadas_m7.reset_index(drop=True),
    }

    meta = {
        "resumo_m7": resumo_m7,
        "auditoria_m7": auditoria_m7,
    }

    return outputs, meta


def executar_m7(*args: Any, **kwargs: Any):
    return executar_m7_sequenciamento_entregas(*args, **kwargs)


def processar_m7_sequenciamento_entregas(*args: Any, **kwargs: Any):
    return executar_m7_sequenciamento_entregas(*args, **kwargs)


def rodar_m7_sequenciamento_entregas(*args: Any, **kwargs: Any):
    return executar_m7_sequenciamento_entregas(*args, **kwargs)
