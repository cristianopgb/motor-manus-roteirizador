# ============================================================
# MÓDULO 2 — ENRIQUECIMENTO GEOGRÁFICO E TEMPORAL
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

FATOR_KM_RODOVIARIO_PADRAO = 1.20
KM_DIA_OPERACIONAL_PADRAO  = 400.0
DISTANCIA_MINIMA_KM        = 5.0


def _safe_float(x: Any, default: float = np.nan) -> float:
    if x is None:
        return default
    try:
        if pd.isna(x):
            return default
    except Exception:
        pass
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 1) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _safe_date(x: Any) -> Optional[pd.Timestamp]:
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if isinstance(x, pd.Timestamp):
        return x
    if isinstance(x, datetime):
        return pd.Timestamp(x)
    try:
        ts = pd.to_datetime(x, dayfirst=True, errors="coerce")
        return None if pd.isna(ts) else ts
    except Exception:
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    for v in [lat1, lon1, lat2, lon2]:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return np.nan
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _enriquecer_geo(carteira: pd.DataFrame, geo: pd.DataFrame) -> pd.DataFrame:
    if geo.empty:
        return carteira

    cols_geo = ["cidade_chave", "uf_chave"]
    for c in ["mesorregiao", "subregiao", "latitude", "longitude"]:
        if c in geo.columns:
            cols_geo.append(c)

    geo_dedup = (
        geo[list(dict.fromkeys(cols_geo))]
        .drop_duplicates(subset=["cidade_chave", "uf_chave"])
        .copy()
    )

    merged = carteira.merge(
        geo_dedup, on=["cidade_chave", "uf_chave"], how="left", suffixes=("", "_geo")
    )

    # Preencher mesorregiao
    if "mesorregiao_geo" in merged.columns:
        if "mesorregiao" not in merged.columns:
            merged["mesorregiao"] = merged["mesorregiao_geo"]
        else:
            merged["mesorregiao"] = merged["mesorregiao"].where(
                merged["mesorregiao"].notna(), merged["mesorregiao_geo"]
            )

    # Preencher subregiao
    if "subregiao_geo" in merged.columns:
        if "sub_regiao" not in merged.columns:
            merged["sub_regiao"] = merged["subregiao_geo"]
        else:
            merged["sub_regiao"] = merged["sub_regiao"].where(
                merged["sub_regiao"].notna(), merged["subregiao_geo"]
            )
    elif "subregiao" in merged.columns and "sub_regiao" not in merged.columns:
        merged["sub_regiao"] = merged["subregiao"]

    # Preencher lat/lon destino
    for src, dst in [("latitude_geo", "latitude_destinatario"), ("longitude_geo", "longitude_destinatario")]:
        if src in merged.columns:
            if dst not in merged.columns:
                merged[dst] = np.nan
            merged[dst] = merged[dst].where(merged[dst].notna(), merged[src])

    drop_cols = [c for c in merged.columns if c.endswith("_geo")]
    merged = merged.drop(columns=drop_cols, errors="ignore")
    return merged


def executar_m2_enriquecimento(
    df_carteira_tratada: pd.DataFrame,
    df_geo_tratado: pd.DataFrame,
    param_dict: Dict[str, Any],
) -> Dict[str, Any]:
    carteira = df_carteira_tratada.copy()
    geo      = df_geo_tratado.copy()

    fator_km = _safe_float(param_dict.get("fator_km_rodoviario"), FATOR_KM_RODOVIARIO_PADRAO)
    if math.isnan(fator_km) or fator_km <= 0:
        fator_km = FATOR_KM_RODOVIARIO_PADRAO

    km_dia = _safe_float(param_dict.get("km_dia_operacional"), KM_DIA_OPERACIONAL_PADRAO)
    if math.isnan(km_dia) or km_dia <= 0:
        km_dia = KM_DIA_OPERACIONAL_PADRAO

    data_base_raw = param_dict.get("data_base_roteirizacao") or param_dict.get("data_execucao")
    data_base = _safe_date(data_base_raw)
    if data_base is None:
        data_base = pd.Timestamp.today().normalize()

    # 1) Enriquecimento geo
    carteira = _enriquecer_geo(carteira, geo)

    # 2) Distâncias
    lat_filial = _safe_float(carteira["latitude_filial"].iloc[0] if "latitude_filial" in carteira.columns else np.nan)
    lon_filial = _safe_float(carteira["longitude_filial"].iloc[0] if "longitude_filial" in carteira.columns else np.nan)

    def _dist(row: pd.Series) -> float:
        lf = _safe_float(row.get("latitude_filial"), lat_filial)
        lof = _safe_float(row.get("longitude_filial"), lon_filial)
        ld = _safe_float(row.get("latitude_destinatario"))
        lod = _safe_float(row.get("longitude_destinatario"))
        d = haversine_km(lf, lof, ld, lod)
        if isinstance(d, float) and math.isnan(d):
            return np.nan
        return max(d * fator_km, DISTANCIA_MINIMA_KM)

    carteira["distancia_rodoviaria_est_km"] = carteira.apply(_dist, axis=1)

    carteira["transit_time_dias"] = carteira["distancia_rodoviaria_est_km"].apply(
        lambda d: max(1, math.ceil(_safe_float(d, 1) / km_dia))
        if not (isinstance(d, float) and math.isnan(d)) else 1
    )

    # 3) Folga e prioridade
    def _data_limite(row: pd.Series) -> Optional[pd.Timestamp]:
        agendada = bool(row.get("agendada", False))
        da = _safe_date(row.get("data_agenda"))
        dl = _safe_date(row.get("data_leadtime"))
        if agendada and da is not None:
            return da
        return dl

    def _folga(row: pd.Series) -> float:
        dlim = _safe_date(row.get("data_limite_considerada"))
        if dlim is None:
            return np.nan
        transit = _safe_int(row.get("transit_time_dias"), 1)
        dias = (dlim.normalize() - data_base.normalize()).days
        return float(dias - transit)

    def _status_folga(f: float) -> str:
        if isinstance(f, float) and math.isnan(f):
            return "sem_data"
        if f < 0:
            return "vencida"
        if f == 0:
            return "critica"
        if f == 1:
            return "urgente"
        return "normal"

    def _ranking(row: pd.Series) -> int:
        agendada = bool(row.get("agendada", False))
        f = _safe_float(row.get("folga_dias"), np.nan)
        if isinstance(f, float) and math.isnan(f):
            return 9
        if agendada:
            return 1 if f <= 0 else (2 if f <= 1 else 3)
        return 2 if f <= 0 else (3 if f <= 1 else 4)

    carteira["data_limite_considerada"] = carteira.apply(_data_limite, axis=1)
    carteira["dias_ate_data_alvo"] = carteira.apply(
        lambda r: (
            (_safe_date(r["data_limite_considerada"]).normalize() - data_base.normalize()).days
            if _safe_date(r["data_limite_considerada"]) is not None else np.nan
        ),
        axis=1,
    )
    carteira["folga_dias"] = carteira.apply(_folga, axis=1)
    carteira["status_folga"] = carteira["folga_dias"].apply(_status_folga)
    carteira["ranking_prioridade_operacional"] = carteira.apply(_ranking, axis=1)

    # 4) Perfil de veículo de referência por distância
    def _perfil_ref(d: float) -> str:
        if isinstance(d, float) and math.isnan(d):
            return "indefinido"
        if d <= 100:
            return "VUC"
        if d <= 300:
            return "TOCO"
        if d <= 600:
            return "TRUCK"
        return "CARRETA"

    carteira["perfil_veiculo_referencia"] = carteira["distancia_rodoviaria_est_km"].apply(
        lambda d: _perfil_ref(_safe_float(d, np.nan))
    )

    # 5) Score de prioridade
    carteira["score_prioridade_preliminar"] = (
        carteira["ranking_prioridade_operacional"].fillna(9) * 1000
        + carteira["folga_dias"].fillna(999)
    )

    return {
        "df_carteira_enriquecida": carteira,
        "data_base": data_base,
        "fator_km": fator_km,
        "km_dia": km_dia,
    }
