# ============================================================
# MÓDULO 7 — SEQUENCIAMENTO DE ENTREGAS
# Algoritmo: Nearest Neighbor com prioridade operacional
# Regra: URGENTE/CRITICO/VENCIDO sempre primeiro
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.pipeline.m5_common import (
    safe_float, safe_int, safe_text, safe_bool,
    peso_total, peso_kg_total, vol_total, km_referencia, qtd_paradas,
    ocupacao_perc,
)

FATOR_KM_RODOVIARIO_PADRAO = 1.20


# ─────────────────────────────────────────────
# HAVERSINE
# ─────────────────────────────────────────────

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    for v in [lat1, lon1, lat2, lon2]:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return 0.0
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi    = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _dist_rodoviaria(lat1: float, lon1: float, lat2: float, lon2: float, fator: float = FATOR_KM_RODOVIARIO_PADRAO) -> float:
    return _haversine(lat1, lon1, lat2, lon2) * fator


# ─────────────────────────────────────────────
# BUCKET DE PRIORIDADE (para garantir urgentes primeiro)
# ─────────────────────────────────────────────

def _bucket_prioridade(row: pd.Series) -> int:
    """
    0 = prioridade_embarque explícita
    1 = agendada + vencida/critica (folga <= 0)
    2 = agendada + urgente (folga == 1)
    3 = não agendada + vencida/critica
    4 = não agendada + urgente
    99 = normal
    """
    prioridade = safe_float(row.get("prioridade_embarque_num", row.get("prioridade_embarque", np.nan)), np.nan)
    agendada   = safe_bool(row.get("agendada"))
    folga      = safe_float(row.get("folga_dias"), 999)

    if not (isinstance(prioridade, float) and math.isnan(prioridade)) and prioridade > 0:
        return 0
    if agendada and folga <= 0:
        return 1
    if agendada and folga == 1:
        return 2
    if (not agendada) and folga <= 0:
        return 3
    if (not agendada) and folga == 1:
        return 4
    return 99


# ─────────────────────────────────────────────
# NEAREST NEIGHBOR POR MANIFESTO
# ─────────────────────────────────────────────

def _sequenciar_manifesto(
    df_itens: pd.DataFrame,
    lat_filial: float,
    lon_filial: float,
    fator_km: float,
) -> pd.DataFrame:
    """
    Sequencia as entregas de um manifesto usando Nearest Neighbor.
    Regra: entregas com bucket <= 2 (urgentes/críticas) são forçadas para o início.
    Retorna df com coluna 'sequencia_entrega' (1-based).
    """
    if df_itens.empty:
        return df_itens.copy()

    df = df_itens.copy().reset_index(drop=True)

    # Calcular bucket de prioridade
    df["_bucket_seq"] = df.apply(_bucket_prioridade, axis=1)

    # Separar urgentes (bucket <= 2) e normais
    urgentes = df[df["_bucket_seq"] <= 2].copy().sort_values(
        by=["_bucket_seq", "folga_dias", "ranking_prioridade_operacional"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    normais = df[df["_bucket_seq"] > 2].copy()

    # Nearest neighbor nos urgentes (mantém ordem de prioridade, não reordena por NN)
    sequencia: List[int] = list(urgentes.index.tolist())

    # Nearest neighbor nos normais
    pendentes = set(normais.index.tolist())
    if pendentes:
        # Ponto de partida: último urgente ou filial
        if sequencia:
            ultimo_idx = sequencia[-1]
            lat_atual = safe_float(df.at[ultimo_idx, "latitude_destinatario"], lat_filial)
            lon_atual = safe_float(df.at[ultimo_idx, "longitude_destinatario"], lon_filial)
        else:
            lat_atual = lat_filial
            lon_atual = lon_filial

        while pendentes:
            melhor_idx  = None
            melhor_dist = None

            for idx in pendentes:
                lat_d = safe_float(df.at[idx, "latitude_destinatario"], lat_filial)
                lon_d = safe_float(df.at[idx, "longitude_destinatario"], lon_filial)
                d = _dist_rodoviaria(lat_atual, lon_atual, lat_d, lon_d, fator_km)
                if melhor_dist is None or d < melhor_dist:
                    melhor_dist = d
                    melhor_idx  = idx

            if melhor_idx is None:
                sequencia.extend(sorted(pendentes))
                break

            sequencia.append(melhor_idx)
            pendentes.remove(melhor_idx)
            lat_atual = safe_float(df.at[melhor_idx, "latitude_destinatario"], lat_filial)
            lon_atual = safe_float(df.at[melhor_idx, "longitude_destinatario"], lon_filial)

    # Atribuir sequência
    seq_map = {orig_idx: seq_num for seq_num, orig_idx in enumerate(sequencia, start=1)}
    df["sequencia_entrega"] = df.index.map(seq_map).fillna(0).astype(int)
    df = df.sort_values("sequencia_entrega", ascending=True, kind="mergesort")
    df = df.drop(columns=["_bucket_seq"], errors="ignore")

    return df.reset_index(drop=True)


# ─────────────────────────────────────────────
# CÁLCULO DE KM TOTAL DO MANIFESTO
# ─────────────────────────────────────────────

def _calcular_km_total_manifesto(
    df_itens_seq: pd.DataFrame,
    lat_filial: float,
    lon_filial: float,
    fator_km: float,
) -> float:
    """
    Calcula km total da rota: filial → entrega_1 → entrega_2 → ... → filial
    """
    if df_itens_seq.empty:
        return 0.0

    df = df_itens_seq.sort_values("sequencia_entrega", ascending=True, kind="mergesort")
    km_total = 0.0
    lat_ant, lon_ant = lat_filial, lon_filial

    for _, row in df.iterrows():
        lat_d = safe_float(row.get("latitude_destinatario"), lat_filial)
        lon_d = safe_float(row.get("longitude_destinatario"), lon_filial)
        km_total += _dist_rodoviaria(lat_ant, lon_ant, lat_d, lon_d, fator_km)
        lat_ant, lon_ant = lat_d, lon_d

    # Retorno à filial
    km_total += _dist_rodoviaria(lat_ant, lon_ant, lat_filial, lon_filial, fator_km)
    return round(km_total, 2)


# ─────────────────────────────────────────────
# FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def executar_m7_sequenciamento(
    df_manifestos_finais: pd.DataFrame,
    df_itens_finais: pd.DataFrame,
    param_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """
    M7 — Sequenciamento de entregas.
    Para cada manifesto, aplica Nearest Neighbor com prioridade operacional.
    Calcula km total da rota (filial → entregas → filial).
    Retorna df_manifestos_sequenciados e df_itens_sequenciados.
    """
    if df_manifestos_finais.empty or df_itens_finais.empty:
        return {
            "df_manifestos_sequenciados": df_manifestos_finais,
            "df_itens_sequenciados":      df_itens_finais,
            "resumo_m7": {
                "total_manifestos":   len(df_manifestos_finais),
                "total_itens":        len(df_itens_finais),
                "km_total_frota":     0.0,
            },
        }

    lat_filial = safe_float(param_dict.get("latitude_filial"), 0.0)
    lon_filial = safe_float(param_dict.get("longitude_filial"), 0.0)
    fator_km   = safe_float(param_dict.get("fator_km_rodoviario"), FATOR_KM_RODOVIARIO_PADRAO)
    if fator_km <= 0:
        fator_km = FATOR_KM_RODOVIARIO_PADRAO

    # Tentar extrair lat/lon da filial dos próprios itens
    if (lat_filial == 0.0 or lon_filial == 0.0) and "latitude_filial" in df_itens_finais.columns:
        lat_filial = safe_float(df_itens_finais["latitude_filial"].dropna().iloc[0] if not df_itens_finais["latitude_filial"].dropna().empty else 0.0, 0.0)
        lon_filial = safe_float(df_itens_finais["longitude_filial"].dropna().iloc[0] if not df_itens_finais["longitude_filial"].dropna().empty else 0.0, 0.0)

    df_manifestos = df_manifestos_finais.copy()
    df_itens_seq  = pd.DataFrame()
    km_total_frota = 0.0

    for idx, manifesto_row in df_manifestos.iterrows():
        mid = manifesto_row["manifesto_id"]
        df_m_itens = df_itens_finais[df_itens_finais["manifesto_id"] == mid].copy()

        if df_m_itens.empty:
            continue

        # Sequenciar
        df_m_itens_seq = _sequenciar_manifesto(df_m_itens, lat_filial, lon_filial, fator_km)

        # Calcular km total da rota
        km_rota = _calcular_km_total_manifesto(df_m_itens_seq, lat_filial, lon_filial, fator_km)
        km_total_frota += km_rota

        # Atualizar km no manifesto
        df_manifestos.at[idx, "km_rota_total"]  = km_rota
        df_manifestos.at[idx, "km_referencia"]  = km_rota

        # Acumular itens sequenciados
        df_itens_seq = pd.concat([df_itens_seq, df_m_itens_seq], ignore_index=True)

    # Garantir coluna km_rota_total
    if "km_rota_total" not in df_manifestos.columns:
        df_manifestos["km_rota_total"] = 0.0

    return {
        "df_manifestos_sequenciados": df_manifestos,
        "df_itens_sequenciados":      df_itens_seq,
        "resumo_m7": {
            "total_manifestos":  len(df_manifestos),
            "total_itens":       len(df_itens_seq),
            "km_total_frota":    round(km_total_frota, 2),
        },
    }
