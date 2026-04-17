# ============================================================
# MÓDULO 3 — TRIAGEM OPERACIONAL
# MÓDULO 3.1 — VALIDAÇÃO DE FRONTEIRA
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import hashlib
import math
from typing import Any, Dict

import numpy as np
import pandas as pd


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


def _safe_bool(x: Any) -> bool:
    if x is None:
        return False
    try:
        if pd.isna(x):
            return False
    except Exception:
        pass
    if isinstance(x, (bool, np.bool_)):
        return bool(x)
    return str(x).strip().lower() in {"true", "1", "sim", "s", "yes", "y"}


# ─────────────────────────────────────────────
# M3 — TRIAGEM
# ─────────────────────────────────────────────

def _classificar_linha(row: pd.Series) -> str:
    """
    Regras de classificação:
    - roteirizavel:       sem agenda + com DLE, OU com agenda e 0 <= folga < 2
    - agendamento_futuro: com agenda e folga >= 2
    - agenda_vencida:     com agenda e folga < 0
    - excecao_triagem:    sem agenda e sem DLE, ou cenário não mapeado
    """
    agendada  = _safe_bool(row.get("agendada"))
    folga     = _safe_float(row.get("folga_dias"), np.nan)
    tem_dle   = pd.notna(row.get("data_leadtime"))
    tem_agenda = pd.notna(row.get("data_agenda"))

    if agendada and tem_agenda:
        if isinstance(folga, float) and math.isnan(folga):
            return "excecao_triagem"
        if folga < 0:
            return "agenda_vencida"
        if folga < 2:
            return "roteirizavel"
        return "agendamento_futuro"

    if not agendada:
        if tem_dle:
            return "roteirizavel"
        return "excecao_triagem"

    return "excecao_triagem"


def executar_m3_triagem(
    df_carteira_enriquecida: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M3 — Triagem operacional.
    Classifica cada linha em: roteirizavel, agendamento_futuro, agenda_vencida, excecao_triagem.
    Valida integridade: soma de todos os grupos = total de entrada.
    """
    carteira = df_carteira_enriquecida.copy()
    total_entrada = len(carteira)

    carteira["status_triagem"] = carteira.apply(_classificar_linha, axis=1)

    df_roteirizavel        = carteira[carteira["status_triagem"] == "roteirizavel"].copy()
    df_agendamento_futuro  = carteira[carteira["status_triagem"] == "agendamento_futuro"].copy()
    df_agenda_vencida      = carteira[carteira["status_triagem"] == "agenda_vencida"].copy()
    df_excecao             = carteira[carteira["status_triagem"] == "excecao_triagem"].copy()

    # Ordenar roteirizáveis por prioridade operacional
    df_roteirizavel = df_roteirizavel.sort_values(
        by=["ranking_prioridade_operacional", "folga_dias", "distancia_rodoviaria_est_km"],
        ascending=[True, True, False],
        kind="mergesort",
    ).reset_index(drop=True)

    # Validação de integridade
    soma = len(df_roteirizavel) + len(df_agendamento_futuro) + len(df_agenda_vencida) + len(df_excecao)
    if soma != total_entrada:
        raise Exception(
            f"M3: Falha de integridade — entrada={total_entrada}, soma grupos={soma}. "
            "Nenhuma linha pode ser perdida na triagem."
        )

    return {
        "df_carteira_triagem":         carteira,
        "df_carteira_roteirizavel":    df_roteirizavel,
        "df_agendamento_futuro":       df_agendamento_futuro,
        "df_agenda_vencida":           df_agenda_vencida,
        "df_excecao_triagem":          df_excecao,
        "resumo_m3": {
            "total_entrada":        total_entrada,
            "roteirizavel":         len(df_roteirizavel),
            "agendamento_futuro":   len(df_agendamento_futuro),
            "agenda_vencida":       len(df_agenda_vencida),
            "excecao_triagem":      len(df_excecao),
        },
    }


# ─────────────────────────────────────────────
# M3.1 — VALIDAÇÃO DE FRONTEIRA
# ─────────────────────────────────────────────

def _gerar_id_linha_pipeline(row: pd.Series, idx: int) -> str:
    """
    Gera id_linha_pipeline único e determinístico.
    Baseado em: romaneio + nro_documento + destinatario + cidade + uf + idx
    """
    partes = [
        str(row.get("romaneio", "")),
        str(row.get("nro_documento", "")),
        str(row.get("destinatario", "")),
        str(row.get("cidade", "")),
        str(row.get("uf", "")),
        str(idx),
    ]
    chave = "|".join(partes)
    return "LP_" + hashlib.md5(chave.encode()).hexdigest()[:12].upper()


def _validar_linha_fronteira(row: pd.Series) -> tuple[bool, str]:
    """
    Valida campos obrigatórios antes de entrar no bloco 4.
    Retorna (valida, motivo_rejeicao).
    """
    peso = _safe_float(row.get("peso_calculado"), np.nan)
    if isinstance(peso, float) and math.isnan(peso):
        return False, "peso_calculado_nulo"
    if peso <= 0:
        return False, "peso_calculado_zero_ou_negativo"

    vol = _safe_float(row.get("vol_m3"), np.nan)
    if isinstance(vol, float) and math.isnan(vol):
        return False, "vol_m3_nulo"

    dist = _safe_float(row.get("distancia_rodoviaria_est_km"), np.nan)
    if isinstance(dist, float) and math.isnan(dist):
        return False, "distancia_nula_sem_coordenadas"

    agendada = _safe_bool(row.get("agendada"))
    folga    = _safe_float(row.get("folga_dias"), np.nan)

    if agendada:
        if isinstance(folga, float) and math.isnan(folga):
            return False, "agendada_sem_data_valida"
        if folga < 0:
            return False, "agenda_vencida_nao_deve_chegar_aqui"
        if folga >= 2:
            return False, "agendamento_futuro_nao_deve_chegar_aqui"
    else:
        if not pd.notna(row.get("data_leadtime")):
            return False, "nao_agendada_sem_dle"

    return True, "ok"


def executar_m3_1_validacao_fronteira(
    df_carteira_roteirizavel: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M3.1 — Validação de fronteira.
    Gera id_linha_pipeline único.
    Valida campos obrigatórios.
    Retorna df_input_oficial_bloco_4 (somente linhas válidas) e df_rejeitados_m3_1.
    """
    carteira = df_carteira_roteirizavel.copy().reset_index(drop=True)

    # Gerar id_linha_pipeline
    carteira["id_linha_pipeline"] = [
        _gerar_id_linha_pipeline(row, i)
        for i, row in carteira.iterrows()
    ]

    # Validar unicidade
    if carteira["id_linha_pipeline"].duplicated().any():
        # Forçar unicidade adicionando sufixo numérico
        vistos: dict[str, int] = {}
        ids_unicos = []
        for pid in carteira["id_linha_pipeline"]:
            if pid not in vistos:
                vistos[pid] = 0
                ids_unicos.append(pid)
            else:
                vistos[pid] += 1
                ids_unicos.append(f"{pid}_{vistos[pid]}")
        carteira["id_linha_pipeline"] = ids_unicos

    # Validar campos obrigatórios
    resultados = carteira.apply(_validar_linha_fronteira, axis=1)
    carteira["valida_fronteira"]    = resultados.apply(lambda r: r[0])
    carteira["motivo_rejeicao_m3_1"] = resultados.apply(lambda r: r[1])

    df_valido    = carteira[carteira["valida_fronteira"]].copy()
    df_rejeitado = carteira[~carteira["valida_fronteira"]].copy()

    df_valido = df_valido.drop(columns=["valida_fronteira", "motivo_rejeicao_m3_1"], errors="ignore")

    return {
        "df_input_oficial_bloco_4": df_valido,
        "df_rejeitados_m3_1":       df_rejeitado,
        "resumo_m3_1": {
            "total_entrada":  len(carteira),
            "validos":        len(df_valido),
            "rejeitados":     len(df_rejeitado),
        },
    }
