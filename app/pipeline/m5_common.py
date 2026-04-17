# ============================================================
# MÓDULO 5 — HELPERS COMUNS (m5_common)
# Contrato interno do bloco de composição territorial
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

OCUPACAO_MINIMA_PADRAO = 0.70
OCUPACAO_MAXIMA_PADRAO = 1.00

MAX_CLIENTES_BASE      = 10
MAX_PREFIXOS_POR_PERFIL = 8
MAX_TROCAS_1           = 20
MAX_TROCAS_2           = 30


# ─────────────────────────────────────────────
# HELPERS BÁSICOS
# ─────────────────────────────────────────────

def safe_float(x: Any, default: float = 0.0) -> float:
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


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def safe_text(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def safe_bool(x: Any) -> bool:
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


def ensure_columns(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
    for col, val in defaults.items():
        if col not in df.columns:
            df[col] = val
    return df


# ─────────────────────────────────────────────
# RESTRIÇÃO DE VEÍCULO
# ─────────────────────────────────────────────

def _normalizar_token(x: Any) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    txt = str(x).strip().upper()
    txt = "".join(
        c for c in unicodedata.normalize("NFKD", txt)
        if not unicodedata.combining(c)
    )
    txt = re.sub(r"[^A-Z0-9]+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")
    return txt


_ALIASES: Dict[str, Set[str]] = {
    "VUC":        {"VUC"},
    "3_4":        {"3_4", "TRES_QUARTOS", "TRES_QUARTO"},
    "TOCO":       {"TOCO"},
    "TRUCK":      {"TRUCK"},
    "CARRETA":    {"CARRETA"},
    "UTILITARIO": {"UTILITARIO", "UTILITARIOS", "FIORINO", "VAN"},
    "CARRO":      {"CARRO", "PASSEIO", "VEICULO_LEVE"},
}


def expandir_alias(token: str) -> Set[str]:
    token = _normalizar_token(token)
    for canonico, grupo in _ALIASES.items():
        if token == canonico or token in grupo:
            return {canonico} | grupo
    return {token}


def tokens_restricao(valor: Any) -> Set[str]:
    if valor is None:
        return set()
    try:
        if pd.isna(valor):
            return set()
    except Exception:
        pass
    txt = str(valor).strip()
    if not txt:
        return set()
    partes = re.split(r"[;,|/]+", txt)
    result: Set[str] = set()
    for p in partes:
        t = _normalizar_token(p)
        if t:
            result |= expandir_alias(t)
    return result


def veiculo_compativel(tipo: Any, restricao: Any) -> bool:
    r = tokens_restricao(restricao)
    if not r:
        return True
    return bool(r & expandir_alias(tipo))


def grupo_respeita_restricao(df_itens: pd.DataFrame, veiculo: pd.Series) -> bool:
    if "restricao_veiculo" not in df_itens.columns:
        return True
    tipo = veiculo.get("tipo") or veiculo.get("perfil", "")
    return all(veiculo_compativel(tipo, r) for r in df_itens["restricao_veiculo"].tolist())


# ─────────────────────────────────────────────
# MÉTRICAS
# ─────────────────────────────────────────────

def peso_total(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["peso_calculado"], errors="coerce").fillna(0).sum())


def peso_kg_total(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df.get("peso_kg", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())


def vol_total(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["vol_m3"], errors="coerce").fillna(0).sum())


def km_referencia(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["distancia_rodoviaria_est_km"], errors="coerce").fillna(0).max())


def qtd_paradas(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return int(df["destinatario"].fillna("").astype(str).nunique())


def ocupacao_perc(df: pd.DataFrame, veiculo: pd.Series) -> float:
    cap = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    if cap <= 0:
        return 0.0
    return (peso_total(df) / cap) * 100.0


def ocupacao_minima_kg(veiculo: pd.Series) -> float:
    cap  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    omin = safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100)
    return cap * (omin / 100.0)


def ocupacao_maxima_kg(veiculo: pd.Series) -> float:
    cap  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    omax = safe_float(veiculo.get("ocupacao_maxima_perc"), OCUPACAO_MAXIMA_PADRAO * 100)
    if omax <= 0:
        omax = OCUPACAO_MAXIMA_PADRAO * 100
    return cap * (omax / 100.0)


# ─────────────────────────────────────────────
# ORDENAÇÃO OPERACIONAL M5
# ─────────────────────────────────────────────

def _fase_bucket(row: pd.Series) -> int:
    prioridade = safe_float(row.get("prioridade_embarque_num", row.get("prioridade_embarque", np.nan)), np.nan)
    agendada = safe_bool(row.get("agendada"))
    folga    = safe_float(row.get("folga_dias"), 999)

    if not (isinstance(prioridade, float) and math.isnan(prioridade)) and prioridade > 0:
        return 0
    if agendada and folga == 0:
        return 1
    if agendada and folga == 1:
        return 2
    if (not agendada) and folga == 0:
        return 3
    if (not agendada) and folga == 1:
        return 4
    return 99


def precalcular_ordenacao_m5(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    temp = df.copy()
    temp[f"_id_str_{suffix}"]      = temp["id_linha_pipeline"].astype(str)
    temp[f"_cidade_key_{suffix}"]  = temp["cidade"].fillna("").astype(str).str.strip()
    temp[f"_uf_key_{suffix}"]      = temp["uf"].fillna("").astype(str).str.strip()
    temp[f"_cliente_key_{suffix}"] = temp["destinatario"].fillna("").astype(str).str.strip()

    if "sub_regiao" in temp.columns:
        temp[f"_subregiao_key_{suffix}"] = temp["sub_regiao"].fillna("").astype(str).str.strip()
    if "mesorregiao" in temp.columns:
        temp[f"_mesorregiao_key_{suffix}"] = temp["mesorregiao"].fillna("").astype(str).str.strip()

    folga   = pd.to_numeric(temp["folga_dias"], errors="coerce").fillna(999)
    ranking = pd.to_numeric(temp["ranking_prioridade_operacional"], errors="coerce").fillna(999)
    km      = pd.to_numeric(temp["distancia_rodoviaria_est_km"], errors="coerce").fillna(999999)
    peso    = pd.to_numeric(temp["peso_calculado"], errors="coerce").fillna(0.0)

    buckets = []
    prioridade_ord = []
    for _, row in temp.iterrows():
        buckets.append(_fase_bucket(row))
        p = safe_float(row.get("prioridade_embarque_num", row.get("prioridade_embarque", np.nan)), np.nan)
        prioridade_ord.append(p if not (isinstance(p, float) and math.isnan(p)) else 999.0)

    temp[f"_bucket_{suffix}"]       = buckets
    temp[f"_prioridade_ord_{suffix}"] = prioridade_ord
    temp[f"_folga_ord_{suffix}"]    = folga
    temp[f"_ranking_ord_{suffix}"]  = ranking
    temp[f"_km_ord_{suffix}"]       = km
    temp[f"_peso_ord_{suffix}"]     = -peso
    return temp


def ordenar_operacional_m5(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    bucket_col = f"_bucket_{suffix}"
    if bucket_col not in df.columns:
        df = precalcular_ordenacao_m5(df, suffix=suffix)
    return (
        df.sort_values(
            by=[f"_bucket_{suffix}", f"_prioridade_ord_{suffix}", f"_folga_ord_{suffix}",
                f"_ranking_ord_{suffix}", f"_km_ord_{suffix}", f"_peso_ord_{suffix}", f"_id_str_{suffix}"],
            ascending=[True, True, True, True, True, True, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
        .copy()
    )


def drop_internal_cols(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    cols = [c for c in df.columns if c.startswith("_") and c.endswith(f"_{suffix}")]
    return df.drop(columns=cols, errors="ignore").copy()


# ─────────────────────────────────────────────
# ORDENAÇÃO DE VEÍCULOS
# ─────────────────────────────────────────────

def veiculos_menor_para_maior(df: pd.DataFrame) -> pd.DataFrame:
    v = df.copy()
    v["_cap"] = pd.to_numeric(v["capacidade_peso_kg"], errors="coerce").fillna(0)
    return v.sort_values("_cap", ascending=True, kind="mergesort").drop(columns=["_cap"]).reset_index(drop=True)


def veiculos_maior_para_menor(df: pd.DataFrame) -> pd.DataFrame:
    v = df.copy()
    v["_cap"] = pd.to_numeric(v["capacidade_peso_kg"], errors="coerce").fillna(0)
    return v.sort_values("_cap", ascending=False, kind="mergesort").drop(columns=["_cap"]).reset_index(drop=True)


# ─────────────────────────────────────────────
# VALIDAÇÃO DE FECHAMENTO
# ─────────────────────────────────────────────

def validar_hard_constraints(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
) -> Tuple[bool, str]:
    if df_itens.empty:
        return False, "grupo_vazio"
    if not grupo_respeita_restricao(df_itens, veiculo):
        return False, "restricao_veiculo_incompativel"

    p   = peso_total(df_itens)
    v   = vol_total(df_itens)
    km  = km_referencia(df_itens)
    par = qtd_paradas(df_itens)

    cap_p  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    cap_v  = safe_float(veiculo.get("capacidade_vol_m3"), 0.0)
    max_km = safe_float(veiculo.get("max_km_distancia"), 0.0)
    max_e  = safe_int(veiculo.get("max_entregas"), 0)
    omax   = safe_float(veiculo.get("ocupacao_maxima_perc"), OCUPACAO_MAXIMA_PADRAO * 100) / 100.0

    if cap_p > 0 and p > cap_p * omax:
        return False, "excede_capacidade_peso"
    if cap_v > 0 and v > cap_v:
        return False, "excede_capacidade_volume"
    if max_e > 0 and par > max_e:
        return False, "excede_max_entregas"
    if max_km > 0 and km > max_km:
        return False, "excede_max_km"

    return True, "ok"


def validar_fechamento(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
) -> Tuple[bool, str]:
    ok, motivo = validar_hard_constraints(df_itens, veiculo)
    if not ok:
        return False, motivo
    omin = safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100) / 100.0
    ocup = ocupacao_perc(df_itens, veiculo) / 100.0
    cap  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    if cap > 0 and ocup < omin:
        return False, "abaixo_ocupacao_minima"
    return True, "ok"


def score_candidato(df_itens: pd.DataFrame, veiculo: pd.Series) -> Tuple[float, float, int, float]:
    ocup = ocupacao_perc(df_itens, veiculo)
    p    = peso_total(df_itens)
    cli  = qtd_paradas(df_itens)
    cap  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    return (round(ocup, 6), round(p, 6), int(cli), -cap)


# ─────────────────────────────────────────────
# SOLVER GUIADO (sem brute force amplo)
# ─────────────────────────────────────────────

def gerar_candidatos_guiados(
    blocks_df: pd.DataFrame,
    veiculo: pd.Series,
    cliente_key_col: str,
) -> List[pd.DataFrame]:
    """
    Gera candidatos sem brute force:
    - Conjunto inteiro
    - Prefixos por peso (top-k clientes mais pesados)
    - Melhor prefixo por gap à capacidade
    - Trocas 1-por-1 (max MAX_TROCAS_1)
    - Trocas 2-por-2 (max MAX_TROCAS_2)
    """
    if blocks_df.empty:
        return []

    from itertools import combinations as _combinations

    candidatos: List[pd.DataFrame] = []
    vistos: Set[Tuple[str, ...]] = set()

    cap_peso = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    omin     = safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100) / 100.0
    min_kg   = cap_peso * omin if cap_peso > 0 else 0.0

    base = blocks_df.head(min(len(blocks_df), MAX_CLIENTES_BASE)).copy().reset_index(drop=True)
    n    = len(base)

    def _add(cand: pd.DataFrame) -> None:
        if cand.empty:
            return
        chave = tuple(sorted(cand[cliente_key_col].astype(str).tolist()))
        if chave in vistos:
            return
        vistos.add(chave)
        candidatos.append(cand.copy())

    _add(base)

    for k in range(1, min(n, MAX_PREFIXOS_POR_PERFIL) + 1):
        cand = base.head(k).copy()
        p = float(cand["peso_total_bloco"].sum())
        if p > 0 and (cap_peso <= 0 or p <= cap_peso * 1.10):
            _add(cand)

    # Melhor prefixo por gap à capacidade
    melhor_k, melhor_gap = None, None
    acum = 0.0
    for k in range(1, n + 1):
        acum += safe_float(base.iloc[k - 1]["peso_total_bloco"], 0.0)
        gap = abs(cap_peso - acum) if cap_peso > 0 else acum
        if melhor_gap is None or gap < melhor_gap:
            melhor_gap = gap
            melhor_k = k

    if melhor_k is None:
        return candidatos

    prefixo = base.head(melhor_k).copy()
    fora    = base.iloc[melhor_k:].copy()

    trocas_1 = 0
    if len(prefixo) >= 1 and len(fora) >= 1:
        for i in range(len(prefixo)):
            for j in range(len(fora)):
                novo = pd.concat(
                    [prefixo.drop(prefixo.index[i]), fora.iloc[[j]]],
                    ignore_index=True,
                )
                p = float(novo["peso_total_bloco"].sum())
                if p >= min_kg * 0.90 and (cap_peso <= 0 or p <= cap_peso * 1.10):
                    _add(novo)
                trocas_1 += 1
                if trocas_1 >= MAX_TROCAS_1:
                    break
            if trocas_1 >= MAX_TROCAS_1:
                break

    trocas_2 = 0
    if len(prefixo) >= 2 and len(fora) >= 2:
        for rem in _combinations(range(len(prefixo)), 2):
            for add in _combinations(range(len(fora)), 2):
                novo = pd.concat(
                    [prefixo.drop(prefixo.index[list(rem)]), fora.iloc[list(add)]],
                    ignore_index=True,
                )
                p = float(novo["peso_total_bloco"].sum())
                if p >= min_kg * 0.90 and (cap_peso <= 0 or p <= cap_peso * 1.10):
                    _add(novo)
                trocas_2 += 1
                if trocas_2 >= MAX_TROCAS_2:
                    break
            if trocas_2 >= MAX_TROCAS_2:
                break

    return candidatos


def agrupar_blocos_cliente(
    df: pd.DataFrame,
    suffix: str,
    extra_agg: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Agrupa itens em blocos de cliente com métricas."""
    if df.empty:
        return pd.DataFrame()

    cliente_key_col = f"_cliente_key_{suffix}"
    bucket_col      = f"_bucket_{suffix}"
    ranking_col     = f"_ranking_ord_{suffix}"

    agg = {
        "peso_total_bloco":    ("peso_calculado", "sum"),
        "peso_kg_total_bloco": ("peso_kg", "sum") if "peso_kg" in df.columns else ("peso_calculado", "sum"),
        "volume_total_bloco":  ("vol_m3", "sum"),
        "km_referencia_bloco": ("distancia_rodoviaria_est_km", "max"),
        "qtd_linhas_bloco":    ("id_linha_pipeline", "count"),
        "prioridade_min":      (bucket_col, "min"),
        "ranking_min":         (ranking_col, "min"),
    }
    if extra_agg:
        agg.update(extra_agg)

    grouped = (
        df.groupby([cliente_key_col, "destinatario"], dropna=False)
        .agg(**agg)
        .reset_index()
        .sort_values(
            by=["peso_total_bloco", "prioridade_min", "ranking_min", cliente_key_col],
            ascending=[False, True, True, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )
    grouped["ordem_bloco_desc"] = range(1, len(grouped) + 1)
    return grouped


def materializar_candidato(
    df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    suffix: str,
) -> pd.DataFrame:
    cliente_key_col = f"_cliente_key_{suffix}"
    keys = set(blocks_df[cliente_key_col].tolist())
    candidato = df[df[cliente_key_col].isin(keys)].copy()
    return ordenar_operacional_m5(candidato, suffix=suffix).reset_index(drop=True)


def build_manifesto_m5(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
    manifesto_id: str,
    escala: str,
    sufixo_etapa: str,
    suffix: str,
    **extra_campos: Any,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_limpo = drop_internal_cols(df_itens, suffix)

    qtd_ctes = int(df_limpo["cte"].nunique()) if "cte" in df_limpo.columns else len(df_limpo)

    row = {
        "manifesto_id":                  manifesto_id,
        "tipo_manifesto":                f"pre_manifesto_bloco_5_{sufixo_etapa}",
        "escala_composicao":             escala,
        "veiculo_tipo":                  safe_text(veiculo.get("tipo") or veiculo.get("perfil")),
        "veiculo_perfil":                safe_text(veiculo.get("perfil")),
        "qtd_eixos":                     safe_int(veiculo.get("qtd_eixos"), 0),
        "qtd_itens":                     len(df_limpo),
        "qtd_ctes":                      qtd_ctes,
        "qtd_paradas":                   qtd_paradas(df_limpo),
        "base_carga_oficial":            round(peso_total(df_limpo), 3),
        "peso_total_kg":                 round(peso_kg_total(df_limpo), 3),
        "vol_total_m3":                  round(vol_total(df_limpo), 3),
        "km_referencia":                 round(km_referencia(df_limpo), 2),
        "ocupacao_oficial_perc":         round(ocupacao_perc(df_limpo, veiculo), 2),
        "capacidade_peso_kg_veiculo":    safe_float(veiculo.get("capacidade_peso_kg"), 0.0),
        "capacidade_vol_m3_veiculo":     safe_float(veiculo.get("capacidade_vol_m3"), 0.0),
        "max_entregas_veiculo":          safe_int(veiculo.get("max_entregas"), 0),
        "max_km_distancia_veiculo":      safe_float(veiculo.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc_veiculo":  safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100),
        "veiculo_exclusivo_flag":        False,
        "ignorar_ocupacao_minima":       False,
        "origem_modulo":                 5,
        "origem_etapa":                  f"m5_{sufixo_etapa}",
        **extra_campos,
    }

    df_manifesto = pd.DataFrame([row])
    df_itens_saida = df_limpo.copy()
    for k, v in row.items():
        df_itens_saida[k] = v

    return df_manifesto, df_itens_saida
