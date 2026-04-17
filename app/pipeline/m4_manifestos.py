# ============================================================
# MÓDULO 4 — MANIFESTOS FECHADOS
# Bloco 4B1: Dedicados (veiculo_exclusivo_flag=True)
# Bloco 4B2: Filtro mínimo de peso
# Bloco 4B3: Fechamento normal por cliente
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import math
import re
import unicodedata
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

OCUPACAO_MINIMA_PADRAO  = 0.70   # 70%
OCUPACAO_MAXIMA_PADRAO  = 1.00   # 100%
MAX_COMBINACOES_CLIENTES = 50


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _safe_float(x: Any, default: float = 0.0) -> float:
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


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
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


_ALIASES_VEICULO = {
    "VUC":       {"VUC"},
    "3_4":       {"3_4", "TRES_QUARTOS", "TRES_QUARTO"},
    "TOCO":      {"TOCO"},
    "TRUCK":     {"TRUCK"},
    "CARRETA":   {"CARRETA"},
    "UTILITARIO": {"UTILITARIO", "UTILITARIOS", "FIORINO", "VAN"},
    "CARRO":     {"CARRO", "PASSEIO", "VEICULO_LEVE"},
}


def _expandir_alias(token: str) -> set:
    token = _normalizar_token(token)
    for canonico, grupo in _ALIASES_VEICULO.items():
        if token == canonico or token in grupo:
            return {canonico} | grupo
    return {token}


def _tokens_restricao(valor: Any) -> set:
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
    tokens: set = set()
    for p in partes:
        t = _normalizar_token(p)
        if t:
            tokens |= _expandir_alias(t)
    return tokens


def _veiculo_compativel(tipo_veiculo: Any, restricao: Any) -> bool:
    tokens_r = _tokens_restricao(restricao)
    if not tokens_r:
        return True
    tokens_v = _expandir_alias(tipo_veiculo)
    return bool(tokens_r & tokens_v)


def _grupo_compativel_com_veiculo(df_itens: pd.DataFrame, veiculo: pd.Series) -> bool:
    if "restricao_veiculo" not in df_itens.columns:
        return True
    tipo = veiculo.get("tipo") or veiculo.get("perfil", "")
    return all(
        _veiculo_compativel(tipo, r)
        for r in df_itens["restricao_veiculo"].tolist()
    )


def _peso_total(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["peso_calculado"], errors="coerce").fillna(0).sum())


def _vol_total(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["vol_m3"], errors="coerce").fillna(0).sum())


def _km_referencia(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["distancia_rodoviaria_est_km"], errors="coerce").fillna(0).max())


def _qtd_paradas(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    return int(df["destinatario"].fillna("").astype(str).nunique())


def _ocupacao(df: pd.DataFrame, veiculo: pd.Series) -> float:
    cap = _safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    if cap <= 0:
        return 0.0
    return _peso_total(df) / cap


def _validar_hard_constraints(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
    ignorar_ocupacao_minima: bool = False,
) -> Tuple[bool, str]:
    if df_itens.empty:
        return False, "grupo_vazio"

    if not _grupo_compativel_com_veiculo(df_itens, veiculo):
        return False, "restricao_veiculo_incompativel"

    peso  = _peso_total(df_itens)
    vol   = _vol_total(df_itens)
    km    = _km_referencia(df_itens)
    paradas = _qtd_paradas(df_itens)

    cap_peso = _safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    cap_vol  = _safe_float(veiculo.get("capacidade_vol_m3"), 0.0)
    max_km   = _safe_float(veiculo.get("max_km_distancia"), 0.0)
    max_ent  = _safe_int(veiculo.get("max_entregas"), 0)
    ocup_max = _safe_float(veiculo.get("ocupacao_maxima_perc"), OCUPACAO_MAXIMA_PADRAO * 100) / 100.0
    ocup_min = _safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100) / 100.0

    if cap_peso > 0 and peso > cap_peso * ocup_max:
        return False, "excede_capacidade_peso"
    if cap_vol > 0 and vol > cap_vol:
        return False, "excede_capacidade_volume"
    if max_ent > 0 and paradas > max_ent:
        return False, "excede_max_entregas"
    if max_km > 0 and km > max_km:
        return False, "excede_max_km"

    if not ignorar_ocupacao_minima:
        ocup = _ocupacao(df_itens, veiculo)
        if cap_peso > 0 and ocup < ocup_min:
            return False, "abaixo_ocupacao_minima"

    return True, "ok"


def _build_manifesto_id(prefixo: str, seq: int) -> str:
    return f"{prefixo}_{seq:04d}"


def _build_manifesto_row(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
    manifesto_id: str,
    modulo: int,
    etapa: str,
    ignorar_ocupacao_minima: bool = False,
) -> Dict[str, Any]:
    cap_peso = _safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    ocup = _ocupacao(df_itens, veiculo) * 100.0

    return {
        "manifesto_id":                  manifesto_id,
        "tipo_manifesto":                f"manifesto_{etapa}",
        "veiculo_tipo":                  str(veiculo.get("tipo") or veiculo.get("perfil", "")),
        "veiculo_perfil":                str(veiculo.get("perfil", "")),
        "qtd_eixos":                     _safe_int(veiculo.get("qtd_eixos"), 0),
        "qtd_itens":                     len(df_itens),
        "qtd_ctes":                      int(df_itens["cte"].nunique()) if "cte" in df_itens.columns else len(df_itens),
        "qtd_paradas":                   _qtd_paradas(df_itens),
        "base_carga_oficial":            round(_peso_total(df_itens), 3),
        "peso_total_kg":                 round(float(pd.to_numeric(df_itens.get("peso_kg", 0), errors="coerce").fillna(0).sum()), 3),
        "vol_total_m3":                  round(_vol_total(df_itens), 3),
        "km_referencia":                 round(_km_referencia(df_itens), 2),
        "ocupacao_oficial_perc":         round(ocup, 2),
        "capacidade_peso_kg_veiculo":    cap_peso,
        "capacidade_vol_m3_veiculo":     _safe_float(veiculo.get("capacidade_vol_m3"), 0.0),
        "max_entregas_veiculo":          _safe_int(veiculo.get("max_entregas"), 0),
        "max_km_distancia_veiculo":      _safe_float(veiculo.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc_veiculo":  _safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100),
        "veiculo_exclusivo_flag":        bool(df_itens["veiculo_exclusivo_flag"].any()) if "veiculo_exclusivo_flag" in df_itens.columns else False,
        "ignorar_ocupacao_minima":       ignorar_ocupacao_minima,
        "origem_modulo":                 modulo,
        "origem_etapa":                  etapa,
    }


# ─────────────────────────────────────────────
# ORDENAÇÃO DE VEÍCULOS
# ─────────────────────────────────────────────

def _veiculos_menor_para_maior(veiculos: pd.DataFrame) -> pd.DataFrame:
    v = veiculos.copy()
    v["_cap"] = pd.to_numeric(v["capacidade_peso_kg"], errors="coerce").fillna(0)
    return v.sort_values("_cap", ascending=True, kind="mergesort").drop(columns=["_cap"]).reset_index(drop=True)


def _veiculos_maior_para_menor(veiculos: pd.DataFrame) -> pd.DataFrame:
    v = veiculos.copy()
    v["_cap"] = pd.to_numeric(v["capacidade_peso_kg"], errors="coerce").fillna(0)
    return v.sort_values("_cap", ascending=False, kind="mergesort").drop(columns=["_cap"]).reset_index(drop=True)


# ─────────────────────────────────────────────
# BLOCO 4B1 — DEDICADOS
# ─────────────────────────────────────────────

def _processar_dedicados(
    df_dedicados: pd.DataFrame,
    veiculos: pd.DataFrame,
    manifesto_seq: int,
) -> Tuple[List[pd.DataFrame], List[pd.DataFrame], List[str], int]:
    manifestos_list: List[pd.DataFrame] = []
    itens_list:      List[pd.DataFrame] = []
    ids_manifestados: List[str] = []

    veiculos_ord = _veiculos_menor_para_maior(veiculos)

    # Agrupa por cliente
    clientes = df_dedicados["destinatario"].fillna("").astype(str).unique()
    for cliente in clientes:
        grupo = df_dedicados[df_dedicados["destinatario"].fillna("").astype(str) == cliente].copy()
        if grupo.empty:
            continue

        fechado = False
        for _, veiculo in veiculos_ord.iterrows():
            ok, motivo = _validar_hard_constraints(grupo, veiculo, ignorar_ocupacao_minima=True)
            if ok:
                mid = _build_manifesto_id("M4B1", manifesto_seq)
                manifesto_seq += 1
                row = _build_manifesto_row(grupo, veiculo, mid, 4, "m4_dedicado", ignorar_ocupacao_minima=True)
                manifestos_list.append(pd.DataFrame([row]))
                itens_grupo = grupo.copy()
                for k, v in row.items():
                    itens_grupo[k] = v
                itens_list.append(itens_grupo)
                ids_manifestados.extend(grupo["id_linha_pipeline"].tolist())
                fechado = True
                break

        if not fechado:
            # Se não fechou em nenhum veículo, vai para remanescente (não perde)
            pass

    return manifestos_list, itens_list, ids_manifestados, manifesto_seq


# ─────────────────────────────────────────────
# BLOCO 4B2 — FILTRO MÍNIMO DE PESO
# ─────────────────────────────────────────────

def _filtrar_peso_minimo(
    df_saldo: pd.DataFrame,
    veiculos: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove clientes cujo peso total < capacidade_menor_perfil × 70%.
    Esses vão para o remanescente do M5.
    """
    if veiculos.empty or df_saldo.empty:
        return df_saldo, pd.DataFrame(columns=df_saldo.columns)

    veiculos_ord = _veiculos_menor_para_maior(veiculos)
    menor_cap = _safe_float(veiculos_ord.iloc[0]["capacidade_peso_kg"], 0.0)
    ocup_min  = _safe_float(veiculos_ord.iloc[0].get("ocupacao_minima_perc", OCUPACAO_MINIMA_PADRAO * 100), OCUPACAO_MINIMA_PADRAO * 100) / 100.0
    peso_minimo = menor_cap * ocup_min

    if peso_minimo <= 0:
        return df_saldo, pd.DataFrame(columns=df_saldo.columns)

    # Peso por cliente
    peso_cliente = (
        df_saldo.groupby("destinatario", dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .rename(columns={"peso_calculado": "peso_total_cliente"})
    )
    df_saldo = df_saldo.merge(peso_cliente, on="destinatario", how="left")

    mask_elegivel = df_saldo["peso_total_cliente"] >= peso_minimo
    df_elegivel   = df_saldo[mask_elegivel].drop(columns=["peso_total_cliente"], errors="ignore").copy()
    df_nao_elegivel = df_saldo[~mask_elegivel].copy()
    df_nao_elegivel["motivo_nao_roteirizavel"] = "peso_abaixo_minimo_menor_perfil"
    df_nao_elegivel = df_nao_elegivel.drop(columns=["peso_total_cliente"], errors="ignore")

    return df_elegivel, df_nao_elegivel


# ─────────────────────────────────────────────
# BLOCO 4B3 — FECHAMENTO NORMAL POR CLIENTE
# ─────────────────────────────────────────────

def _fechar_cliente(
    df_cliente: pd.DataFrame,
    veiculos: pd.DataFrame,
    manifesto_seq: int,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], int, str]:
    """
    Tenta fechar manifesto para um cliente, do maior para o menor veículo.
    Retorna (manifesto_row, itens, novo_seq, motivo).
    """
    veiculos_ord = _veiculos_maior_para_menor(veiculos)

    for _, veiculo in veiculos_ord.iterrows():
        ok, motivo = _validar_hard_constraints(df_cliente, veiculo, ignorar_ocupacao_minima=False)
        if ok:
            mid = _build_manifesto_id("M4B3", manifesto_seq)
            manifesto_seq += 1
            row = _build_manifesto_row(df_cliente, veiculo, mid, 4, "m4_normal")
            df_manifesto = pd.DataFrame([row])
            df_itens = df_cliente.copy()
            for k, v in row.items():
                df_itens[k] = v
            return df_manifesto, df_itens, manifesto_seq, "ok"

    return None, None, manifesto_seq, "nenhum_perfil_adequado"


def _processar_fechamento_normal(
    df_elegivel: pd.DataFrame,
    veiculos: pd.DataFrame,
    manifesto_seq: int,
) -> Tuple[List[pd.DataFrame], List[pd.DataFrame], pd.DataFrame, int]:
    manifestos_list: List[pd.DataFrame] = []
    itens_list:      List[pd.DataFrame] = []
    remanescente_list: List[pd.DataFrame] = []

    # Ordenar clientes por peso total decrescente
    peso_cliente = (
        df_elegivel.groupby("destinatario", dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .rename(columns={"peso_calculado": "_peso_cli"})
        .sort_values("_peso_cli", ascending=False, kind="mergesort")
    )

    for _, cli_row in peso_cliente.iterrows():
        cliente = cli_row["destinatario"]
        grupo = df_elegivel[df_elegivel["destinatario"].fillna("").astype(str) == str(cliente)].copy()
        if grupo.empty:
            continue

        df_manifesto, df_itens, manifesto_seq, motivo = _fechar_cliente(
            grupo, veiculos, manifesto_seq
        )

        if df_manifesto is not None:
            manifestos_list.append(df_manifesto)
            itens_list.append(df_itens)
        else:
            grupo["motivo_nao_roteirizavel"] = motivo
            remanescente_list.append(grupo)

    df_remanescente = (
        pd.concat(remanescente_list, ignore_index=True)
        if remanescente_list else pd.DataFrame(columns=df_elegivel.columns)
    )

    return manifestos_list, itens_list, df_remanescente, manifesto_seq


# ─────────────────────────────────────────────
# FUNÇÃO PRINCIPAL
# ─────────────────────────────────────────────

def executar_m4_manifestos_fechados(
    df_input_oficial_bloco_4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M4 — Manifestos fechados.
    Fluxo: Dedicados → Filtro mínimo → Fechamento normal
    """
    carteira  = df_input_oficial_bloco_4.copy()
    veiculos  = df_veiculos_tratados.copy()
    total_entrada = len(carteira)

    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    manifestos_all: List[pd.DataFrame] = []
    itens_all:      List[pd.DataFrame] = []
    ids_manifestados: List[str] = []
    manifesto_seq = 1

    # ── Bloco 4B1: Dedicados ──
    mask_dedicado = carteira.get("veiculo_exclusivo_flag", pd.Series(False, index=carteira.index)).apply(_safe_bool)
    df_dedicados = carteira[mask_dedicado].copy()
    df_nao_dedicados = carteira[~mask_dedicado].copy()

    if not df_dedicados.empty:
        m_list, i_list, ids_ded, manifesto_seq = _processar_dedicados(
            df_dedicados, veiculos, manifesto_seq
        )
        manifestos_all.extend(m_list)
        itens_all.extend(i_list)
        ids_manifestados.extend(ids_ded)

    # Saldo após dedicados
    df_saldo = df_nao_dedicados.copy()
    # Adicionar dedicados não fechados de volta ao saldo
    ids_ded_set = set(ids_manifestados)
    df_ded_nao_fechados = df_dedicados[~df_dedicados["id_linha_pipeline"].isin(ids_ded_set)].copy()
    if not df_ded_nao_fechados.empty:
        df_ded_nao_fechados["motivo_nao_roteirizavel"] = "dedicado_sem_perfil_adequado"
        df_saldo = pd.concat([df_saldo, df_ded_nao_fechados], ignore_index=True)

    # ── Bloco 4B2: Filtro mínimo ──
    df_elegivel, df_abaixo_minimo = _filtrar_peso_minimo(df_saldo, veiculos)

    # ── Bloco 4B3: Fechamento normal ──
    m_list, i_list, df_remanescente_normal, manifesto_seq = _processar_fechamento_normal(
        df_elegivel, veiculos, manifesto_seq
    )
    manifestos_all.extend(m_list)
    itens_all.extend(i_list)

    # Remanescente final = abaixo do mínimo + não fechados no bloco 4B3
    df_remanescente = pd.concat(
        [df_abaixo_minimo, df_remanescente_normal],
        ignore_index=True,
    )

    # Consolidar outputs
    df_manifestos_m4 = (
        pd.concat(manifestos_all, ignore_index=True)
        if manifestos_all else pd.DataFrame()
    )
    df_itens_m4 = (
        pd.concat(itens_all, ignore_index=True)
        if itens_all else pd.DataFrame()
    )

    return {
        "df_manifestos_m4":    df_manifestos_m4,
        "df_itens_m4":         df_itens_m4,
        "df_remanescente_m4":  df_remanescente,
        "resumo_m4": {
            "total_entrada":           total_entrada,
            "dedicados_encontrados":   len(df_dedicados),
            "manifestos_fechados":     len(df_manifestos_m4),
            "itens_manifestados":      len(df_itens_m4),
            "remanescente":            len(df_remanescente),
        },
    }
