# ============================================================
# MÓDULO 6 — CONSOLIDAÇÃO DE MANIFESTOS
# M6.1: Consolidação dos pré-manifestos do bloco 5
# M6.2: Complemento de ocupação (adicionar itens remanescentes a manifestos abertos)
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
    validar_hard_constraints,
    veiculos_maior_para_menor,
)

OCUPACAO_MINIMA_PADRAO = 0.70
OCUPACAO_MAXIMA_PADRAO = 1.00


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _build_manifesto_consolidado(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
    manifesto_id: str,
    etapa: str,
    modulo: int,
) -> pd.DataFrame:
    cap_peso = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    ocup     = ocupacao_perc(df_itens, veiculo)
    qtd_ctes = int(df_itens["cte"].nunique()) if "cte" in df_itens.columns else len(df_itens)

    row = {
        "manifesto_id":                  manifesto_id,
        "tipo_manifesto":                f"manifesto_{etapa}",
        "veiculo_tipo":                  safe_text(veiculo.get("tipo") or veiculo.get("perfil")),
        "veiculo_perfil":                safe_text(veiculo.get("perfil")),
        "qtd_eixos":                     safe_int(veiculo.get("qtd_eixos"), 0),
        "qtd_itens":                     len(df_itens),
        "qtd_ctes":                      qtd_ctes,
        "qtd_paradas":                   qtd_paradas(df_itens),
        "base_carga_oficial":            round(peso_total(df_itens), 3),
        "peso_total_kg":                 round(peso_kg_total(df_itens), 3),
        "vol_total_m3":                  round(vol_total(df_itens), 3),
        "km_referencia":                 round(km_referencia(df_itens), 2),
        "ocupacao_oficial_perc":         round(ocup, 2),
        "capacidade_peso_kg_veiculo":    cap_peso,
        "capacidade_vol_m3_veiculo":     safe_float(veiculo.get("capacidade_vol_m3"), 0.0),
        "max_entregas_veiculo":          safe_int(veiculo.get("max_entregas"), 0),
        "max_km_distancia_veiculo":      safe_float(veiculo.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc_veiculo":  safe_float(veiculo.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100),
        "veiculo_exclusivo_flag":        False,
        "ignorar_ocupacao_minima":       False,
        "origem_modulo":                 modulo,
        "origem_etapa":                  etapa,
    }
    return pd.DataFrame([row])


# ─────────────────────────────────────────────
# M6.1 — CONSOLIDAÇÃO DOS PRÉ-MANIFESTOS
# ─────────────────────────────────────────────

def executar_m6_1_consolidacao_manifestos(
    df_premanifestos_m5_2: pd.DataFrame,
    df_itens_m5_2: pd.DataFrame,
    df_premanifestos_m5_3: pd.DataFrame,
    df_itens_m5_3: pd.DataFrame,
    df_premanifestos_m5_4: pd.DataFrame,
    df_itens_m5_4: pd.DataFrame,
    df_manifestos_m4: pd.DataFrame,
    df_itens_m4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M6.1 — Consolida todos os pré-manifestos do bloco 5 e os manifestos do bloco 4
    em uma lista única de manifestos oficiais, renumerados sequencialmente.
    """
    veiculos = df_veiculos_tratados.copy()
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    # Unificar todos os pré-manifestos
    frames_manifestos = []
    frames_itens      = []

    for df_m, df_i in [
        (df_manifestos_m4,       df_itens_m4),
        (df_premanifestos_m5_2,  df_itens_m5_2),
        (df_premanifestos_m5_3,  df_itens_m5_3),
        (df_premanifestos_m5_4,  df_itens_m5_4),
    ]:
        if df_m is not None and not df_m.empty:
            frames_manifestos.append(df_m)
        if df_i is not None and not df_i.empty:
            frames_itens.append(df_i)

    if not frames_manifestos:
        return {
            "df_manifestos_consolidados":  pd.DataFrame(),
            "df_itens_consolidados":       pd.DataFrame(),
            "resumo_m6_1": {"total_manifestos": 0, "total_itens": 0},
        }

    df_manifestos = pd.concat(frames_manifestos, ignore_index=True)
    df_itens      = pd.concat(frames_itens, ignore_index=True) if frames_itens else pd.DataFrame()

    # Renumerar manifestos sequencialmente: MAN_0001, MAN_0002, ...
    id_map: Dict[str, str] = {}
    for seq, old_id in enumerate(df_manifestos["manifesto_id"].tolist(), start=1):
        id_map[old_id] = f"MAN_{seq:04d}"

    df_manifestos["manifesto_id"] = df_manifestos["manifesto_id"].map(id_map)

    if not df_itens.empty and "manifesto_id" in df_itens.columns:
        df_itens["manifesto_id"] = df_itens["manifesto_id"].map(id_map).fillna(df_itens["manifesto_id"])

    return {
        "df_manifestos_consolidados": df_manifestos,
        "df_itens_consolidados":      df_itens,
        "resumo_m6_1": {
            "total_manifestos": len(df_manifestos),
            "total_itens":      len(df_itens),
        },
    }


# ─────────────────────────────────────────────
# M6.2 — COMPLEMENTO DE OCUPAÇÃO
# ─────────────────────────────────────────────

def _gap_capacidade_kg(df_itens_manifesto: pd.DataFrame, veiculo: pd.Series) -> float:
    cap  = safe_float(veiculo.get("capacidade_peso_kg"), 0.0)
    omax = safe_float(veiculo.get("ocupacao_maxima_perc"), OCUPACAO_MAXIMA_PADRAO * 100) / 100.0
    if cap <= 0:
        return 0.0
    return max(0.0, cap * omax - peso_total(df_itens_manifesto))


def _candidatos_complemento(
    df_remanescente: pd.DataFrame,
    gap_kg: float,
    veiculo: pd.Series,
    df_itens_manifesto: pd.DataFrame,
) -> pd.DataFrame:
    """
    Seleciona itens remanescentes que cabem no gap do manifesto.
    Critério: peso_calculado <= gap_kg, sem violar restrição de veículo.
    Ordena por prioridade operacional e peso decrescente.
    """
    if df_remanescente.empty or gap_kg <= 0:
        return pd.DataFrame()

    tipo = safe_text(veiculo.get("tipo") or veiculo.get("perfil"))

    # Filtrar por restrição de veículo
    def _compativel(row: pd.Series) -> bool:
        restricao = row.get("restricao_veiculo")
        if restricao is None:
            return True
        try:
            if pd.isna(restricao):
                return True
        except Exception:
            pass
        from app.pipeline.m5_common import veiculo_compativel
        return veiculo_compativel(tipo, restricao)

    mask_compativel = df_remanescente.apply(_compativel, axis=1)
    mask_peso       = pd.to_numeric(df_remanescente["peso_calculado"], errors="coerce").fillna(0) <= gap_kg

    candidatos = df_remanescente[mask_compativel & mask_peso].copy()
    if candidatos.empty:
        return pd.DataFrame()

    ranking = pd.to_numeric(candidatos.get("ranking_prioridade_operacional", pd.Series(999, index=candidatos.index)), errors="coerce").fillna(999)
    peso    = pd.to_numeric(candidatos["peso_calculado"], errors="coerce").fillna(0)

    candidatos["_rank_comp"] = ranking
    candidatos["_peso_comp"] = -peso
    candidatos = candidatos.sort_values(
        by=["_rank_comp", "_peso_comp"],
        ascending=[True, True],
        kind="mergesort",
    ).drop(columns=["_rank_comp", "_peso_comp"], errors="ignore")

    return candidatos


def executar_m6_2_complemento_ocupacao(
    df_manifestos_consolidados: pd.DataFrame,
    df_itens_consolidados: pd.DataFrame,
    df_remanescente_m5_4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M6.2 — Complemento de ocupação.
    Para cada manifesto com gap positivo, tenta adicionar itens remanescentes
    respeitando capacidade máxima, restrição de veículo e max_entregas.
    """
    if df_manifestos_consolidados.empty or df_remanescente_m5_4.empty:
        return {
            "df_manifestos_finais":  df_manifestos_consolidados,
            "df_itens_finais":       df_itens_consolidados,
            "df_nao_roteirizados":   df_remanescente_m5_4,
            "resumo_m6_2": {
                "total_manifestos":    len(df_manifestos_consolidados),
                "total_itens":         len(df_itens_consolidados),
                "itens_complementados": 0,
                "nao_roteirizados":    len(df_remanescente_m5_4),
            },
        }

    veiculos = df_veiculos_tratados.copy()
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    df_manifestos = df_manifestos_consolidados.copy()
    df_itens      = df_itens_consolidados.copy()
    df_saldo      = df_remanescente_m5_4.copy()

    itens_complementados_total = 0

    for idx, manifesto_row in df_manifestos.iterrows():
        mid = manifesto_row["manifesto_id"]

        # Reconstruir veiculo do manifesto
        tipo_veiculo = safe_text(manifesto_row.get("veiculo_tipo") or manifesto_row.get("veiculo_perfil"))
        veiculo_match = veiculos[
            (veiculos["tipo"].fillna("").astype(str).str.strip() == tipo_veiculo)
            | (veiculos["perfil"].fillna("").astype(str).str.strip() == tipo_veiculo)
        ]
        if veiculo_match.empty:
            continue
        veiculo = veiculo_match.iloc[0]

        # Itens atuais do manifesto
        df_itens_manifesto = df_itens[df_itens["manifesto_id"] == mid].copy()
        if df_itens_manifesto.empty:
            continue

        gap_kg = _gap_capacidade_kg(df_itens_manifesto, veiculo)
        if gap_kg <= 0:
            continue

        candidatos = _candidatos_complemento(df_saldo, gap_kg, veiculo, df_itens_manifesto)
        if candidatos.empty:
            continue

        # Adicionar itens greedy respeitando gap
        acum_peso = 0.0
        ids_adicionados: List[str] = []
        for _, item_row in candidatos.iterrows():
            item_peso = safe_float(item_row.get("peso_calculado"), 0.0)
            if acum_peso + item_peso > gap_kg:
                continue

            # Validar max_entregas
            max_e = safe_int(veiculo.get("max_entregas"), 0)
            if max_e > 0:
                paradas_atuais = int(df_itens_manifesto["destinatario"].fillna("").astype(str).nunique())
                novo_dest = safe_text(item_row.get("destinatario"))
                if novo_dest not in df_itens_manifesto["destinatario"].fillna("").astype(str).values:
                    if paradas_atuais >= max_e:
                        continue

            acum_peso += item_peso
            ids_adicionados.append(safe_text(item_row.get("id_linha_pipeline")))

            # Adicionar ao df_itens
            item_df = pd.DataFrame([item_row])
            for k, v in manifesto_row.items():
                item_df[k] = v
            df_itens = pd.concat([df_itens, item_df], ignore_index=True)
            df_itens_manifesto = pd.concat([df_itens_manifesto, item_df], ignore_index=True)

        if ids_adicionados:
            itens_complementados_total += len(ids_adicionados)
            df_saldo = df_saldo[~df_saldo["id_linha_pipeline"].isin(ids_adicionados)].copy()

            # Atualizar métricas do manifesto
            df_manifestos.at[idx, "qtd_itens"]           = len(df_itens_manifesto)
            df_manifestos.at[idx, "qtd_paradas"]         = qtd_paradas(df_itens_manifesto)
            df_manifestos.at[idx, "base_carga_oficial"]  = round(peso_total(df_itens_manifesto), 3)
            df_manifestos.at[idx, "vol_total_m3"]        = round(vol_total(df_itens_manifesto), 3)
            df_manifestos.at[idx, "ocupacao_oficial_perc"] = round(ocupacao_perc(df_itens_manifesto, veiculo), 2)

    # Marcar não roteirizados
    if not df_saldo.empty:
        if "motivo_nao_roteirizavel" not in df_saldo.columns:
            df_saldo["motivo_nao_roteirizavel"] = "sem_manifesto_disponivel_apos_complemento"

    return {
        "df_manifestos_finais":  df_manifestos,
        "df_itens_finais":       df_itens,
        "df_nao_roteirizados":   df_saldo,
        "resumo_m6_2": {
            "total_manifestos":     len(df_manifestos),
            "total_itens":          len(df_itens),
            "itens_complementados": itens_complementados_total,
            "nao_roteirizados":     len(df_saldo),
        },
    }
