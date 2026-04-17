# ============================================================
# MÓDULO 5 — COMPOSIÇÃO TERRITORIAL
# M5.1: Triagem por cidades
# M5.2: Composição por cidades (solver guiado)
# M5.3: Composição por sub-regiões (solver guiado)
# M5.4: Composição por mesorregiões (solver guiado)
# Fiel ao contrato do motor original REC Transportes
# ============================================================
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from app.pipeline.m5_common import (
    safe_float, safe_int, safe_text, safe_bool,
    ensure_columns,
    precalcular_ordenacao_m5, ordenar_operacional_m5, drop_internal_cols,
    peso_total, peso_kg_total, vol_total, km_referencia, qtd_paradas,
    ocupacao_perc, ocupacao_minima_kg,
    validar_hard_constraints, validar_fechamento, score_candidato,
    gerar_candidatos_guiados, agrupar_blocos_cliente, materializar_candidato,
    build_manifesto_m5,
    veiculos_menor_para_maior, veiculos_maior_para_menor,
    grupo_respeita_restricao,
)

OCUPACAO_MINIMA_PADRAO = 0.70


# ─────────────────────────────────────────────
# HELPERS LOCAIS
# ─────────────────────────────────────────────

def _normalizar_saldo(df: pd.DataFrame, etapa: str) -> pd.DataFrame:
    """Valida colunas obrigatórias e normaliza o saldo de entrada."""
    obrigatorias = [
        "id_linha_pipeline", "peso_calculado", "vol_m3",
        "distancia_rodoviaria_est_km", "destinatario", "cidade", "uf",
        "latitude_destinatario", "longitude_destinatario",
        "folga_dias", "ranking_prioridade_operacional",
    ]
    faltando = [c for c in obrigatorias if c not in df.columns]
    if faltando:
        raise Exception(f"{etapa}: colunas obrigatórias ausentes: {faltando}")

    defaults = {
        "peso_kg":            pd.NA,
        "restricao_veiculo":  None,
        "agendada":           False,
        "sub_regiao":         None,
        "mesorregiao":        None,
        "prioridade_embarque_num": pd.NA,
    }
    return ensure_columns(df.copy(), defaults)


def _get_veiculos_para_escala(
    veiculos: pd.DataFrame,
    escala_key: str,
    escala_col: str,
    escala_val: str,
    uf_val: str,
) -> pd.DataFrame:
    """Retorna perfis de veículo elegíveis para uma escala (cidade/subregiao/mesorregiao)."""
    if veiculos.empty:
        return pd.DataFrame()
    # Filtro por escala se houver coluna correspondente
    if escala_col in veiculos.columns:
        mask = (
            (veiculos[escala_col].fillna("").astype(str).str.strip() == escala_val)
            & (veiculos["uf"].fillna("").astype(str).str.strip() == uf_val)
        ) if "uf" in veiculos.columns else (
            veiculos[escala_col].fillna("").astype(str).str.strip() == escala_val
        )
        resultado = veiculos[mask].copy()
        if not resultado.empty:
            return veiculos_maior_para_menor(resultado)
    # Fallback: retorna todos os veículos ordenados
    return veiculos_maior_para_menor(veiculos)


def _remover_clientes_fora_do_raio(
    df_itens: pd.DataFrame,
    veiculo: pd.Series,
    suffix: str,
) -> pd.DataFrame:
    """Remove blocos de cliente cujo km_referencia excede max_km do veículo."""
    max_km = safe_float(veiculo.get("max_km_distancia"), 0.0)
    if max_km <= 0:
        return df_itens

    cliente_key_col = f"_cliente_key_{suffix}"
    if cliente_key_col not in df_itens.columns:
        return df_itens

    km_por_cliente = (
        df_itens.groupby(cliente_key_col)["distancia_rodoviaria_est_km"]
        .max()
        .reset_index()
        .rename(columns={"distancia_rodoviaria_est_km": "_km_max_cli"})
    )
    df_merged = df_itens.merge(km_por_cliente, on=cliente_key_col, how="left")
    df_filtrado = df_merged[df_merged["_km_max_cli"] <= max_km].drop(columns=["_km_max_cli"], errors="ignore")
    return df_filtrado.copy()


# ─────────────────────────────────────────────
# SOLVER PRINCIPAL (reutilizado em M5.2, 5.3, 5.4)
# ─────────────────────────────────────────────

def _buscar_melhor_fechamento(
    df_grupo: pd.DataFrame,
    veiculos: pd.DataFrame,
    suffix: str,
    tentativas: List[Dict[str, Any]],
    label: str,
    label_val: str,
    uf_val: str,
    aplicar_filtro_raio: bool = False,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], str]:
    """
    Solver guiado: para cada perfil de veículo, gera candidatos e escolhe o melhor fechamento.
    """
    if df_grupo.empty:
        return None, None, "grupo_vazio"

    cliente_key_col = f"_cliente_key_{suffix}"
    melhor_df: Optional[pd.DataFrame] = None
    melhor_veiculo: Optional[pd.Series] = None
    melhor_score: Optional[Tuple] = None
    melhor_motivo = "nenhum_fechamento"
    tentativa_idx = 1

    for _, veiculo in veiculos.iterrows():
        df_candidato = df_grupo.copy()

        if aplicar_filtro_raio:
            df_candidato = _remover_clientes_fora_do_raio(df_candidato, veiculo, suffix)

        if df_candidato.empty:
            tentativas.append({
                label: label_val, "uf": uf_val,
                "tentativa_idx": tentativa_idx, "resultado": "falhou",
                "motivo": "sem_itens_apos_filtro_raio",
                "veiculo_tipo": safe_text(veiculo.get("tipo") or veiculo.get("perfil")),
            })
            tentativa_idx += 1
            continue

        # Filtrar blocos compatíveis com restrição do veículo
        blocks_df = agrupar_blocos_cliente(df_candidato, suffix)
        if blocks_df.empty:
            tentativa_idx += 1
            continue

        tipo_veiculo = safe_text(veiculo.get("tipo") or veiculo.get("perfil"))
        blocks_compativeis = blocks_df[
            blocks_df[cliente_key_col].apply(
                lambda ck: grupo_respeita_restricao(
                    df_candidato[df_candidato[cliente_key_col] == ck],
                    veiculo,
                )
            )
        ].copy()

        if blocks_compativeis.empty:
            tentativas.append({
                label: label_val, "uf": uf_val,
                "tentativa_idx": tentativa_idx, "resultado": "falhou",
                "motivo": "sem_blocos_compativeis",
                "veiculo_tipo": tipo_veiculo,
            })
            tentativa_idx += 1
            continue

        candidatos_blocos = gerar_candidatos_guiados(blocks_compativeis, veiculo, cliente_key_col)

        for blocks_cand in candidatos_blocos:
            cand = materializar_candidato(df_candidato, blocks_cand, suffix)
            ok, motivo = validar_fechamento(cand, veiculo)

            tentativas.append({
                label: label_val, "uf": uf_val,
                "tentativa_idx": tentativa_idx,
                "resultado": "fechado" if ok else "falhou",
                "motivo": motivo,
                "veiculo_tipo": tipo_veiculo,
                "qtd_itens": len(cand),
                "peso_total": round(peso_total(cand), 3),
                "ocupacao_perc": round(ocupacao_perc(cand, veiculo), 2),
            })
            tentativa_idx += 1
            melhor_motivo = motivo

            if not ok:
                continue

            s = score_candidato(cand, veiculo)
            if melhor_score is None or s > melhor_score:
                melhor_score   = s
                melhor_df      = cand.copy()
                melhor_veiculo = veiculo.copy()

    return melhor_df, melhor_veiculo, melhor_motivo


# ─────────────────────────────────────────────
# M5.1 — TRIAGEM POR CIDADES
# ─────────────────────────────────────────────

def executar_m5_1_triagem_cidades(
    df_remanescente_m4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M5.1 — Triagem por cidades.
    Separa cidades elegíveis (peso >= 70% do menor perfil) das não elegíveis.
    """
    saldo    = _normalizar_saldo(df_remanescente_m4, "M5.1")
    veiculos = df_veiculos_tratados.copy()

    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    if saldo.empty or veiculos.empty:
        return {
            "df_saldo_elegivel_m5_1":     saldo,
            "df_nao_elegivel_m5_1":       pd.DataFrame(columns=saldo.columns),
            "df_perfis_por_cidade_m5_1":  pd.DataFrame(),
            "resumo_m5_1": {"total_entrada": len(saldo), "elegivel": 0, "nao_elegivel": len(saldo)},
        }

    veiculos_ord = veiculos_menor_para_maior(veiculos)
    menor_cap    = safe_float(veiculos_ord.iloc[0]["capacidade_peso_kg"], 0.0)
    omin         = safe_float(veiculos_ord.iloc[0].get("ocupacao_minima_perc", OCUPACAO_MINIMA_PADRAO * 100), OCUPACAO_MINIMA_PADRAO * 100) / 100.0
    peso_minimo  = menor_cap * omin

    peso_por_cidade = (
        saldo.groupby(["cidade", "uf"], dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .rename(columns={"peso_calculado": "_peso_cidade"})
    )
    saldo = saldo.merge(peso_por_cidade, on=["cidade", "uf"], how="left")

    mask_elegivel = saldo["_peso_cidade"] >= peso_minimo
    df_elegivel   = saldo[mask_elegivel].drop(columns=["_peso_cidade"], errors="ignore").copy()
    df_nao_elegivel = saldo[~mask_elegivel].copy()
    df_nao_elegivel["motivo_nao_elegivel_m5_1"] = "peso_cidade_abaixo_minimo"
    df_nao_elegivel = df_nao_elegivel.drop(columns=["_peso_cidade"], errors="ignore")

    # Construir perfis elegíveis por cidade
    perfis_list = []
    for (cidade, uf), grupo in df_elegivel.groupby(["cidade", "uf"], dropna=False):
        for _, veiculo in veiculos.iterrows():
            ok, _ = validar_hard_constraints(grupo, veiculo)
            if ok:
                row = veiculo.to_dict()
                row["cidade"] = cidade
                row["uf"]     = uf
                perfis_list.append(row)

    df_perfis = pd.DataFrame(perfis_list) if perfis_list else pd.DataFrame()

    return {
        "df_saldo_elegivel_m5_1":    df_elegivel,
        "df_nao_elegivel_m5_1":      df_nao_elegivel,
        "df_perfis_por_cidade_m5_1": df_perfis,
        "resumo_m5_1": {
            "total_entrada": len(saldo),
            "elegivel":      len(df_elegivel),
            "nao_elegivel":  len(df_nao_elegivel),
        },
    }


# ─────────────────────────────────────────────
# M5.2 — COMPOSIÇÃO POR CIDADES
# ─────────────────────────────────────────────

def executar_m5_2_composicao_cidades(
    df_saldo_elegivel_m5_1: pd.DataFrame,
    df_perfis_por_cidade_m5_1: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M5.2 — Composição por cidades.
    Solver guiado: cidade por cidade, blocos de cliente, maximiza ocupação.
    """
    suffix = "m5_2"
    saldo  = _normalizar_saldo(df_saldo_elegivel_m5_1, "M5.2")
    veiculos = df_veiculos_tratados.copy()
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    if saldo.empty:
        return {
            "df_premanifestos_m5_2":  pd.DataFrame(),
            "df_itens_m5_2":          pd.DataFrame(),
            "df_remanescente_m5_2":   pd.DataFrame(),
            "df_tentativas_m5_2":     pd.DataFrame(),
            "resumo_m5_2": {"total_entrada": 0, "pre_manifestos": 0, "itens": 0, "remanescente": 0},
        }

    saldo = precalcular_ordenacao_m5(saldo, suffix)
    saldo = ordenar_operacional_m5(saldo, suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_list:      List[pd.DataFrame] = []
    tentativas:      List[Dict[str, Any]] = []
    ids_manifestados: List[str] = []
    manifesto_seq = 1

    # Ordenar cidades por massa total (maior primeiro)
    cidades_massa = (
        saldo.groupby(["cidade", "uf"], dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .sort_values("peso_calculado", ascending=False, kind="mergesort")
    )

    for _, cidade_row in cidades_massa.iterrows():
        cidade_val = safe_text(cidade_row["cidade"])
        uf_val     = safe_text(cidade_row["uf"])

        while True:
            city_df = saldo[
                (saldo[f"_cidade_key_{suffix}"] == cidade_val)
                & (saldo[f"_uf_key_{suffix}"] == uf_val)
            ].copy()

            if city_df.empty:
                break

            veiculos_cidade = _get_veiculos_para_escala(
                veiculos, "cidade", "cidade", cidade_val, uf_val
            )

            candidato, veiculo, motivo = _buscar_melhor_fechamento(
                city_df, veiculos_cidade, suffix, tentativas,
                "cidade", cidade_val, uf_val, aplicar_filtro_raio=False,
            )

            if candidato is None or veiculo is None:
                break

            mid = f"PM52_{manifesto_seq:04d}"
            manifesto_seq += 1

            df_m, df_i = build_manifesto_m5(
                candidato, veiculo, mid,
                escala=f"{cidade_val}|{uf_val}",
                sufixo_etapa="2_cidade",
                suffix=suffix,
                cidade=cidade_val, uf=uf_val,
            )
            manifestos_list.append(df_m)
            itens_list.append(df_i)

            ids_alocados = candidato["id_linha_pipeline"].tolist()
            ids_manifestados.extend(ids_alocados)
            saldo = saldo[~saldo["id_linha_pipeline"].isin(ids_alocados)].copy()

    df_remanescente = saldo.copy()
    df_remanescente = drop_internal_cols(df_remanescente, suffix)

    return {
        "df_premanifestos_m5_2":  pd.concat(manifestos_list, ignore_index=True) if manifestos_list else pd.DataFrame(),
        "df_itens_m5_2":          pd.concat(itens_list, ignore_index=True) if itens_list else pd.DataFrame(),
        "df_remanescente_m5_2":   df_remanescente,
        "df_tentativas_m5_2":     pd.DataFrame(tentativas),
        "resumo_m5_2": {
            "total_entrada":   len(df_saldo_elegivel_m5_1),
            "pre_manifestos":  len(manifestos_list),
            "itens":           sum(len(i) for i in itens_list),
            "remanescente":    len(df_remanescente),
        },
    }


# ─────────────────────────────────────────────
# M5.3 — COMPOSIÇÃO POR SUB-REGIÕES
# ─────────────────────────────────────────────

def executar_m5_3_composicao_subregioes(
    df_remanescente_m5_2: pd.DataFrame,
    df_nao_elegivel_m5_1: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M5.3 — Composição por sub-regiões.
    Unifica remanescente do M5.2 + não elegíveis do M5.1 e tenta composição por sub-região.
    """
    suffix = "m5_3"

    saldo_entrada = pd.concat(
        [df_remanescente_m5_2, df_nao_elegivel_m5_1],
        ignore_index=True,
    )

    if saldo_entrada.empty or "sub_regiao" not in saldo_entrada.columns:
        return {
            "df_premanifestos_m5_3":  pd.DataFrame(),
            "df_itens_m5_3":          pd.DataFrame(),
            "df_remanescente_m5_3":   saldo_entrada,
            "df_tentativas_m5_3":     pd.DataFrame(),
            "resumo_m5_3": {"total_entrada": len(saldo_entrada), "pre_manifestos": 0, "remanescente": len(saldo_entrada)},
        }

    saldo = _normalizar_saldo(saldo_entrada, "M5.3")
    veiculos = df_veiculos_tratados.copy()
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    saldo = precalcular_ordenacao_m5(saldo, suffix)
    saldo = ordenar_operacional_m5(saldo, suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_list:      List[pd.DataFrame] = []
    tentativas:      List[Dict[str, Any]] = []
    manifesto_seq = 1

    # Ordenar sub-regiões por massa
    subregioes_massa = (
        saldo.groupby(["sub_regiao", "uf"], dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .sort_values("peso_calculado", ascending=False, kind="mergesort")
    )

    for _, sr_row in subregioes_massa.iterrows():
        sr_val = safe_text(sr_row["sub_regiao"])
        uf_val = safe_text(sr_row["uf"])
        if not sr_val:
            continue

        while True:
            sr_df = saldo[
                (saldo["sub_regiao"].fillna("").astype(str).str.strip() == sr_val)
                & (saldo["uf"].fillna("").astype(str).str.strip() == uf_val)
            ].copy()

            if sr_df.empty:
                break

            candidato, veiculo, motivo = _buscar_melhor_fechamento(
                sr_df, veiculos, suffix, tentativas,
                "sub_regiao", sr_val, uf_val, aplicar_filtro_raio=True,
            )

            if candidato is None or veiculo is None:
                break

            mid = f"PM53_{manifesto_seq:04d}"
            manifesto_seq += 1

            df_m, df_i = build_manifesto_m5(
                candidato, veiculo, mid,
                escala=f"{sr_val}|{uf_val}",
                sufixo_etapa="3_subregiao",
                suffix=suffix,
                sub_regiao=sr_val, uf=uf_val,
            )
            manifestos_list.append(df_m)
            itens_list.append(df_i)

            ids_alocados = candidato["id_linha_pipeline"].tolist()
            saldo = saldo[~saldo["id_linha_pipeline"].isin(ids_alocados)].copy()

    df_remanescente = drop_internal_cols(saldo, suffix)

    return {
        "df_premanifestos_m5_3":  pd.concat(manifestos_list, ignore_index=True) if manifestos_list else pd.DataFrame(),
        "df_itens_m5_3":          pd.concat(itens_list, ignore_index=True) if itens_list else pd.DataFrame(),
        "df_remanescente_m5_3":   df_remanescente,
        "df_tentativas_m5_3":     pd.DataFrame(tentativas),
        "resumo_m5_3": {
            "total_entrada":  len(saldo_entrada),
            "pre_manifestos": len(manifestos_list),
            "remanescente":   len(df_remanescente),
        },
    }


# ─────────────────────────────────────────────
# M5.4 — COMPOSIÇÃO POR MESORREGIÕES
# ─────────────────────────────────────────────

def executar_m5_4_composicao_mesorregioes(
    df_remanescente_m5_3: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Dict[str, Any]:
    """
    M5.4 — Composição por mesorregiões.
    Última escala territorial antes do complemento de ocupação (M6.2).
    """
    suffix = "m5_4"

    if df_remanescente_m5_3.empty or "mesorregiao" not in df_remanescente_m5_3.columns:
        return {
            "df_premanifestos_m5_4":  pd.DataFrame(),
            "df_itens_m5_4":          pd.DataFrame(),
            "df_remanescente_m5_4":   df_remanescente_m5_3,
            "df_tentativas_m5_4":     pd.DataFrame(),
            "resumo_m5_4": {"total_entrada": len(df_remanescente_m5_3), "pre_manifestos": 0, "remanescente": len(df_remanescente_m5_3)},
        }

    saldo = _normalizar_saldo(df_remanescente_m5_3, "M5.4")
    veiculos = df_veiculos_tratados.copy()
    if "tipo" not in veiculos.columns and "perfil" in veiculos.columns:
        veiculos["tipo"] = veiculos["perfil"]

    saldo = precalcular_ordenacao_m5(saldo, suffix)
    saldo = ordenar_operacional_m5(saldo, suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_list:      List[pd.DataFrame] = []
    tentativas:      List[Dict[str, Any]] = []
    manifesto_seq = 1

    meso_massa = (
        saldo.groupby(["mesorregiao", "uf"], dropna=False)["peso_calculado"]
        .sum()
        .reset_index()
        .sort_values("peso_calculado", ascending=False, kind="mergesort")
    )

    for _, meso_row in meso_massa.iterrows():
        meso_val = safe_text(meso_row["mesorregiao"])
        uf_val   = safe_text(meso_row["uf"])
        if not meso_val:
            continue

        while True:
            meso_df = saldo[
                (saldo["mesorregiao"].fillna("").astype(str).str.strip() == meso_val)
                & (saldo["uf"].fillna("").astype(str).str.strip() == uf_val)
            ].copy()

            if meso_df.empty:
                break

            candidato, veiculo, motivo = _buscar_melhor_fechamento(
                meso_df, veiculos, suffix, tentativas,
                "mesorregiao", meso_val, uf_val, aplicar_filtro_raio=True,
            )

            if candidato is None or veiculo is None:
                break

            mid = f"PM54_{manifesto_seq:04d}"
            manifesto_seq += 1

            df_m, df_i = build_manifesto_m5(
                candidato, veiculo, mid,
                escala=f"{meso_val}|{uf_val}",
                sufixo_etapa="4_mesorregiao",
                suffix=suffix,
                mesorregiao=meso_val, uf=uf_val,
            )
            manifestos_list.append(df_m)
            itens_list.append(df_i)

            ids_alocados = candidato["id_linha_pipeline"].tolist()
            saldo = saldo[~saldo["id_linha_pipeline"].isin(ids_alocados)].copy()

    df_remanescente = drop_internal_cols(saldo, suffix)

    return {
        "df_premanifestos_m5_4":  pd.concat(manifestos_list, ignore_index=True) if manifestos_list else pd.DataFrame(),
        "df_itens_m5_4":          pd.concat(itens_list, ignore_index=True) if itens_list else pd.DataFrame(),
        "df_remanescente_m5_4":   df_remanescente,
        "df_tentativas_m5_4":     pd.DataFrame(tentativas),
        "resumo_m5_4": {
            "total_entrada":  len(df_remanescente_m5_3),
            "pre_manifestos": len(manifestos_list),
            "remanescente":   len(df_remanescente),
        },
    }
