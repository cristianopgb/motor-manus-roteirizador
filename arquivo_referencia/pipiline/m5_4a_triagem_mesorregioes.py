from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from app.pipeline.m5_common import (
    normalize_saldo_m5,
    normalize_veiculos_m5,
    safe_float,
    safe_int,
    safe_text,
)


# =========================================================================================
# M5.4A - TRIAGEM DE MESORREGIÕES
# -----------------------------------------------------------------------------------------
# OBJETIVO
# - receber o remanescente oficial do M5.3B
# - agrupar por mesorregiao
# - ordenar da maior massa para a menor
# - testar todos os perfis por mesorregião
# - nesta etapa olhar SOMENTE ocupação mínima >= 70%
# - nesta etapa NÃO olhar raio
# - nesta etapa NÃO olhar ocupação máxima
#
# SAÍDA
# - mesorregiões consolidadas
# - perfis elegíveis por mesorregião
# - saldo elegível para composição por mesorregião (M5.4B)
# - saldo não elegível que permanece como remanescente
# - tentativas auditáveis mesorregião x perfil
# =========================================================================================


def _veiculos_menor_para_maior(df_veiculos: pd.DataFrame) -> pd.DataFrame:
    temp = df_veiculos.copy()
    temp["_cap_peso_tmp"] = pd.to_numeric(temp["capacidade_peso_kg"], errors="coerce").fillna(0)
    temp["_cap_vol_tmp"] = pd.to_numeric(temp["capacidade_vol_m3"], errors="coerce").fillna(0)

    return (
        temp.sort_values(
            by=["_cap_peso_tmp", "_cap_vol_tmp", "tipo", "perfil"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        .drop(columns=["_cap_peso_tmp", "_cap_vol_tmp"], errors="ignore")
        .reset_index(drop=True)
        .copy()
    )


def _agrupar_saldo_por_mesorregiao(df_saldo: pd.DataFrame) -> pd.DataFrame:
    if df_saldo.empty:
        return pd.DataFrame()

    temp = df_saldo.copy()

    if "mesorregiao" not in temp.columns:
        raise ValueError("M5.4A exige coluna 'mesorregiao' no saldo de entrada.")

    grouped = (
        temp.groupby(["mesorregiao"], dropna=False, sort=False)
        .agg(
            peso_total_mesorregiao=("peso_calculado", "sum"),
            km_referencia_mesorregiao=("distancia_rodoviaria_est_km", "max"),
            qtd_clientes_mesorregiao=("destinatario", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_subregioes_mesorregiao=("subregiao", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_cidades_mesorregiao=("cidade", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_linhas_mesorregiao=("id_linha_pipeline", "count"),
        )
        .reset_index()
    )

    grouped["mesorregiao"] = grouped["mesorregiao"].fillna("").astype(str).str.strip()

    return grouped


def _ordenar_mesorregioes_por_massa(df_mesorregioes: pd.DataFrame) -> pd.DataFrame:
    if df_mesorregioes.empty:
        return df_mesorregioes.copy()

    return (
        df_mesorregioes.sort_values(
            by=["peso_total_mesorregiao", "mesorregiao"],
            ascending=[False, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
        .copy()
    )


def _avaliar_perfil_na_mesorregiao_agregada(
    row_mesorregiao: pd.Series,
    vehicle_row: pd.Series,
) -> Dict[str, Any]:
    peso_mesorregiao = safe_float(row_mesorregiao.get("peso_total_mesorregiao"), 0.0)
    km_mesorregiao = safe_float(row_mesorregiao.get("km_referencia_mesorregiao"), 0.0)
    qtd_clientes_mesorregiao = safe_int(row_mesorregiao.get("qtd_clientes_mesorregiao"), 0)
    qtd_subregioes_mesorregiao = safe_int(row_mesorregiao.get("qtd_subregioes_mesorregiao"), 0)
    qtd_cidades_mesorregiao = safe_int(row_mesorregiao.get("qtd_cidades_mesorregiao"), 0)
    qtd_linhas_mesorregiao = safe_int(row_mesorregiao.get("qtd_linhas_mesorregiao"), 0)

    capacidade_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocupacao = (peso_mesorregiao / capacidade_peso * 100.0) if capacidade_peso > 0 else 0.0

    status = "elegivel" if ocupacao >= 70.0 else "nao_elegivel"
    motivo = "atinge_ocupacao_minima_70" if status == "elegivel" else "abaixo_ocupacao_minima_70"

    return {
        "mesorregiao": safe_text(row_mesorregiao.get("mesorregiao")),
        "peso_total_mesorregiao": round(peso_mesorregiao, 3),
        "km_referencia_mesorregiao": round(km_mesorregiao, 2),
        "qtd_clientes_mesorregiao": qtd_clientes_mesorregiao,
        "qtd_subregioes_mesorregiao": qtd_subregioes_mesorregiao,
        "qtd_cidades_mesorregiao": qtd_cidades_mesorregiao,
        "qtd_linhas_mesorregiao": qtd_linhas_mesorregiao,
        "perfil": safe_text(vehicle_row.get("perfil")),
        "tipo": safe_text(vehicle_row.get("tipo")),
        "capacidade_peso_kg": capacidade_peso,
        "capacidade_vol_m3": safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0),
        "max_entregas": safe_int(vehicle_row.get("max_entregas"), 0),
        "max_km_distancia": safe_float(vehicle_row.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc": safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0),
        "ocupacao_maxima_perc": safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0),
        "ocupacao_calculada_perc": round(ocupacao, 2),
        "status_perfil_mesorregiao": status,
        "motivo_status_perfil_mesorregiao": motivo,
        "regra_aplicada": "somente_ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }


def _montar_mesorregioes_consolidadas(
    df_mesorregioes_agg: pd.DataFrame,
    df_tentativas: pd.DataFrame,
) -> pd.DataFrame:
    if df_mesorregioes_agg.empty:
        return pd.DataFrame()

    base = df_mesorregioes_agg.copy()

    if df_tentativas.empty:
        base["qtd_perfis_elegiveis"] = 0
        base["qtd_perfis_descartados"] = 0
        base["mesorregiao_elegivel_m5_4"] = False
        base["motivo_status_mesorregiao_m5_4"] = "nenhum_perfil_atinge_ocupacao_minima_70"
        base["ordem_mesorregiao_m5_4"] = range(1, len(base) + 1)
        return base

    elegiveis = (
        df_tentativas.loc[df_tentativas["status_perfil_mesorregiao"] == "elegivel"]
        .groupby(["mesorregiao"], as_index=False)
        .agg(qtd_perfis_elegiveis=("perfil", "count"))
    )

    descartados = (
        df_tentativas.loc[df_tentativas["status_perfil_mesorregiao"] == "nao_elegivel"]
        .groupby(["mesorregiao"], as_index=False)
        .agg(qtd_perfis_descartados=("perfil", "count"))
    )

    base = base.merge(elegiveis, how="left", on=["mesorregiao"])
    base = base.merge(descartados, how="left", on=["mesorregiao"])

    base["qtd_perfis_elegiveis"] = pd.to_numeric(base["qtd_perfis_elegiveis"], errors="coerce").fillna(0).astype(int)
    base["qtd_perfis_descartados"] = pd.to_numeric(base["qtd_perfis_descartados"], errors="coerce").fillna(0).astype(int)

    base["mesorregiao_elegivel_m5_4"] = base["qtd_perfis_elegiveis"] > 0
    base["motivo_status_mesorregiao_m5_4"] = base["mesorregiao_elegivel_m5_4"].map(
        {
            True: "mesorregiao_tem_ao_menos_um_perfil_com_ocupacao_minima",
            False: "nenhum_perfil_atinge_ocupacao_minima_70",
        }
    )
    base["ordem_mesorregiao_m5_4"] = range(1, len(base) + 1)

    return base.reset_index(drop=True).copy()


def _filtrar_saldo_por_mesorregioes(
    df_saldo: pd.DataFrame,
    mesorregioes_set: set[str],
) -> pd.DataFrame:
    if df_saldo.empty or not mesorregioes_set:
        return pd.DataFrame(columns=df_saldo.columns)

    mask = df_saldo["mesorregiao"].fillna("").astype(str).str.strip().isin(mesorregioes_set)
    return df_saldo.loc[mask].copy().reset_index(drop=True)


def executar_m5_4a_triagem_mesorregioes(
    df_remanescente_m5_3: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    saldo = normalize_saldo_m5(
        df_input=df_remanescente_m5_3,
        etapa="M5.4A",
        require_geo=True,
        require_subregiao=True,
        require_mesorregiao=True,
    )
    veiculos = normalize_veiculos_m5(
        df_veiculos=df_veiculos_tratados,
        etapa="M5.4A",
    )

    if saldo.empty:
        outputs_vazios = {
            "df_mesorregioes_consolidadas_m5_4": pd.DataFrame(),
            "df_perfis_viaveis_por_mesorregiao_m5_4": pd.DataFrame(),
            "df_perfis_elegiveis_por_mesorregiao_m5_4": pd.DataFrame(),
            "df_perfis_descartados_por_mesorregiao_m5_4": pd.DataFrame(),
            "df_saldo_elegivel_composicao_m5_4": pd.DataFrame(),
            "df_saldo_nao_elegivel_m5_4": pd.DataFrame(),
            "df_tentativas_triagem_mesorregioes_m5_4": pd.DataFrame(),
        }
        meta = {
            "resumo_m5_4a": {
                "modulo": "M5.4A",
                "linhas_entrada": 0,
                "mesorregioes_total": 0,
                "mesorregioes_elegiveis": 0,
                "mesorregioes_nao_elegiveis": 0,
                "perfis_testados_total": 0,
                "perfis_elegiveis_total": 0,
                "perfis_descartados_total": 0,
                "linhas_saldo_elegivel_composicao_m5_4": 0,
                "linhas_saldo_nao_elegivel_m5_4": 0,
                "regra_m5_4": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
            }
        }
        return outputs_vazios, meta

    df_mesorregioes_agg = _agrupar_saldo_por_mesorregiao(saldo)
    df_mesorregioes_agg = _ordenar_mesorregioes_por_massa(df_mesorregioes_agg)

    veiculos_ord = _veiculos_menor_para_maior(veiculos)

    tentativas: List[Dict[str, Any]] = []

    for _, row_meso in df_mesorregioes_agg.iterrows():
        for _, row_veic in veiculos_ord.iterrows():
            tentativas.append(
                _avaliar_perfil_na_mesorregiao_agregada(
                    row_mesorregiao=row_meso,
                    vehicle_row=row_veic,
                )
            )

    df_tentativas_triagem_mesorregioes_m5_4 = pd.DataFrame(tentativas)
    df_perfis_viaveis_por_mesorregiao_m5_4 = df_tentativas_triagem_mesorregioes_m5_4.copy()

    df_perfis_elegiveis_por_mesorregiao_m5_4 = (
        df_perfis_viaveis_por_mesorregiao_m5_4.loc[
            df_perfis_viaveis_por_mesorregiao_m5_4["status_perfil_mesorregiao"] == "elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_perfis_descartados_por_mesorregiao_m5_4 = (
        df_perfis_viaveis_por_mesorregiao_m5_4.loc[
            df_perfis_viaveis_por_mesorregiao_m5_4["status_perfil_mesorregiao"] == "nao_elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_mesorregioes_consolidadas_m5_4 = _montar_mesorregioes_consolidadas(
        df_mesorregioes_agg=df_mesorregioes_agg,
        df_tentativas=df_tentativas_triagem_mesorregioes_m5_4,
    )

    mesorregioes_elegiveis = set(
        df_mesorregioes_consolidadas_m5_4.loc[
            df_mesorregioes_consolidadas_m5_4["mesorregiao_elegivel_m5_4"] == True,
            "mesorregiao",
        ].fillna("").astype(str).str.strip().tolist()
    )

    mesorregioes_nao_elegiveis = set(
        df_mesorregioes_consolidadas_m5_4.loc[
            df_mesorregioes_consolidadas_m5_4["mesorregiao_elegivel_m5_4"] == False,
            "mesorregiao",
        ].fillna("").astype(str).str.strip().tolist()
    )

    df_saldo_elegivel_composicao_m5_4 = _filtrar_saldo_por_mesorregioes(
        df_saldo=saldo,
        mesorregioes_set=mesorregioes_elegiveis,
    )

    df_saldo_nao_elegivel_m5_4 = _filtrar_saldo_por_mesorregioes(
        df_saldo=saldo,
        mesorregioes_set=mesorregioes_nao_elegiveis,
    )

    resumo_m5_4a = {
        "modulo": "M5.4A",
        "linhas_entrada": int(len(saldo)),
        "mesorregioes_total": int(len(df_mesorregioes_consolidadas_m5_4)),
        "mesorregioes_elegiveis": int(df_mesorregioes_consolidadas_m5_4["mesorregiao_elegivel_m5_4"].sum()) if not df_mesorregioes_consolidadas_m5_4.empty else 0,
        "mesorregioes_nao_elegiveis": int((df_mesorregioes_consolidadas_m5_4["mesorregiao_elegivel_m5_4"] == False).sum()) if not df_mesorregioes_consolidadas_m5_4.empty else 0,
        "perfis_testados_total": int(len(df_tentativas_triagem_mesorregioes_m5_4)),
        "perfis_elegiveis_total": int(len(df_perfis_elegiveis_por_mesorregiao_m5_4)),
        "perfis_descartados_total": int(len(df_perfis_descartados_por_mesorregiao_m5_4)),
        "linhas_saldo_elegivel_composicao_m5_4": int(len(df_saldo_elegivel_composicao_m5_4)),
        "linhas_saldo_nao_elegivel_m5_4": int(len(df_saldo_nao_elegivel_m5_4)),
        "regra_m5_4": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }

    outputs = {
        "df_mesorregioes_consolidadas_m5_4": df_mesorregioes_consolidadas_m5_4,
        "df_perfis_viaveis_por_mesorregiao_m5_4": df_perfis_viaveis_por_mesorregiao_m5_4,
        "df_perfis_elegiveis_por_mesorregiao_m5_4": df_perfis_elegiveis_por_mesorregiao_m5_4,
        "df_perfis_descartados_por_mesorregiao_m5_4": df_perfis_descartados_por_mesorregiao_m5_4,
        "df_saldo_elegivel_composicao_m5_4": df_saldo_elegivel_composicao_m5_4,
        "df_saldo_nao_elegivel_m5_4": df_saldo_nao_elegivel_m5_4,
        "df_tentativas_triagem_mesorregioes_m5_4": df_tentativas_triagem_mesorregioes_m5_4,
    }

    meta = {
        "resumo_m5_4a": resumo_m5_4a
    }

    return outputs, meta
