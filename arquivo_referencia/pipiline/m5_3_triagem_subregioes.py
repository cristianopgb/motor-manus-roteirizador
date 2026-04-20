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
# M5.3A - TRIAGEM DE SUBREGIÕES
# -----------------------------------------------------------------------------------------
# OBJETIVO
# - receber o remanescente oficial do M5.2
# - agrupar por subregiao
# - ordenar da maior massa para a menor
# - testar todos os perfis por subregião
# - nesta etapa olhar SOMENTE ocupação mínima >= 70%
# - nesta etapa NÃO olhar raio
# - nesta etapa NÃO olhar ocupação máxima
#
# SAÍDA
# - subregiões consolidadas
# - perfis elegíveis por subregião
# - saldo elegível para composição por subregião (M5.3B)
# - saldo não elegível que permanece como remanescente
# - tentativas auditáveis subregião x perfil
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


def _agrupar_saldo_por_subregiao(df_saldo: pd.DataFrame) -> pd.DataFrame:
    if df_saldo.empty:
        return pd.DataFrame()

    temp = df_saldo.copy()

    if "subregiao" not in temp.columns:
        raise ValueError("M5.3 exige coluna 'subregiao' no saldo de entrada.")

    grouped = (
        temp.groupby(["subregiao"], dropna=False, sort=False)
        .agg(
            peso_total_subregiao=("peso_calculado", "sum"),
            km_referencia_subregiao=("distancia_rodoviaria_est_km", "max"),
            qtd_clientes_subregiao=("destinatario", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_cidades_subregiao=("cidade", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_linhas_subregiao=("id_linha_pipeline", "count"),
        )
        .reset_index()
    )

    grouped["subregiao"] = grouped["subregiao"].fillna("").astype(str).str.strip()

    return grouped


def _ordenar_subregioes_por_massa(df_subregioes: pd.DataFrame) -> pd.DataFrame:
    if df_subregioes.empty:
        return df_subregioes.copy()

    return (
        df_subregioes.sort_values(
            by=["peso_total_subregiao", "subregiao"],
            ascending=[False, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
        .copy()
    )


def _avaliar_perfil_na_subregiao_agregada(
    row_subregiao: pd.Series,
    vehicle_row: pd.Series,
) -> Dict[str, Any]:
    peso_subregiao = safe_float(row_subregiao.get("peso_total_subregiao"), 0.0)
    km_subregiao = safe_float(row_subregiao.get("km_referencia_subregiao"), 0.0)
    qtd_clientes_subregiao = safe_int(row_subregiao.get("qtd_clientes_subregiao"), 0)
    qtd_cidades_subregiao = safe_int(row_subregiao.get("qtd_cidades_subregiao"), 0)
    qtd_linhas_subregiao = safe_int(row_subregiao.get("qtd_linhas_subregiao"), 0)

    capacidade_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocupacao = (peso_subregiao / capacidade_peso * 100.0) if capacidade_peso > 0 else 0.0

    status = "elegivel" if ocupacao >= 70.0 else "nao_elegivel"
    motivo = "atinge_ocupacao_minima_70" if status == "elegivel" else "abaixo_ocupacao_minima_70"

    return {
        "subregiao": safe_text(row_subregiao.get("subregiao")),
        "peso_total_subregiao": round(peso_subregiao, 3),
        "km_referencia_subregiao": round(km_subregiao, 2),
        "qtd_clientes_subregiao": qtd_clientes_subregiao,
        "qtd_cidades_subregiao": qtd_cidades_subregiao,
        "qtd_linhas_subregiao": qtd_linhas_subregiao,
        "perfil": safe_text(vehicle_row.get("perfil")),
        "tipo": safe_text(vehicle_row.get("tipo")),
        "capacidade_peso_kg": capacidade_peso,
        "capacidade_vol_m3": safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0),
        "max_entregas": safe_int(vehicle_row.get("max_entregas"), 0),
        "max_km_distancia": safe_float(vehicle_row.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc": safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0),
        "ocupacao_maxima_perc": safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0),
        "ocupacao_calculada_perc": round(ocupacao, 2),
        "status_perfil_subregiao": status,
        "motivo_status_perfil_subregiao": motivo,
        "regra_aplicada": "somente_ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }


def _montar_subregioes_consolidadas(
    df_subregioes_agg: pd.DataFrame,
    df_tentativas: pd.DataFrame,
) -> pd.DataFrame:
    if df_subregioes_agg.empty:
        return pd.DataFrame()

    base = df_subregioes_agg.copy()

    if df_tentativas.empty:
        base["qtd_perfis_elegiveis"] = 0
        base["qtd_perfis_descartados"] = 0
        base["subregiao_elegivel_m5_3"] = False
        base["motivo_status_subregiao_m5_3"] = "nenhum_perfil_atinge_ocupacao_minima_70"
        base["ordem_subregiao_m5_3"] = range(1, len(base) + 1)
        return base

    elegiveis = (
        df_tentativas.loc[df_tentativas["status_perfil_subregiao"] == "elegivel"]
        .groupby(["subregiao"], as_index=False)
        .agg(qtd_perfis_elegiveis=("perfil", "count"))
    )

    descartados = (
        df_tentativas.loc[df_tentativas["status_perfil_subregiao"] == "nao_elegivel"]
        .groupby(["subregiao"], as_index=False)
        .agg(qtd_perfis_descartados=("perfil", "count"))
    )

    base = base.merge(elegiveis, how="left", on=["subregiao"])
    base = base.merge(descartados, how="left", on=["subregiao"])

    base["qtd_perfis_elegiveis"] = pd.to_numeric(base["qtd_perfis_elegiveis"], errors="coerce").fillna(0).astype(int)
    base["qtd_perfis_descartados"] = pd.to_numeric(base["qtd_perfis_descartados"], errors="coerce").fillna(0).astype(int)

    base["subregiao_elegivel_m5_3"] = base["qtd_perfis_elegiveis"] > 0
    base["motivo_status_subregiao_m5_3"] = base["subregiao_elegivel_m5_3"].map(
        {
            True: "subregiao_tem_ao_menos_um_perfil_com_ocupacao_minima",
            False: "nenhum_perfil_atinge_ocupacao_minima_70",
        }
    )
    base["ordem_subregiao_m5_3"] = range(1, len(base) + 1)

    return base.reset_index(drop=True).copy()


def _filtrar_saldo_por_subregioes(
    df_saldo: pd.DataFrame,
    subregioes_set: set[str],
) -> pd.DataFrame:
    if df_saldo.empty or not subregioes_set:
        return pd.DataFrame(columns=df_saldo.columns)

    mask = df_saldo["subregiao"].fillna("").astype(str).str.strip().isin(subregioes_set)
    return df_saldo.loc[mask].copy().reset_index(drop=True)


def executar_m5_3_triagem_subregioes(
    df_remanescente_m5_2: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    saldo = normalize_saldo_m5(
        df_input=df_remanescente_m5_2,
        etapa="M5.3",
        require_geo=True,
        require_subregiao=True,
        require_mesorregiao=False,
    )
    veiculos = normalize_veiculos_m5(
        df_veiculos=df_veiculos_tratados,
        etapa="M5.3",
    )

    if saldo.empty:
        outputs_vazios = {
            "df_subregioes_consolidadas_m5_3": pd.DataFrame(),
            "df_perfis_viaveis_por_subregiao_m5_3": pd.DataFrame(),
            "df_perfis_elegiveis_por_subregiao_m5_3": pd.DataFrame(),
            "df_perfis_descartados_por_subregiao_m5_3": pd.DataFrame(),
            "df_saldo_elegivel_composicao_m5_3": pd.DataFrame(),
            "df_saldo_nao_elegivel_m5_3": pd.DataFrame(),
            "df_tentativas_triagem_subregioes_m5_3": pd.DataFrame(),
        }
        meta = {
            "resumo_m5_3": {
                "modulo": "M5.3A",
                "linhas_entrada": 0,
                "subregioes_total": 0,
                "subregioes_elegiveis": 0,
                "subregioes_nao_elegiveis": 0,
                "perfis_testados_total": 0,
                "perfis_elegiveis_total": 0,
                "perfis_descartados_total": 0,
                "linhas_saldo_elegivel_composicao_m5_3": 0,
                "linhas_saldo_nao_elegivel_m5_3": 0,
                "regra_m5_3": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
            }
        }
        return outputs_vazios, meta

    df_subregioes_agg = _agrupar_saldo_por_subregiao(saldo)
    df_subregioes_agg = _ordenar_subregioes_por_massa(df_subregioes_agg)

    veiculos_ord = _veiculos_menor_para_maior(veiculos)

    tentativas: List[Dict[str, Any]] = []

    for _, row_subregiao in df_subregioes_agg.iterrows():
        for _, row_veic in veiculos_ord.iterrows():
            tentativas.append(
                _avaliar_perfil_na_subregiao_agregada(
                    row_subregiao=row_subregiao,
                    vehicle_row=row_veic,
                )
            )

    df_tentativas_triagem_subregioes_m5_3 = pd.DataFrame(tentativas)
    df_perfis_viaveis_por_subregiao_m5_3 = df_tentativas_triagem_subregioes_m5_3.copy()

    df_perfis_elegiveis_por_subregiao_m5_3 = (
        df_perfis_viaveis_por_subregiao_m5_3.loc[
            df_perfis_viaveis_por_subregiao_m5_3["status_perfil_subregiao"] == "elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_perfis_descartados_por_subregiao_m5_3 = (
        df_perfis_viaveis_por_subregiao_m5_3.loc[
            df_perfis_viaveis_por_subregiao_m5_3["status_perfil_subregiao"] == "nao_elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_subregioes_consolidadas_m5_3 = _montar_subregioes_consolidadas(
        df_subregioes_agg=df_subregioes_agg,
        df_tentativas=df_tentativas_triagem_subregioes_m5_3,
    )

    subregioes_elegiveis = set(
        df_subregioes_consolidadas_m5_3.loc[
            df_subregioes_consolidadas_m5_3["subregiao_elegivel_m5_3"] == True,
            "subregiao",
        ].fillna("").astype(str).str.strip().tolist()
    )

    subregioes_nao_elegiveis = set(
        df_subregioes_consolidadas_m5_3.loc[
            df_subregioes_consolidadas_m5_3["subregiao_elegivel_m5_3"] == False,
            "subregiao",
        ].fillna("").astype(str).str.strip().tolist()
    )

    df_saldo_elegivel_composicao_m5_3 = _filtrar_saldo_por_subregioes(
        df_saldo=saldo,
        subregioes_set=subregioes_elegiveis,
    )

    df_saldo_nao_elegivel_m5_3 = _filtrar_saldo_por_subregioes(
        df_saldo=saldo,
        subregioes_set=subregioes_nao_elegiveis,
    )

    resumo_m5_3 = {
        "modulo": "M5.3A",
        "linhas_entrada": int(len(saldo)),
        "subregioes_total": int(len(df_subregioes_consolidadas_m5_3)),
        "subregioes_elegiveis": int(df_subregioes_consolidadas_m5_3["subregiao_elegivel_m5_3"].sum()) if not df_subregioes_consolidadas_m5_3.empty else 0,
        "subregioes_nao_elegiveis": int((df_subregioes_consolidadas_m5_3["subregiao_elegivel_m5_3"] == False).sum()) if not df_subregioes_consolidadas_m5_3.empty else 0,
        "perfis_testados_total": int(len(df_tentativas_triagem_subregioes_m5_3)),
        "perfis_elegiveis_total": int(len(df_perfis_elegiveis_por_subregiao_m5_3)),
        "perfis_descartados_total": int(len(df_perfis_descartados_por_subregiao_m5_3)),
        "linhas_saldo_elegivel_composicao_m5_3": int(len(df_saldo_elegivel_composicao_m5_3)),
        "linhas_saldo_nao_elegivel_m5_3": int(len(df_saldo_nao_elegivel_m5_3)),
        "regra_m5_3": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }

    outputs = {
        "df_subregioes_consolidadas_m5_3": df_subregioes_consolidadas_m5_3,
        "df_perfis_viaveis_por_subregiao_m5_3": df_perfis_viaveis_por_subregiao_m5_3,
        "df_perfis_elegiveis_por_subregiao_m5_3": df_perfis_elegiveis_por_subregiao_m5_3,
        "df_perfis_descartados_por_subregiao_m5_3": df_perfis_descartados_por_subregiao_m5_3,
        "df_saldo_elegivel_composicao_m5_3": df_saldo_elegivel_composicao_m5_3,
        "df_saldo_nao_elegivel_m5_3": df_saldo_nao_elegivel_m5_3,
        "df_tentativas_triagem_subregioes_m5_3": df_tentativas_triagem_subregioes_m5_3,
    }

    meta = {
        "resumo_m5_3": resumo_m5_3
    }

    return outputs, meta
