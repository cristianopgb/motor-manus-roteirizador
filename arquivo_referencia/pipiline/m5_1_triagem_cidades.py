from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from app.pipeline.m5_common import (
    normalize_saldo_m5,
    normalize_veiculos_m5,
    agrupar_saldo_por_cidade,
    safe_float,
    safe_int,
    safe_text,
)


# =========================================================================================
# M5.1 - TRIAGEM DE CIDADES
# -----------------------------------------------------------------------------------------
# OBJETIVO
# - receber o remanescente global oficial do M4
# - agrupar por cidade
# - ordenar da maior massa para a menor
# - testar todos os perfis por cidade
# - nesta etapa olhar SOMENTE ocupação mínima >= 70%
# - nesta etapa NÃO olhar raio
# - nesta etapa NÃO olhar ocupação máxima
#
# SAÍDA
# - cidades consolidadas
# - perfis elegíveis por cidade
# - saldo elegível para composição por cidade (M5.2)
# - saldo não elegível para seguir ao agrupamento por subregião (M5.3)
# - tentativas auditáveis cidade x perfil
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


def _ordenar_cidades_por_massa(df_cidades: pd.DataFrame) -> pd.DataFrame:
    if df_cidades.empty:
        return df_cidades.copy()

    return (
        df_cidades.sort_values(
            by=["peso_total_cidade", "cidade", "uf"],
            ascending=[False, True, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
        .copy()
    )


def _avaliar_perfil_na_cidade_agregada(
    row_cidade: pd.Series,
    vehicle_row: pd.Series,
) -> Dict[str, Any]:
    peso_cidade = safe_float(row_cidade.get("peso_total_cidade"), 0.0)
    km_cidade = safe_float(row_cidade.get("km_referencia_cidade"), 0.0)
    qtd_clientes_cidade = safe_int(row_cidade.get("qtd_clientes_cidade"), 0)
    qtd_linhas_cidade = safe_int(row_cidade.get("qtd_linhas_cidade"), 0)

    capacidade_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocupacao = (peso_cidade / capacidade_peso * 100.0) if capacidade_peso > 0 else 0.0

    status = "elegivel" if ocupacao >= 70.0 else "nao_elegivel"
    motivo = "atinge_ocupacao_minima_70" if status == "elegivel" else "abaixo_ocupacao_minima_70"

    return {
        "cidade": safe_text(row_cidade.get("cidade")),
        "uf": safe_text(row_cidade.get("uf")),
        "peso_total_cidade": round(peso_cidade, 3),
        "km_referencia_cidade": round(km_cidade, 2),
        "qtd_clientes_cidade": qtd_clientes_cidade,
        "qtd_linhas_cidade": qtd_linhas_cidade,
        "perfil": safe_text(vehicle_row.get("perfil")),
        "tipo": safe_text(vehicle_row.get("tipo")),
        "capacidade_peso_kg": capacidade_peso,
        "capacidade_vol_m3": safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0),
        "max_entregas": safe_int(vehicle_row.get("max_entregas"), 0),
        "max_km_distancia": safe_float(vehicle_row.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc": safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0),
        "ocupacao_maxima_perc": safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0),
        "ocupacao_calculada_perc": round(ocupacao, 2),
        "status_perfil_cidade": status,
        "motivo_status_perfil_cidade": motivo,
        "regra_aplicada": "somente_ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }


def _montar_cidades_consolidadas(
    df_cidades_agg: pd.DataFrame,
    df_tentativas: pd.DataFrame,
) -> pd.DataFrame:
    if df_cidades_agg.empty:
        return pd.DataFrame()

    base = df_cidades_agg.copy()

    if df_tentativas.empty:
        base["qtd_perfis_elegiveis"] = 0
        base["qtd_perfis_descartados"] = 0
        base["cidade_elegivel_m5_1"] = False
        base["motivo_status_cidade_m5_1"] = "nenhum_perfil_atinge_ocupacao_minima_70"
        base["ordem_cidade_m5_1"] = range(1, len(base) + 1)
        return base

    elegiveis = (
        df_tentativas.loc[df_tentativas["status_perfil_cidade"] == "elegivel"]
        .groupby(["cidade", "uf"], as_index=False)
        .agg(qtd_perfis_elegiveis=("perfil", "count"))
    )

    descartados = (
        df_tentativas.loc[df_tentativas["status_perfil_cidade"] == "nao_elegivel"]
        .groupby(["cidade", "uf"], as_index=False)
        .agg(qtd_perfis_descartados=("perfil", "count"))
    )

    base = base.merge(elegiveis, how="left", on=["cidade", "uf"])
    base = base.merge(descartados, how="left", on=["cidade", "uf"])

    base["qtd_perfis_elegiveis"] = pd.to_numeric(base["qtd_perfis_elegiveis"], errors="coerce").fillna(0).astype(int)
    base["qtd_perfis_descartados"] = pd.to_numeric(base["qtd_perfis_descartados"], errors="coerce").fillna(0).astype(int)

    base["cidade_elegivel_m5_1"] = base["qtd_perfis_elegiveis"] > 0
    base["motivo_status_cidade_m5_1"] = base["cidade_elegivel_m5_1"].map(
        {
            True: "cidade_tem_ao_menos_um_perfil_com_ocupacao_minima",
            False: "nenhum_perfil_atinge_ocupacao_minima_70",
        }
    )
    base["ordem_cidade_m5_1"] = range(1, len(base) + 1)

    return base.reset_index(drop=True).copy()


def _filtrar_saldo_por_cidades(
    df_saldo: pd.DataFrame,
    cidades_set: set[tuple[str, str]],
) -> pd.DataFrame:
    if df_saldo.empty or not cidades_set:
        return pd.DataFrame(columns=df_saldo.columns)

    mask = df_saldo.apply(
        lambda row: (safe_text(row["cidade"]), safe_text(row["uf"])) in cidades_set,
        axis=1,
    )
    return df_saldo.loc[mask].copy().reset_index(drop=True)


def executar_m5_1_triagem_cidades(
    df_remanescente_roteirizavel_bloco_4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    saldo = normalize_saldo_m5(
        df_input=df_remanescente_roteirizavel_bloco_4,
        etapa="M5.1",
        require_geo=True,
        require_subregiao=False,
        require_mesorregiao=False,
    )
    veiculos = normalize_veiculos_m5(
        df_veiculos=df_veiculos_tratados,
        etapa="M5.1",
    )

    if saldo.empty:
        outputs_vazios = {
            "df_cidades_consolidadas_m5_1": pd.DataFrame(),
            "df_perfis_viaveis_por_cidade_m5_1": pd.DataFrame(),
            "df_perfis_elegiveis_por_cidade_m5_1": pd.DataFrame(),
            "df_perfis_descartados_por_cidade_m5_1": pd.DataFrame(),
            "df_saldo_elegivel_composicao_m5_1": pd.DataFrame(),
            "df_saldo_nao_elegivel_m5_1": pd.DataFrame(),
            "df_tentativas_triagem_cidades_m5_1": pd.DataFrame(),
        }
        meta = {
            "resumo_m5_1": {
                "modulo": "M5.1",
                "linhas_entrada": 0,
                "cidades_total": 0,
                "cidades_elegiveis": 0,
                "cidades_nao_elegiveis": 0,
                "perfis_testados_total": 0,
                "perfis_elegiveis_total": 0,
                "perfis_descartados_total": 0,
                "linhas_saldo_elegivel_composicao_m5_1": 0,
                "linhas_saldo_nao_elegivel_m5_1": 0,
                "regra_m5_1": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
            }
        }
        return outputs_vazios, meta

    df_cidades_agg = agrupar_saldo_por_cidade(saldo)
    df_cidades_agg = _ordenar_cidades_por_massa(df_cidades_agg)

    veiculos_ord = _veiculos_menor_para_maior(veiculos)

    tentativas: List[Dict[str, Any]] = []

    for _, row_cidade in df_cidades_agg.iterrows():
        for _, row_veic in veiculos_ord.iterrows():
            tentativas.append(
                _avaliar_perfil_na_cidade_agregada(
                    row_cidade=row_cidade,
                    vehicle_row=row_veic,
                )
            )

    df_tentativas_triagem_cidades_m5_1 = pd.DataFrame(tentativas)
    df_perfis_viaveis_por_cidade_m5_1 = df_tentativas_triagem_cidades_m5_1.copy()

    df_perfis_elegiveis_por_cidade_m5_1 = (
        df_perfis_viaveis_por_cidade_m5_1.loc[
            df_perfis_viaveis_por_cidade_m5_1["status_perfil_cidade"] == "elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_perfis_descartados_por_cidade_m5_1 = (
        df_perfis_viaveis_por_cidade_m5_1.loc[
            df_perfis_viaveis_por_cidade_m5_1["status_perfil_cidade"] == "nao_elegivel"
        ]
        .copy()
        .reset_index(drop=True)
    )

    df_cidades_consolidadas_m5_1 = _montar_cidades_consolidadas(
        df_cidades_agg=df_cidades_agg,
        df_tentativas=df_tentativas_triagem_cidades_m5_1,
    )

    cidades_elegiveis = set(
        df_cidades_consolidadas_m5_1.loc[
            df_cidades_consolidadas_m5_1["cidade_elegivel_m5_1"] == True,
            ["cidade", "uf"],
        ].apply(lambda row: (safe_text(row["cidade"]), safe_text(row["uf"])), axis=1).tolist()
    )

    cidades_nao_elegiveis = set(
        df_cidades_consolidadas_m5_1.loc[
            df_cidades_consolidadas_m5_1["cidade_elegivel_m5_1"] == False,
            ["cidade", "uf"],
        ].apply(lambda row: (safe_text(row["cidade"]), safe_text(row["uf"])), axis=1).tolist()
    )

    df_saldo_elegivel_composicao_m5_1 = _filtrar_saldo_por_cidades(
        df_saldo=saldo,
        cidades_set=cidades_elegiveis,
    )

    df_saldo_nao_elegivel_m5_1 = _filtrar_saldo_por_cidades(
        df_saldo=saldo,
        cidades_set=cidades_nao_elegiveis,
    )

    resumo_m5_1 = {
        "modulo": "M5.1",
        "linhas_entrada": int(len(saldo)),
        "cidades_total": int(len(df_cidades_consolidadas_m5_1)),
        "cidades_elegiveis": int(df_cidades_consolidadas_m5_1["cidade_elegivel_m5_1"].sum()) if not df_cidades_consolidadas_m5_1.empty else 0,
        "cidades_nao_elegiveis": int((df_cidades_consolidadas_m5_1["cidade_elegivel_m5_1"] == False).sum()) if not df_cidades_consolidadas_m5_1.empty else 0,
        "perfis_testados_total": int(len(df_tentativas_triagem_cidades_m5_1)),
        "perfis_elegiveis_total": int(len(df_perfis_elegiveis_por_cidade_m5_1)),
        "perfis_descartados_total": int(len(df_perfis_descartados_por_cidade_m5_1)),
        "linhas_saldo_elegivel_composicao_m5_1": int(len(df_saldo_elegivel_composicao_m5_1)),
        "linhas_saldo_nao_elegivel_m5_1": int(len(df_saldo_nao_elegivel_m5_1)),
        "regra_m5_1": "ocupacao_minima_sem_raio_sem_ocupacao_maxima",
    }

    outputs = {
        "df_cidades_consolidadas_m5_1": df_cidades_consolidadas_m5_1,
        "df_perfis_viaveis_por_cidade_m5_1": df_perfis_viaveis_por_cidade_m5_1,
        "df_perfis_elegiveis_por_cidade_m5_1": df_perfis_elegiveis_por_cidade_m5_1,
        "df_perfis_descartados_por_cidade_m5_1": df_perfis_descartados_por_cidade_m5_1,
        "df_saldo_elegivel_composicao_m5_1": df_saldo_elegivel_composicao_m5_1,
        "df_saldo_nao_elegivel_m5_1": df_saldo_nao_elegivel_m5_1,
        "df_tentativas_triagem_cidades_m5_1": df_tentativas_triagem_cidades_m5_1,
    }

    meta = {
        "resumo_m5_1": resumo_m5_1
    }

    return outputs, meta
