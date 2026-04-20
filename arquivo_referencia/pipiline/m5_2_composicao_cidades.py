from __future__ import annotations

from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.pipeline.m5_common import (
    normalize_saldo_m5,
    safe_float,
    safe_int,
    safe_text,
    precalcular_ordenacao_m5,
    ordenar_operacional_m5,
    peso_total,
    peso_auditoria_total,
    volume_total,
    km_referencia,
    qtd_paradas,
    ocupacao_perc,
    grupo_respeita_restricao_veiculo,
)


# =========================================================================================
# M5.2 - COMPOSIÇÃO POR CIDADE
# -----------------------------------------------------------------------------------------
# REGRA OPERACIONAL
# - processa cidade por cidade
# - dentro da cidade, trabalha por blocos de cliente
# - busca a MELHOR combinação de clientes da rodada
# - objetivo: tirar o máximo de cargas possível com a melhor ocupação válida
# - não prioriza simplesmente o maior cliente
# - pode gerar múltiplos pré-manifestos na mesma cidade
# - perfis elegíveis são orientativos; não desistir no primeiro perfil que falhar
#
# OTIMIZAÇÃO
# - sem brute force amplo
# - busca guiada com poda
# - pré-cálculo por cliente
# - sem recalcular massa desnecessariamente
#
# REGRAS DE PESO
# - base oficial = peso_calculado
# - peso_kg = auditoria
# - vol_m3 = volume
# - M5.2 não recria peso_calculado
# =========================================================================================


MAX_CLIENTES_BASE = 10
MAX_PREFIXOS_POR_PERFIL = 8
MAX_TROCAS_1 = 20
MAX_TROCAS_2 = 30


# -----------------------------------------------------------------------------------------
# Helpers locais
# -----------------------------------------------------------------------------------------
def _drop_internal_cols(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()

    cols_internal = [
        f"_id_str_{suffix}",
        f"_cidade_key_{suffix}",
        f"_uf_key_{suffix}",
        f"_cliente_key_{suffix}",
        f"_bucket_{suffix}",
        f"_prioridade_ord_{suffix}",
        f"_folga_ord_{suffix}",
        f"_ranking_ord_{suffix}",
        f"_km_ord_{suffix}",
        f"_peso_ord_{suffix}",
    ]
    existentes = [c for c in cols_internal if c in df.columns]
    if not existentes:
        return df.copy()
    return df.drop(columns=existentes, errors="ignore").copy()


def _ordenar_cidades_por_massa(df_saldo: pd.DataFrame) -> List[Tuple[str, str]]:
    if df_saldo.empty:
        return []

    agrupado = (
        df_saldo.groupby(["cidade", "uf"], dropna=False, sort=False)
        .agg(
            peso_total_cidade=("peso_calculado", "sum"),
            qtd_linhas_cidade=("id_linha_pipeline", "count"),
        )
        .reset_index()
        .sort_values(
            by=["peso_total_cidade", "cidade", "uf"],
            ascending=[False, True, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )

    return [(safe_text(r["cidade"]), safe_text(r["uf"])) for _, r in agrupado.iterrows()]


def _agrupar_blocos_cliente_na_cidade(city_df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if city_df.empty:
        return pd.DataFrame()

    temp = city_df.copy()

    cliente_key_col = f"_cliente_key_{suffix}"
    bucket_col = f"_bucket_{suffix}"
    ranking_col = f"_ranking_ord_{suffix}"

    grouped = (
        temp.groupby([cliente_key_col, "destinatario"], dropna=False)
        .agg(
            peso_total_bloco=("peso_calculado", "sum"),
            peso_kg_total_bloco=("peso_kg", "sum"),
            volume_total_bloco=("vol_m3", "sum"),
            km_referencia_bloco=("distancia_rodoviaria_est_km", "max"),
            qtd_linhas_bloco=("id_linha_pipeline", "count"),
            prioridade_min=(bucket_col, "min"),
            ranking_min=(ranking_col, "min"),
        )
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


def _materializar_candidato_por_blocos(
    city_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    suffix: str,
) -> pd.DataFrame:
    if city_df.empty or blocks_df.empty:
        return pd.DataFrame(columns=city_df.columns)

    cliente_key_col = f"_cliente_key_{suffix}"
    keys = set(blocks_df[cliente_key_col].tolist())

    candidato = city_df[city_df[cliente_key_col].isin(keys)].copy()
    candidato = ordenar_operacional_m5(candidato, suffix=suffix)
    return candidato.reset_index(drop=True)


def _bloco_compativel_com_veiculo(
    bloco_df: pd.DataFrame,
    vehicle_row: pd.Series,
) -> bool:
    if bloco_df is None or bloco_df.empty:
        return False
    return bool(grupo_respeita_restricao_veiculo(bloco_df, vehicle_row))


def _filtrar_blocos_compativeis_por_perfil(
    city_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    vehicle_row: pd.Series,
    suffix: str,
) -> pd.DataFrame:
    if city_df.empty or blocks_df.empty:
        return pd.DataFrame(columns=blocks_df.columns)

    cliente_key_col = f"_cliente_key_{suffix}"
    if cliente_key_col not in city_df.columns or cliente_key_col not in blocks_df.columns:
        return pd.DataFrame(columns=blocks_df.columns)

    blocos_validos: List[pd.Series] = []

    for _, bloco_row in blocks_df.iterrows():
        chave = bloco_row[cliente_key_col]
        bloco_df = city_df[city_df[cliente_key_col] == chave].copy()
        if _bloco_compativel_com_veiculo(bloco_df, vehicle_row):
            blocos_validos.append(bloco_row)

    if not blocos_validos:
        return pd.DataFrame(columns=blocks_df.columns)

    filtrado = pd.DataFrame(blocos_validos).reset_index(drop=True)
    return filtrado


def _validar_hard_constraints(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> Tuple[bool, str]:
    if df_itens.empty:
        return False, "grupo_vazio"

    if not grupo_respeita_restricao_veiculo(df_itens, vehicle_row):
        return False, "restricao_veiculo_incompativel"

    peso_oficial = peso_total(df_itens)
    volume = volume_total(df_itens)
    paradas = qtd_paradas(df_itens)
    km_ref = km_referencia(df_itens)

    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    cap_vol = safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0)
    max_entregas = safe_int(vehicle_row.get("max_entregas"), 0)
    max_km = safe_float(vehicle_row.get("max_km_distancia"), 0.0)
    ocup_max = safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0)

    if cap_peso > 0 and peso_oficial > cap_peso:
        return False, "excede_capacidade_peso"
    if cap_vol > 0 and volume > cap_vol:
        return False, "excede_capacidade_volume"
    if max_entregas > 0 and paradas > max_entregas:
        return False, "excede_max_entregas"
    if max_km > 0 and km_ref > max_km:
        return False, "excede_max_km"

    ocup = ocupacao_perc(df_itens, vehicle_row)
    if ocup > ocup_max:
        return False, "excede_ocupacao_maxima"

    return True, "ok"


def _validar_fechamento(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> Tuple[bool, str]:
    ok_hard, motivo_hard = _validar_hard_constraints(df_itens, vehicle_row)
    if not ok_hard:
        return False, motivo_hard

    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0)
    ocup = ocupacao_perc(df_itens, vehicle_row)

    if ocup < ocup_min:
        return False, "abaixo_ocupacao_minima"

    return True, "ok"


def _score_candidato(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> Tuple[float, float, int, float]:
    ocup = ocupacao_perc(df_itens, vehicle_row)
    peso = peso_total(df_itens)
    clientes = qtd_paradas(df_itens)
    cap = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)

    return (
        round(ocup, 6),
        round(peso, 6),
        int(clientes),
        -cap,
    )


def _tentativa_dict(
    cidade: str,
    uf: str,
    vehicle_row: Optional[pd.Series],
    resultado: str,
    motivo: str,
    df_candidato: Optional[pd.DataFrame],
    tentativa_idx: int,
    blocos_considerados: int,
) -> Dict[str, Any]:
    candidato = df_candidato if df_candidato is not None else pd.DataFrame()

    return {
        "cidade": cidade,
        "uf": uf,
        "tentativa_idx": tentativa_idx,
        "blocos_considerados": blocos_considerados,
        "veiculo_tipo_tentado": None if vehicle_row is None else safe_text(vehicle_row.get("tipo")),
        "veiculo_perfil_tentado": None if vehicle_row is None else safe_text(vehicle_row.get("perfil")),
        "resultado": resultado,
        "motivo": motivo,
        "qtd_itens_candidato": int(len(candidato)),
        "qtd_paradas_candidato": qtd_paradas(candidato),
        "peso_total_candidato": round(peso_total(candidato), 3),
        "peso_kg_total_candidato": round(peso_auditoria_total(candidato), 3),
        "volume_total_candidato": round(volume_total(candidato), 3),
        "km_referencia_candidato": round(km_referencia(candidato), 2),
        "ocupacao_perc_candidato": round(ocupacao_perc(candidato, vehicle_row), 2)
        if vehicle_row is not None and not candidato.empty
        else 0.0,
    }


def _build_manifesto_id(seq: int) -> str:
    return f"PM52_{seq:04d}"


def _build_manifesto(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    manifesto_id: str,
    cidade: str,
    uf: str,
    suffix: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_itens_limpo = _drop_internal_cols(df_itens, suffix=suffix)

    qtd_itens = int(len(df_itens_limpo))
    qtd_ctes = int(df_itens_limpo["cte"].nunique(dropna=True)) if "cte" in df_itens_limpo.columns else qtd_itens

    manifesto = {
        "manifesto_id": manifesto_id,
        "tipo_manifesto": "pre_manifesto_bloco_5_2_cidade",
        "cidade": cidade,
        "uf": uf,
        "veiculo_tipo": safe_text(vehicle_row.get("tipo")),
        "veiculo_perfil": safe_text(vehicle_row.get("perfil")),
        "qtd_itens": qtd_itens,
        "qtd_ctes": qtd_ctes,
        "qtd_paradas": qtd_paradas(df_itens_limpo),
        "base_carga_oficial": round(peso_total(df_itens_limpo), 3),
        "peso_total_kg": round(peso_auditoria_total(df_itens_limpo), 3),
        "vol_total_m3": round(volume_total(df_itens_limpo), 3),
        "km_referencia": round(km_referencia(df_itens_limpo), 2),
        "ocupacao_oficial_perc": round(ocupacao_perc(df_itens_limpo, vehicle_row), 2),
        "capacidade_peso_kg_veiculo": safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0),
        "capacidade_vol_m3_veiculo": safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0),
        "max_entregas_veiculo": safe_int(vehicle_row.get("max_entregas"), 0),
        "max_km_distancia_veiculo": safe_float(vehicle_row.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc_veiculo": safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0),
        "ocupacao_maxima_perc_veiculo": safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0),
        "ignorar_ocupacao_minima": False,
        "origem_modulo": 5,
        "origem_etapa": "m5_2_composicao_cidade",
    }

    df_manifesto = pd.DataFrame([manifesto])

    df_itens_saida = df_itens_limpo.copy()
    for k, v in manifesto.items():
        df_itens_saida[k] = v

    return df_manifesto, df_itens_saida


def _get_eligible_vehicles_for_city(
    city: str,
    uf: str,
    perfis_elegiveis_df: pd.DataFrame,
) -> pd.DataFrame:
    base = perfis_elegiveis_df[
        (perfis_elegiveis_df["cidade"].fillna("").astype(str).str.strip() == city)
        & (perfis_elegiveis_df["uf"].fillna("").astype(str).str.strip() == uf)
    ].copy()

    if base.empty:
        return pd.DataFrame()

    for col in [
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]:
        if col in base.columns:
            base[col] = pd.to_numeric(base[col], errors="coerce")

    base = base.sort_values(
        by=["capacidade_peso_kg", "capacidade_vol_m3", "tipo", "perfil"],
        ascending=[False, False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)

    return base


# -----------------------------------------------------------------------------------------
# Solver guiado
# -----------------------------------------------------------------------------------------
def _selecionar_blocos_base_para_busca(blocks_df: pd.DataFrame) -> pd.DataFrame:
    if blocks_df.empty:
        return blocks_df.copy()

    return blocks_df.head(min(len(blocks_df), MAX_CLIENTES_BASE)).copy().reset_index(drop=True)


def _gerar_candidatos_guiados(
    blocks_df: pd.DataFrame,
    vehicle_row: pd.Series,
    cliente_key_col: str,
) -> List[pd.DataFrame]:
    """
    Gera candidatos sem brute force amplo:
    - cidade inteira
    - prefixos por peso
    - prefixos + troca de 1
    - prefixos + troca de 2
    """
    if blocks_df.empty:
        return []

    candidatos: List[pd.DataFrame] = []
    vistos: set[Tuple[str, ...]] = set()

    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0)
    min_kg = cap_peso * (ocup_min / 100.0) if cap_peso > 0 else 0.0

    base = _selecionar_blocos_base_para_busca(blocks_df)
    n = len(base)

    def _adicionar(df_candidate: pd.DataFrame) -> None:
        if df_candidate.empty:
            return
        chave = tuple(sorted(df_candidate[cliente_key_col].astype(str).tolist()))
        if chave in vistos:
            return
        vistos.add(chave)
        candidatos.append(df_candidate.copy())

    _adicionar(base)

    for k in range(1, min(n, MAX_PREFIXOS_POR_PERFIL) + 1):
        cand = base.head(k).copy()
        peso = float(cand["peso_total_bloco"].sum())
        if peso > 0 and (peso <= cap_peso * 1.10 or cap_peso <= 0):
            _adicionar(cand)

    melhor_k = None
    melhor_gap = None
    acumulado = 0.0
    for k in range(1, n + 1):
        acumulado += safe_float(base.iloc[k - 1]["peso_total_bloco"], 0.0)
        gap = abs(cap_peso - acumulado) if cap_peso > 0 else acumulado
        if melhor_gap is None or gap < melhor_gap:
            melhor_gap = gap
            melhor_k = k

    if melhor_k is None:
        return candidatos

    prefixo_base = base.head(melhor_k).copy()
    fora_prefixo = base.iloc[melhor_k:].copy()

    trocas_1 = 0
    if len(prefixo_base) >= 1 and len(fora_prefixo) >= 1:
        idxs_prefixo = list(range(len(prefixo_base)))
        idxs_fora = list(range(len(fora_prefixo)))
        for i in idxs_prefixo:
            for j in idxs_fora:
                novo = pd.concat(
                    [
                        prefixo_base.drop(prefixo_base.index[i]),
                        fora_prefixo.iloc[[j]],
                    ],
                    ignore_index=True,
                )
                peso = float(novo["peso_total_bloco"].sum())
                if peso >= min_kg * 0.90 and (cap_peso <= 0 or peso <= cap_peso * 1.10):
                    _adicionar(novo)
                trocas_1 += 1
                if trocas_1 >= MAX_TROCAS_1:
                    break
            if trocas_1 >= MAX_TROCAS_1:
                break

    trocas_2 = 0
    if len(prefixo_base) >= 2 and len(fora_prefixo) >= 2:
        idxs_prefixo = list(range(len(prefixo_base)))
        idxs_fora = list(range(len(fora_prefixo)))
        for rem in combinations(idxs_prefixo, 2):
            for add in combinations(idxs_fora, 2):
                novo = pd.concat(
                    [
                        prefixo_base.drop(prefixo_base.index[list(rem)]),
                        fora_prefixo.iloc[list(add)],
                    ],
                    ignore_index=True,
                )
                peso = float(novo["peso_total_bloco"].sum())
                if peso >= min_kg * 0.90 and (cap_peso <= 0 or peso <= cap_peso * 1.10):
                    _adicionar(novo)
                trocas_2 += 1
                if trocas_2 >= MAX_TROCAS_2:
                    break
            if trocas_2 >= MAX_TROCAS_2:
                break

    return candidatos


def _buscar_melhor_fechamento_na_cidade(
    city_df: pd.DataFrame,
    perfis_elegiveis_df: pd.DataFrame,
    cidade: str,
    uf: str,
    tentativas: List[Dict[str, Any]],
    suffix: str,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], str]:
    if city_df.empty:
        return None, None, "cidade_vazia"

    vehicles_city = _get_eligible_vehicles_for_city(
        city=cidade,
        uf=uf,
        perfis_elegiveis_df=perfis_elegiveis_df,
    )
    if vehicles_city.empty:
        tentativas.append(
            _tentativa_dict(
                cidade=cidade,
                uf=uf,
                vehicle_row=None,
                resultado="falhou",
                motivo="sem_perfil_elegivel_na_cidade",
                df_candidato=city_df,
                tentativa_idx=1,
                blocos_considerados=0,
            )
        )
        return None, None, "sem_perfil_elegivel_na_cidade"

    blocks_df = _agrupar_blocos_cliente_na_cidade(city_df, suffix=suffix)
    if blocks_df.empty:
        return None, None, "sem_blocos_na_cidade"

    cliente_key_col = f"_cliente_key_{suffix}"

    melhor_df: Optional[pd.DataFrame] = None
    melhor_vehicle: Optional[pd.Series] = None
    melhor_score: Optional[Tuple[float, float, int, float]] = None
    melhor_motivo = "nenhum_fechamento"

    tentativa_idx = 1

    for _, vehicle_row in vehicles_city.iterrows():
        blocks_df_compativeis = _filtrar_blocos_compativeis_por_perfil(
            city_df=city_df,
            blocks_df=blocks_df,
            vehicle_row=vehicle_row,
            suffix=suffix,
        )

        if blocks_df_compativeis.empty:
            tentativas.append(
                _tentativa_dict(
                    cidade=cidade,
                    uf=uf,
                    vehicle_row=vehicle_row,
                    resultado="falhou",
                    motivo="sem_blocos_compativeis_com_perfil",
                    df_candidato=pd.DataFrame(),
                    tentativa_idx=tentativa_idx,
                    blocos_considerados=0,
                )
            )
            tentativa_idx += 1
            continue

        candidatos_blocos = _gerar_candidatos_guiados(
            blocks_df=blocks_df_compativeis,
            vehicle_row=vehicle_row,
            cliente_key_col=cliente_key_col,
        )

        if not candidatos_blocos:
            tentativas.append(
                _tentativa_dict(
                    cidade=cidade,
                    uf=uf,
                    vehicle_row=vehicle_row,
                    resultado="falhou",
                    motivo="sem_candidato_gerado",
                    df_candidato=pd.DataFrame(),
                    tentativa_idx=tentativa_idx,
                    blocos_considerados=0,
                )
            )
            tentativa_idx += 1
            continue

        for blocks_candidato in candidatos_blocos:
            candidato = _materializar_candidato_por_blocos(city_df, blocks_candidato, suffix=suffix)
            ok, motivo = _validar_fechamento(candidato, vehicle_row)

            tentativas.append(
                _tentativa_dict(
                    cidade=cidade,
                    uf=uf,
                    vehicle_row=vehicle_row,
                    resultado="fechado" if ok else "falhou",
                    motivo=motivo,
                    df_candidato=candidato,
                    tentativa_idx=tentativa_idx,
                    blocos_considerados=int(len(blocks_candidato)),
                )
            )
            tentativa_idx += 1
            melhor_motivo = motivo

            if not ok:
                continue

            score = _score_candidato(candidato, vehicle_row)

            if melhor_score is None or score > melhor_score:
                melhor_score = score
                melhor_df = candidato.copy()
                melhor_vehicle = vehicle_row.copy()

    if melhor_df is None or melhor_vehicle is None:
        return None, None, melhor_motivo

    return melhor_df, melhor_vehicle, "ok"


# -----------------------------------------------------------------------------------------
# Função principal
# -----------------------------------------------------------------------------------------
def executar_m5_2_composicao_cidades(
    df_saldo_elegivel_composicao_m5_1: pd.DataFrame,
    df_perfis_elegiveis_por_cidade_m5_1: pd.DataFrame,
    rodada_id: Optional[str] = None,
    data_base_roteirizacao: Optional[Any] = None,
    tipo_roteirizacao: str = "carteira",
    caminhos_pipeline: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del rodada_id, kwargs

    suffix = "m5_2"

    saldo = normalize_saldo_m5(
        df_input=df_saldo_elegivel_composicao_m5_1,
        etapa="M5.2",
        require_geo=True,
        require_subregiao=False,
        require_mesorregiao=False,
    )

    perfis_elegiveis = (
        df_perfis_elegiveis_por_cidade_m5_1.copy()
        if df_perfis_elegiveis_por_cidade_m5_1 is not None
        else pd.DataFrame()
    )

    if perfis_elegiveis.empty:
        raise ValueError("M5.2 exige df_perfis_elegiveis_por_cidade_m5_1.")

    if saldo.empty:
        outputs_vazio = {
            "df_premanifestos_m5_2": pd.DataFrame(),
            "df_itens_premanifestos_m5_2": pd.DataFrame(),
            "df_tentativas_m5_2": pd.DataFrame(),
            "df_remanescente_m5_2": pd.DataFrame(),
        }
        meta_vazio = {
            "resumo_m5_2": {
                "modulo": "M5.2",
                "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
                "tipo_roteirizacao": tipo_roteirizacao,
                "linhas_entrada_m5_2": 0,
                "pre_manifestos_gerados_m5_2": 0,
                "itens_pre_manifestados_m5_2": 0,
                "remanescente_saida_m5_2": 0,
                "cidades_processadas_m5_2": 0,
                "estrategia_m5_2": [
                    "cidade_por_cidade",
                    "solver_guiado_com_poda",
                    "filtro_previo_blocos_compativeis_por_perfil",
                    "maximiza_ocupacao_e_aproveitamento",
                    "multiplos_fechamentos_na_mesma_cidade",
                    "VERSAO_M5_2_2026_04_15_FIX_RESTRICAO",
                ],
                "caminhos_pipeline": caminhos_pipeline or {},
            },
            "auditoria_m5_2": {
                "total_tentativas": 0,
                "total_pre_manifestos": 0,
                "total_itens_pre_manifestados": 0,
                "total_remanescentes": 0,
                "total_cidades_processadas": 0,
            },
        }
        return outputs_vazio, meta_vazio

    saldo = precalcular_ordenacao_m5(saldo, suffix=suffix)
    saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_manifestados_list: List[pd.DataFrame] = []
    tentativas: List[Dict[str, Any]] = []

    manifesto_seq = 1
    cidades_processadas = 0

    cidades_keys = _ordenar_cidades_por_massa(saldo)

    for cidade_key, uf_key in cidades_keys:
        cidades_processadas += 1

        while True:
            city_df = saldo[
                (saldo[f"_cidade_key_{suffix}"] == cidade_key)
                & (saldo[f"_uf_key_{suffix}"] == uf_key)
            ].copy()

            if city_df.empty:
                break

            candidato, vehicle_row, motivo = _buscar_melhor_fechamento_na_cidade(
                city_df=city_df,
                perfis_elegiveis_df=perfis_elegiveis,
                cidade=cidade_key,
                uf=uf_key,
                tentativas=tentativas,
                suffix=suffix,
            )

            if candidato is None or vehicle_row is None:
                tentativas.append(
                    {
                        "cidade": cidade_key,
                        "uf": uf_key,
                        "tentativa_idx": None,
                        "blocos_considerados": 0,
                        "veiculo_tipo_tentado": None,
                        "veiculo_perfil_tentado": None,
                        "resultado": "saldo",
                        "motivo": motivo,
                        "qtd_itens_candidato": int(len(city_df)),
                        "qtd_paradas_candidato": qtd_paradas(city_df),
                        "peso_total_candidato": round(peso_total(city_df), 3),
                        "peso_kg_total_candidato": round(peso_auditoria_total(city_df), 3),
                        "volume_total_candidato": round(volume_total(city_df), 3),
                        "km_referencia_candidato": round(km_referencia(city_df), 2),
                        "ocupacao_perc_candidato": 0.0,
                    }
                )
                break

            manifesto_id = _build_manifesto_id(manifesto_seq)
            manifesto_seq += 1

            df_manifesto, df_itens = _build_manifesto(
                df_itens=candidato,
                vehicle_row=vehicle_row,
                manifesto_id=manifesto_id,
                cidade=cidade_key,
                uf=uf_key,
                suffix=suffix,
            )

            manifestos_list.append(df_manifesto)
            itens_manifestados_list.append(df_itens)

            ids_consumidos = set(candidato[f"_id_str_{suffix}"].tolist())
            saldo = saldo[~saldo[f"_id_str_{suffix}"].isin(ids_consumidos)].copy()

            if saldo.empty:
                break

            saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    df_premanifestos_m5_2 = (
        pd.concat(manifestos_list, ignore_index=True)
        if manifestos_list
        else pd.DataFrame()
    )

    df_itens_premanifestos_m5_2 = (
        pd.concat(itens_manifestados_list, ignore_index=True)
        if itens_manifestados_list
        else pd.DataFrame()
    )

    df_tentativas_m5_2 = pd.DataFrame(tentativas)
    df_remanescente_m5_2 = _drop_internal_cols(saldo.reset_index(drop=True), suffix=suffix)

    resumo_m5_2 = {
        "modulo": "M5.2",
        "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
        "tipo_roteirizacao": tipo_roteirizacao,
        "linhas_entrada_m5_2": int(len(df_saldo_elegivel_composicao_m5_1)),
        "pre_manifestos_gerados_m5_2": int(df_premanifestos_m5_2["manifesto_id"].nunique()) if not df_premanifestos_m5_2.empty else 0,
        "itens_pre_manifestados_m5_2": int(len(df_itens_premanifestos_m5_2)),
        "remanescente_saida_m5_2": int(len(df_remanescente_m5_2)),
        "cidades_processadas_m5_2": int(cidades_processadas),
        "estrategia_m5_2": [
            "cidade_por_cidade",
            "solver_guiado_com_poda",
            "filtro_previo_blocos_compativeis_por_perfil",
            "maximiza_ocupacao_e_aproveitamento",
            "multiplos_fechamentos_na_mesma_cidade",
            "VERSAO_M5_2_2026_04_15_FIX_RESTRICAO",
        ],
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    auditoria_m5_2 = {
        "total_tentativas": int(len(df_tentativas_m5_2)),
        "total_pre_manifestos": int(df_premanifestos_m5_2["manifesto_id"].nunique()) if not df_premanifestos_m5_2.empty else 0,
        "total_itens_pre_manifestados": int(len(df_itens_premanifestos_m5_2)),
        "total_remanescentes": int(len(df_remanescente_m5_2)),
        "total_cidades_processadas": int(cidades_processadas),
    }

    outputs_m5_2 = {
        "df_premanifestos_m5_2": df_premanifestos_m5_2,
        "df_itens_premanifestos_m5_2": df_itens_premanifestos_m5_2,
        "df_tentativas_m5_2": df_tentativas_m5_2,
        "df_remanescente_m5_2": df_remanescente_m5_2,
    }

    meta_m5_2 = {
        "resumo_m5_2": resumo_m5_2,
        "auditoria_m5_2": auditoria_m5_2,
    }

    return outputs_m5_2, meta_m5_2


# Aliases defensivos
def executar_m5_composicao_cidades(*args: Any, **kwargs: Any):
    return executar_m5_2_composicao_cidades(*args, **kwargs)


def processar_m5_2_composicao_cidades(*args: Any, **kwargs: Any):
    return executar_m5_2_composicao_cidades(*args, **kwargs)


def rodar_m5_2_composicao_cidades(*args: Any, **kwargs: Any):
    return executar_m5_2_composicao_cidades(*args, **kwargs)
