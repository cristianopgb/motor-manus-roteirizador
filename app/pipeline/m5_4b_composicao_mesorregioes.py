from __future__ import annotations

import math
from itertools import combinations
from typing import Any, Dict, List, Optional, Tuple
import time

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
    ocupacao_perc,
    grupo_respeita_restricao_veiculo,
    buscar_fechamento_territorial_oversized_m5,
    buscar_fechamento_com_agenda_obrigatoria_m5,
    TOLERANCIA_CORREDOR_MESORREGIAO,
)


MAX_CLIENTES_BASE = 10
MAX_PREFIXOS_POR_PERFIL = 8
MAX_TROCAS_1 = 20
MAX_TROCAS_2 = 30

COLS_PREMANIFESTOS_M5_4 = [
    "manifesto_id", "tipo_manifesto", "mesorregiao", "veiculo_tipo", "veiculo_perfil", "qtd_itens", "qtd_ctes",
    "qtd_paradas", "qtd_subregioes", "qtd_cidades", "base_carga_oficial", "peso_total_kg", "vol_total_m3",
    "km_referencia", "ocupacao_oficial_perc", "capacidade_peso_kg_veiculo", "capacidade_vol_m3_veiculo",
    "max_entregas_veiculo", "max_km_distancia_veiculo", "ocupacao_minima_perc_veiculo",
    "ocupacao_maxima_perc_veiculo", "ignorar_ocupacao_minima", "origem_modulo", "origem_etapa",
    "km_total_estimado_m5_4", "corredor_ancora_m5_4", "diff_corredor_max_m5_4",
]

COLS_TENTATIVAS_M5_4 = [
    "mesorregiao", "tentativa_idx", "blocos_considerados", "veiculo_tipo_tentado", "veiculo_perfil_tentado",
    "resultado", "motivo", "qtd_itens_candidato", "qtd_paradas_candidato", "peso_total_candidato",
    "peso_kg_total_candidato", "volume_total_candidato", "km_referencia_candidato", "km_total_estimado_candidato",
    "ocupacao_perc_candidato", "corredores_considerados", "corredor_ancora", "diff_corredor_max",
]

FATOR_RODOVIARIO_M5_4 = 1.20


def _empty_like(colunas: List[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=colunas)


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


def _ordenar_mesorregioes_por_massa(df_saldo: pd.DataFrame) -> List[str]:
    if df_saldo.empty:
        return []

    agrupado = (
        df_saldo.groupby(["mesorregiao"], dropna=False, sort=False)
        .agg(
            peso_total_mesorregiao=("peso_calculado", "sum"),
            qtd_linhas_mesorregiao=("id_linha_pipeline", "count"),
        )
        .reset_index()
        .sort_values(
            by=["peso_total_mesorregiao", "mesorregiao"],
            ascending=[False, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )

    return [safe_text(v) for v in agrupado["mesorregiao"].tolist()]


def _agrupar_blocos_cliente_na_mesorregiao(pool_df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if pool_df.empty:
        return pd.DataFrame()

    temp = pool_df.copy()

    cliente_key_col = f"_cliente_key_{suffix}"
    bucket_col = f"_bucket_{suffix}"
    ranking_col = f"_ranking_ord_{suffix}"

    grouped = (
        temp.groupby([cliente_key_col, "destinatario"], dropna=False)
        .agg(
            mesorregiao=("mesorregiao", "first"),
            subregiao=("subregiao", "first"),
            cidade=("cidade", "first"),
            peso_total_bloco=("peso_calculado", "sum"),
            peso_kg_total_bloco=("peso_kg", "sum"),
            volume_total_bloco=("vol_m3", "sum"),
            km_referencia_bloco=("distancia_rodoviaria_est_km", "max"),
            qtd_linhas_bloco=("id_linha_pipeline", "count"),
            corredor_dominante_bloco=("corredor_30g_idx", lambda s: pd.to_numeric(s, errors="coerce").dropna().astype(int).mode().iloc[0] if not pd.to_numeric(s, errors="coerce").dropna().empty else pd.NA),
            corredor_min_idx=("corredor_30g_idx", lambda s: pd.to_numeric(s, errors="coerce").dropna().min()),
            corredor_max_idx=("corredor_30g_idx", lambda s: pd.to_numeric(s, errors="coerce").dropna().max()),
            eixo_dominante_bloco=("eixo_8_setores", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().mode().iloc[0] if not s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().empty else ""),
            qtd_corredores_bloco=("corredor_30g_idx", lambda s: pd.to_numeric(s, errors="coerce").dropna().astype(int).nunique()),
            qtd_subregioes_bloco=("subregiao", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
            qtd_cidades_bloco=("cidade", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
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
    grouped["corredor_dominante_bloco"] = grouped["corredor_dominante_bloco"].apply(
        lambda v: f"C{int(v):02d}" if pd.notna(v) and int(v) > 0 else ""
    )
    return grouped


def _materializar_candidato_por_blocos(
    pool_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    suffix: str,
) -> pd.DataFrame:
    if pool_df.empty or blocks_df.empty:
        return pd.DataFrame(columns=pool_df.columns)

    cliente_key_col = f"_cliente_key_{suffix}"
    keys = set(blocks_df[cliente_key_col].tolist())

    candidato = pool_df[pool_df[cliente_key_col].isin(keys)].copy()
    candidato = ordenar_operacional_m5(candidato, suffix=suffix)
    return candidato.reset_index(drop=True)


def _qtd_paradas_validas(df_itens: pd.DataFrame) -> int:
    if df_itens is None or df_itens.empty or "destinatario" not in df_itens.columns:
        return 0

    serie = df_itens["destinatario"].fillna("").astype(str).str.strip()
    serie = serie[serie != ""]
    return int(serie.nunique())


def _km_referencia_manifesto(df_itens: pd.DataFrame) -> float:
    if df_itens is None or df_itens.empty or "distancia_rodoviaria_est_km" not in df_itens.columns:
        return 0.0

    return float(pd.to_numeric(df_itens["distancia_rodoviaria_est_km"], errors="coerce").fillna(0).max())


def _diff_corredor_circular(idx_a: Optional[int], idx_b: Optional[int]) -> Optional[int]:
    if idx_a is None or idx_b is None:
        return None
    diff_linear = abs(idx_a - idx_b)
    return int(min(diff_linear, 12 - diff_linear))


def _metricas_corredor(df_itens: pd.DataFrame) -> Tuple[Optional[int], List[str], Optional[int]]:
    if df_itens is None or df_itens.empty or "corredor_30g_idx" not in df_itens.columns:
        return None, [], None

    serie_idx = pd.to_numeric(df_itens["corredor_30g_idx"], errors="coerce").dropna().astype(int)
    serie_idx = serie_idx[(serie_idx >= 1) & (serie_idx <= 12)]
    if serie_idx.empty:
        return None, [], None

    contagem = serie_idx.value_counts()
    corredor_ancora = int(contagem.index[0])
    diffs = [
        _diff_corredor_circular(corredor_ancora, int(v))
        for v in serie_idx.unique().tolist()
    ]
    diffs_validos = [d for d in diffs if d is not None]
    diff_max = int(max(diffs_validos)) if diffs_validos else None
    corredores = [f"C{int(v):02d}" for v in sorted(serie_idx.unique().tolist())]
    return corredor_ancora, corredores, diff_max


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    raio_terra_km = 6371.0088
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return float(raio_terra_km * c)


def _km_total_estimado_candidato(df_itens: pd.DataFrame) -> Tuple[float, str]:
    if df_itens is None or df_itens.empty:
        return 0.0, "fallback_km_referencia_sem_itens"

    colunas_coord = {"origem_latitude", "origem_longitude", "latitude_destinatario", "longitude_destinatario"}
    if not colunas_coord.issubset(set(df_itens.columns)):
        return round(_km_referencia_manifesto(df_itens), 2), "fallback_km_referencia_sem_coordenadas"

    temp = df_itens.copy()
    for c in colunas_coord:
        temp[c] = pd.to_numeric(temp[c], errors="coerce")

    temp = temp.dropna(subset=["latitude_destinatario", "longitude_destinatario"]).copy()
    if temp.empty:
        return round(_km_referencia_manifesto(df_itens), 2), "fallback_km_referencia_sem_destino_valido"

    agrupado = (
        temp.groupby(["cidade", "uf"], dropna=False, sort=False)
        .agg(
            lat_dest=("latitude_destinatario", "mean"),
            lon_dest=("longitude_destinatario", "mean"),
            km_ref=("distancia_rodoviaria_est_km", "max"),
        )
        .reset_index()
        .sort_values(by=["km_ref"], ascending=[True], kind="mergesort")
        .reset_index(drop=True)
    )
    if agrupado.empty:
        return round(_km_referencia_manifesto(df_itens), 2), "fallback_km_referencia_sem_blocos"

    origem_lat = pd.to_numeric(df_itens["origem_latitude"], errors="coerce").dropna()
    origem_lon = pd.to_numeric(df_itens["origem_longitude"], errors="coerce").dropna()
    if origem_lat.empty or origem_lon.empty:
        return round(_km_referencia_manifesto(df_itens), 2), "fallback_km_referencia_sem_origem_valida"

    atual_lat = float(origem_lat.iloc[0])
    atual_lon = float(origem_lon.iloc[0])
    km_total = 0.0

    for _, row in agrupado.iterrows():
        prox_lat = float(row["lat_dest"])
        prox_lon = float(row["lon_dest"])
        km_total += _haversine_km(atual_lat, atual_lon, prox_lat, prox_lon) * FATOR_RODOVIARIO_M5_4
        atual_lat = prox_lat
        atual_lon = prox_lon

    return round(km_total, 2), "estimado_haversine_1_20"


def _remover_clientes_fora_do_raio(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    suffix: str,
) -> Tuple[pd.DataFrame, int]:
    if df_itens.empty:
        return df_itens.copy(), 0

    max_km = safe_float(vehicle_row.get("max_km_distancia"), 0.0)
    if max_km <= 0 or "distancia_rodoviaria_est_km" not in df_itens.columns:
        return df_itens.copy(), 0

    cliente_key_col = f"_cliente_key_{suffix}"
    if cliente_key_col not in df_itens.columns:
        return df_itens.copy(), 0

    temp = df_itens.copy()
    temp["_dist_tmp_raio"] = pd.to_numeric(temp["distancia_rodoviaria_est_km"], errors="coerce").fillna(0)

    chaves_fora = set(
        temp.loc[temp["_dist_tmp_raio"] > max_km, cliente_key_col].astype(str).tolist()
    )
    if not chaves_fora:
        return df_itens.copy(), 0

    reduzido = temp.loc[~temp[cliente_key_col].astype(str).isin(chaves_fora)].copy()
    reduzido = reduzido.drop(columns=["_dist_tmp_raio"], errors="ignore")
    removidos = len(chaves_fora)

    if not reduzido.empty:
        reduzido = ordenar_operacional_m5(reduzido, suffix=suffix)

    return reduzido.reset_index(drop=True), removidos


def _validar_hard_constraints(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    suffix: str,
) -> Tuple[bool, str, pd.DataFrame]:
    if df_itens.empty:
        return False, "grupo_vazio", df_itens.copy()

    candidato = df_itens.copy()

    if not grupo_respeita_restricao_veiculo(candidato, vehicle_row):
        return False, "restricao_veiculo_incompativel", candidato

    candidato, qtd_removidos_raio = _remover_clientes_fora_do_raio(
        df_itens=candidato,
        vehicle_row=vehicle_row,
        suffix=suffix,
    )

    if candidato.empty:
        return False, "todos_clientes_fora_do_raio", candidato

    if not grupo_respeita_restricao_veiculo(candidato, vehicle_row):
        return False, "restricao_veiculo_incompativel", candidato

    peso_oficial = peso_total(candidato)
    volume = volume_total(candidato)
    paradas = _qtd_paradas_validas(candidato)
    km_ref = _km_referencia_manifesto(candidato)

    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    cap_vol = safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0)
    max_entregas = safe_int(vehicle_row.get("max_entregas"), 0)
    max_km = safe_float(vehicle_row.get("max_km_distancia"), 0.0)
    ocup_max = safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0)

    if cap_peso > 0 and peso_oficial > cap_peso:
        return False, "excede_capacidade_peso", candidato
    if cap_vol > 0 and volume > cap_vol:
        return False, "excede_capacidade_volume", candidato
    if max_entregas > 0 and paradas > max_entregas:
        return False, "excede_max_entregas", candidato
    if max_km > 0 and km_ref > max_km:
        return False, "excede_max_km_referencia", candidato

    ocup = ocupacao_perc(candidato, vehicle_row)
    if ocup > ocup_max:
        return False, "excede_ocupacao_maxima", candidato

    if qtd_removidos_raio > 0:
        return True, "ok_com_poda_raio", candidato

    return True, "ok", candidato


def _validar_fechamento(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    suffix: str,
    tolerancia_corredor: int = 2,
) -> Tuple[bool, str, pd.DataFrame, Dict[str, Any]]:
    ok_hard, motivo_hard, candidato_ajustado = _validar_hard_constraints(
        df_itens=df_itens,
        vehicle_row=vehicle_row,
        suffix=suffix,
    )
    if not ok_hard:
        km_estimado, fonte_km_estimado = _km_total_estimado_candidato(candidato_ajustado)
        corredor_ancora, corredores_considerados, diff_corredor_max = _metricas_corredor(candidato_ajustado)
        return False, motivo_hard, candidato_ajustado, {
            "km_total_estimado_candidato": km_estimado,
            "fonte_km_total_estimado": fonte_km_estimado,
            "corredor_ancora": corredor_ancora,
            "corredores_considerados": corredores_considerados,
            "diff_corredor_max": diff_corredor_max,
        }

    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0)
    ocup = ocupacao_perc(candidato_ajustado, vehicle_row)

    if ocup < ocup_min:
        km_estimado, fonte_km_estimado = _km_total_estimado_candidato(candidato_ajustado)
        corredor_ancora, corredores_considerados, diff_corredor_max = _metricas_corredor(candidato_ajustado)
        return False, "abaixo_ocupacao_minima", candidato_ajustado, {
            "km_total_estimado_candidato": km_estimado,
            "fonte_km_total_estimado": fonte_km_estimado,
            "corredor_ancora": corredor_ancora,
            "corredores_considerados": corredores_considerados,
            "diff_corredor_max": diff_corredor_max,
        }

    corredor_ancora, corredores_considerados, diff_corredor_max = _metricas_corredor(candidato_ajustado)
    if diff_corredor_max is not None and diff_corredor_max > int(tolerancia_corredor):
        km_estimado, fonte_km_estimado = _km_total_estimado_candidato(candidato_ajustado)
        return False, "corredor_distante", candidato_ajustado, {
            "km_total_estimado_candidato": km_estimado,
            "fonte_km_total_estimado": fonte_km_estimado,
            "corredor_ancora": corredor_ancora,
            "corredores_considerados": corredores_considerados,
            "diff_corredor_max": diff_corredor_max,
        }

    km_estimado, fonte_km_estimado = _km_total_estimado_candidato(candidato_ajustado)
    max_km = safe_float(vehicle_row.get("max_km_distancia"), 0.0)
    if max_km > 0 and km_estimado > max_km:
        return False, "excede_max_km_estimado", candidato_ajustado, {
            "km_total_estimado_candidato": km_estimado,
            "fonte_km_total_estimado": fonte_km_estimado,
            "corredor_ancora": corredor_ancora,
            "corredores_considerados": corredores_considerados,
            "diff_corredor_max": diff_corredor_max,
        }

    return True, "ok", candidato_ajustado, {
        "km_total_estimado_candidato": km_estimado,
        "fonte_km_total_estimado": fonte_km_estimado,
        "corredor_ancora": corredor_ancora,
        "corredores_considerados": corredores_considerados,
        "diff_corredor_max": diff_corredor_max,
    }


def _score_candidato(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> Tuple[float, float, int, float]:
    ocup = ocupacao_perc(df_itens, vehicle_row)
    peso = peso_total(df_itens)
    clientes = _qtd_paradas_validas(df_itens)
    cap = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)

    return (
        round(ocup, 6),
        round(peso, 6),
        int(clientes),
        -cap,
    )


def _tentativa_dict(
    mesorregiao: str,
    vehicle_row: Optional[pd.Series],
    resultado: str,
    motivo: str,
    df_candidato: Optional[pd.DataFrame],
    tentativa_idx: int,
    blocos_considerados: int,
    auditoria_extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    candidato = df_candidato if df_candidato is not None else pd.DataFrame()
    auditoria_extra = auditoria_extra or {}
    corredores = auditoria_extra.get("corredores_considerados") or []

    return {
        "mesorregiao": mesorregiao,
        "tentativa_idx": tentativa_idx,
        "blocos_considerados": blocos_considerados,
        "veiculo_tipo_tentado": None if vehicle_row is None else safe_text(vehicle_row.get("tipo")),
        "veiculo_perfil_tentado": None if vehicle_row is None else safe_text(vehicle_row.get("perfil")),
        "resultado": resultado,
        "motivo": motivo,
        "qtd_itens_candidato": int(len(candidato)),
        "qtd_paradas_candidato": _qtd_paradas_validas(candidato),
        "peso_total_candidato": round(peso_total(candidato), 3),
        "peso_kg_total_candidato": round(peso_auditoria_total(candidato), 3),
        "volume_total_candidato": round(volume_total(candidato), 3),
        "km_referencia_candidato": round(_km_referencia_manifesto(candidato), 2),
        "km_total_estimado_candidato": round(safe_float(auditoria_extra.get("km_total_estimado_candidato"), 0.0), 2),
        "ocupacao_perc_candidato": round(ocupacao_perc(candidato, vehicle_row), 2)
        if vehicle_row is not None and not candidato.empty
        else 0.0,
        "corredores_considerados": "|".join([safe_text(v) for v in corredores]) if corredores else "",
        "corredor_ancora": (
            f"C{safe_int(auditoria_extra.get('corredor_ancora'), 0):02d}"
            if safe_int(auditoria_extra.get("corredor_ancora"), 0) > 0
            else ""
        ),
        "diff_corredor_max": safe_int(auditoria_extra.get("diff_corredor_max"), 0),
    }


def _build_manifesto_id(seq: int) -> str:
    return f"PM54_{seq:04d}"


def _build_manifesto(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    manifesto_id: str,
    mesorregiao: str,
    suffix: str,
    auditoria_extra: Optional[Dict[str, Any]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    auditoria_extra = auditoria_extra or {}
    df_itens_limpo = _drop_internal_cols(df_itens, suffix=suffix)

    qtd_itens = int(len(df_itens_limpo))
    qtd_ctes = int(df_itens_limpo["cte"].nunique(dropna=True)) if "cte" in df_itens_limpo.columns else qtd_itens
    qtd_subregioes = int(
        df_itens_limpo["subregiao"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    ) if "subregiao" in df_itens_limpo.columns else 0
    qtd_cidades = int(
        df_itens_limpo["cidade"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    ) if "cidade" in df_itens_limpo.columns else 0

    manifesto = {
        "manifesto_id": manifesto_id,
        "tipo_manifesto": "pre_manifesto_bloco_5_4_mesorregiao",
        "mesorregiao": mesorregiao,
        "veiculo_tipo": safe_text(vehicle_row.get("tipo")),
        "veiculo_perfil": safe_text(vehicle_row.get("perfil")),
        "qtd_itens": qtd_itens,
        "qtd_ctes": qtd_ctes,
        "qtd_paradas": _qtd_paradas_validas(df_itens_limpo),
        "qtd_subregioes": qtd_subregioes,
        "qtd_cidades": qtd_cidades,
        "base_carga_oficial": round(peso_total(df_itens_limpo), 3),
        "peso_total_kg": round(peso_auditoria_total(df_itens_limpo), 3),
        "vol_total_m3": round(volume_total(df_itens_limpo), 3),
        "km_referencia": round(_km_referencia_manifesto(df_itens_limpo), 2),
        "ocupacao_oficial_perc": round(ocupacao_perc(df_itens_limpo, vehicle_row), 2),
        "capacidade_peso_kg_veiculo": safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0),
        "capacidade_vol_m3_veiculo": safe_float(vehicle_row.get("capacidade_vol_m3"), 0.0),
        "max_entregas_veiculo": safe_int(vehicle_row.get("max_entregas"), 0),
        "max_km_distancia_veiculo": safe_float(vehicle_row.get("max_km_distancia"), 0.0),
        "ocupacao_minima_perc_veiculo": safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0),
        "ocupacao_maxima_perc_veiculo": safe_float(vehicle_row.get("ocupacao_maxima_perc"), 100.0),
        "ignorar_ocupacao_minima": False,
        "origem_modulo": 5,
        "origem_etapa": "m5_4b_composicao_mesorregiao",
        "km_total_estimado_m5_4": round(safe_float(auditoria_extra.get("km_total_estimado_candidato"), 0.0), 2),
        "corredor_ancora_m5_4": (
            f"C{safe_int(auditoria_extra.get('corredor_ancora'), 0):02d}"
            if safe_int(auditoria_extra.get("corredor_ancora"), 0) > 0
            else ""
        ),
        "diff_corredor_max_m5_4": safe_int(auditoria_extra.get("diff_corredor_max"), 0),
    }

    df_manifesto = pd.DataFrame([manifesto])

    df_itens_saida = df_itens_limpo.copy()
    for k, v in manifesto.items():
        df_itens_saida[k] = v

    return df_manifesto, df_itens_saida


def _get_eligible_vehicles_for_mesorregiao(
    mesorregiao: str,
    perfis_elegiveis_df: pd.DataFrame,
) -> pd.DataFrame:
    base = perfis_elegiveis_df[
        perfis_elegiveis_df["mesorregiao"].fillna("").astype(str).str.strip() == mesorregiao
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


def _bloco_compativel_com_veiculo(
    bloco_df: pd.DataFrame,
    vehicle_row: pd.Series,
) -> bool:
    if bloco_df is None or bloco_df.empty:
        return False
    return bool(grupo_respeita_restricao_veiculo(bloco_df, vehicle_row))


def _filtrar_blocos_compativeis_por_perfil(
    pool_df: pd.DataFrame,
    blocks_df: pd.DataFrame,
    vehicle_row: pd.Series,
    suffix: str,
) -> pd.DataFrame:
    if pool_df.empty or blocks_df.empty:
        return pd.DataFrame(columns=blocks_df.columns)

    cliente_key_col = f"_cliente_key_{suffix}"
    if cliente_key_col not in pool_df.columns or cliente_key_col not in blocks_df.columns:
        return pd.DataFrame(columns=blocks_df.columns)

    blocos_validos: List[pd.Series] = []

    for _, bloco_row in blocks_df.iterrows():
        chave = bloco_row[cliente_key_col]
        bloco_df = pool_df[pool_df[cliente_key_col] == chave].copy()
        if _bloco_compativel_com_veiculo(bloco_df, vehicle_row):
            blocos_validos.append(bloco_row)

    if not blocos_validos:
        return pd.DataFrame(columns=blocks_df.columns)

    filtrado = pd.DataFrame(blocos_validos).reset_index(drop=True)
    return filtrado


def _selecionar_blocos_base_para_busca(blocks_df: pd.DataFrame) -> pd.DataFrame:
    if blocks_df.empty:
        return blocks_df.copy()

    return blocks_df.head(min(len(blocks_df), MAX_CLIENTES_BASE)).copy().reset_index(drop=True)


def _selecionar_blocos_base_para_busca_com_prioridade_agenda(
    blocks_df: pd.DataFrame,
    flag_col: str = "flag_agendada_roteirizavel",
) -> pd.DataFrame:
    if blocks_df.empty:
        return blocks_df.copy()
    limite = min(len(blocks_df), MAX_CLIENTES_BASE)
    if flag_col not in blocks_df.columns or len(blocks_df) <= 1:
        return blocks_df.head(limite).copy().reset_index(drop=True)
    primeiro = blocks_df.head(1).copy()
    restante = blocks_df.iloc[1:].copy()
    restante["_prioridade_agenda_tmp"] = ~restante[flag_col].fillna(False).astype(bool)
    restante = restante.sort_values(by=["_prioridade_agenda_tmp"], ascending=[True], kind="mergesort").drop(columns=["_prioridade_agenda_tmp"], errors="ignore")
    base = pd.concat([primeiro, restante], ignore_index=True)
    return base.head(limite).copy().reset_index(drop=True)


def _possui_agendada_roteirizavel(df_itens: pd.DataFrame) -> bool:
    return bool(
        isinstance(df_itens, pd.DataFrame)
        and "flag_agendada_roteirizavel" in df_itens.columns
        and df_itens["flag_agendada_roteirizavel"].fillna(False).astype(bool).any()
    )


def _gerar_candidatos_guiados(
    blocks_df: pd.DataFrame,
    vehicle_row: pd.Series,
    cliente_key_col: str,
) -> List[pd.DataFrame]:
    if blocks_df.empty:
        return []

    candidatos: List[pd.DataFrame] = []
    vistos: set[Tuple[str, ...]] = set()

    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0)
    min_kg = cap_peso * (ocup_min / 100.0) if cap_peso > 0 else 0.0

    base = _selecionar_blocos_base_para_busca_com_prioridade_agenda(blocks_df)
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


def _buscar_melhor_fechamento_na_mesorregiao(
    pool_df: pd.DataFrame,
    perfis_elegiveis_df: pd.DataFrame,
    mesorregiao: str,
    tentativas: List[Dict[str, Any]],
    suffix: str,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], str, int, int]:
    if pool_df.empty:
        return None, None, "mesorregiao_vazia", 0, 0

    vehicles_meso = _get_eligible_vehicles_for_mesorregiao(
        mesorregiao=mesorregiao,
        perfis_elegiveis_df=perfis_elegiveis_df,
    )
    if vehicles_meso.empty:
        tentativas.append(
            _tentativa_dict(
                mesorregiao=mesorregiao,
                vehicle_row=None,
                resultado="falhou",
                motivo="sem_perfil_elegivel_na_mesorregiao",
                df_candidato=pool_df,
                tentativa_idx=1,
                blocos_considerados=0,
            )
        )
        return None, None, "sem_perfil_elegivel_na_mesorregiao", 1, 0

    blocks_df = _agrupar_blocos_cliente_na_mesorregiao(pool_df, suffix=suffix)
    if blocks_df.empty:
        return None, None, "sem_blocos_na_mesorregiao", 0, 0

    cliente_key_col = f"_cliente_key_{suffix}"

    melhor_df: Optional[pd.DataFrame] = None
    melhor_vehicle: Optional[pd.Series] = None
    melhor_score: Optional[Tuple[float, float, int, float]] = None
    melhor_auditoria: Dict[str, Any] = {}
    melhor_motivo = "nenhum_fechamento"
    chamadas_prioritarias = 0
    fechamentos_com_agenda = 0

    tentativa_idx = 1

    for _, vehicle_row in vehicles_meso.iterrows():
        blocks_df_compativeis = _filtrar_blocos_compativeis_por_perfil(
            pool_df=pool_df,
            blocks_df=blocks_df,
            vehicle_row=vehicle_row,
            suffix=suffix,
        )

        if blocks_df_compativeis.empty:
            tentativas.append(
                _tentativa_dict(
                    mesorregiao=mesorregiao,
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
                    mesorregiao=mesorregiao,
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
            candidato_bruto = _materializar_candidato_por_blocos(pool_df, blocks_candidato, suffix=suffix)
            if _possui_agendada_roteirizavel(candidato_bruto):
                chamadas_prioritarias += 1
            ok, motivo, candidato, auditoria_extra = _validar_fechamento(
                df_itens=candidato_bruto,
                vehicle_row=vehicle_row,
                suffix=suffix,
            )

            tentativas.append(
                _tentativa_dict(
                    mesorregiao=mesorregiao,
                    vehicle_row=vehicle_row,
                    resultado="fechado" if ok else "falhou",
                    motivo=motivo,
                    df_candidato=candidato,
                    tentativa_idx=tentativa_idx,
                    blocos_considerados=int(len(blocks_candidato)),
                    auditoria_extra=auditoria_extra,
                )
            )
            tentativa_idx += 1
            melhor_motivo = motivo

            if not ok or candidato.empty:
                continue
            if _possui_agendada_roteirizavel(candidato):
                fechamentos_com_agenda += 1
                candidato.attrs["auditoria_m5_4"] = dict(auditoria_extra)
                return candidato.copy(), vehicle_row.copy(), "ok", chamadas_prioritarias, fechamentos_com_agenda

            score = _score_candidato(candidato, vehicle_row)

            if melhor_score is None or score > melhor_score:
                melhor_score = score
                melhor_df = candidato.copy()
                melhor_vehicle = vehicle_row.copy()
                melhor_auditoria = dict(auditoria_extra)

    if melhor_df is None or melhor_vehicle is None:
        return None, None, melhor_motivo, chamadas_prioritarias, fechamentos_com_agenda

    if melhor_df is not None:
        melhor_df.attrs["auditoria_m5_4"] = melhor_auditoria
    return melhor_df, melhor_vehicle, "ok", chamadas_prioritarias, fechamentos_com_agenda


def executar_m5_4b_composicao_mesorregioes(
    df_saldo_elegivel_composicao_m5_4: pd.DataFrame,
    df_perfis_elegiveis_por_mesorregiao_m5_4: pd.DataFrame,
    rodada_id: Optional[str] = None,
    data_base_roteirizacao: Optional[Any] = None,
    tipo_roteirizacao: str = "carteira",
    caminhos_pipeline: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del rodada_id, kwargs

    suffix = "m5_4b"

    saldo = normalize_saldo_m5(
        df_input=df_saldo_elegivel_composicao_m5_4,
        etapa="M5.4B",
        require_geo=True,
        require_subregiao=True,
        require_mesorregiao=True,
    )

    perfis_elegiveis = (
        df_perfis_elegiveis_por_mesorregiao_m5_4.copy()
        if df_perfis_elegiveis_por_mesorregiao_m5_4 is not None
        else pd.DataFrame()
    )

    if saldo.empty or perfis_elegiveis.empty:
        motivo_pulo = "sem_saldo_elegivel_m5_4b" if saldo.empty else "sem_perfis_elegiveis_m5_4b"
        df_remanescente = (
            _drop_internal_cols(saldo.reset_index(drop=True), suffix=suffix)
            if isinstance(saldo, pd.DataFrame)
            else _empty_like([])
        )
        outputs_vazio = {
            "df_premanifestos_m5_4": _empty_like(COLS_PREMANIFESTOS_M5_4),
            "df_itens_premanifestos_m5_4": _empty_like(list(df_remanescente.columns) + COLS_PREMANIFESTOS_M5_4),
            "df_tentativas_m5_4": _empty_like(COLS_TENTATIVAS_M5_4),
            "df_remanescente_m5_4": df_remanescente,
            "df_pool_mesorregiao_m5_4": _empty_like([]),
            "df_blocos_cliente_mesorregiao_m5_4": _empty_like([]),
            "df_manifestos_m5_4": _empty_like(COLS_PREMANIFESTOS_M5_4),
            "df_itens_manifestos_m5_4": _empty_like([]),
        }
        meta_vazio = {
            "resumo_m5_4b": {
                "modulo": "M5.4B",
                "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
                "tipo_roteirizacao": tipo_roteirizacao,
                "etapa_pulada": True,
                "motivo_etapa_pulada": motivo_pulo,
                "linhas_entrada_m5_4": 0,
                "pre_manifestos_gerados_m5_4": 0,
                "itens_pre_manifestados_m5_4": 0,
                "remanescente_saida_m5_4": int(len(df_remanescente)),
                "mesorregioes_processadas_m5_4": 0,
                "linhas_saida_m5_4": 0,
                "remanescente_preservado_m5_4": int(len(df_remanescente)),
                "estrategia_m5_4": [
                    "mesorregiao_por_mesorregiao",
                    "solver_guiado_com_poda",
                    "filtro_previo_blocos_compativeis_por_perfil",
                    "poda_de_raio_por_cliente",
                    "maximiza_ocupacao_e_aproveitamento",
                    "multiplos_fechamentos_na_mesma_mesorregiao",
                    "VERSAO_M5_4B_2026_04_14_FIX_RESTRICAO",
                ],
                "caminhos_pipeline": caminhos_pipeline or {},
            },
            "auditoria_m5_4b": {
                "total_tentativas": 0,
                "total_pre_manifestos": 0,
                "total_itens_pre_manifestados": 0,
                "total_remanescentes": int(len(df_remanescente)),
                "total_mesorregioes_processadas": 0,
                "motivo_etapa_pulada": motivo_pulo,
            },
        }
        return outputs_vazio, meta_vazio

    saldo = precalcular_ordenacao_m5(saldo, suffix=suffix)
    saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_manifestados_list: List[pd.DataFrame] = []
    tentativas: List[Dict[str, Any]] = []
    pools_auditoria_list: List[pd.DataFrame] = []
    blocos_auditoria_list: List[pd.DataFrame] = []

    manifesto_seq = 1
    mesorregioes_processadas = 0
    chamadas_prioritarias_total = 0
    fechamentos_agendada_total = 0
    t0_m5_4 = time.perf_counter()
    fallback_tentado = 0
    fallback_fechado = 0
    fallback_sem_fechamento = 0
    agenda_obrigatoria_tentada = 0
    agenda_obrigatoria_fechada = 0
    agenda_obrigatoria_sem_fechamento = 0
    agenda_obrigatoria_substituiu_sem_agenda = 0

    mesorregioes_keys = _ordenar_mesorregioes_por_massa(saldo)

    for mesorregiao_key in mesorregioes_keys:
        mesorregioes_processadas += 1

        while True:
            pool_df = saldo[
                saldo["mesorregiao"].fillna("").astype(str).str.strip() == mesorregiao_key
            ].copy()

            if pool_df.empty:
                break

            cols_pool = [
                "id_linha_pipeline", "nro_documento", "destinatario", "cidade", "uf", "subregiao", "mesorregiao",
                "distancia_rodoviaria_est_km", "angulo_origem_destino_graus", "eixo_8_setores", "corredor_30g",
                "corredor_30g_idx", "peso_calculado", "peso_kg", "vol_m3", "restricao_veiculo", "veiculo_exclusivo_flag",
            ]
            existentes_pool = [c for c in cols_pool if c in pool_df.columns]
            if existentes_pool:
                pools_auditoria_list.append(pool_df[existentes_pool].copy())
            blocos_pool = _agrupar_blocos_cliente_na_mesorregiao(pool_df, suffix=suffix)
            if not blocos_pool.empty:
                blocos_cols = [
                    "mesorregiao", "subregiao", "destinatario", "cidade", "qtd_linhas_bloco", "peso_total_bloco",
                    "volume_total_bloco", "km_referencia_bloco", "corredor_dominante_bloco", "corredor_min_idx",
                    "corredor_max_idx", "eixo_dominante_bloco", "qtd_corredores_bloco",
                ]
                blocos_saida = blocos_pool[[c for c in blocos_cols if c in blocos_pool.columns]].copy()
                blocos_saida = blocos_saida.rename(columns={"qtd_linhas_bloco": "qtd_itens_bloco", "volume_total_bloco": "volume_total_bloco"})
                blocos_auditoria_list.append(blocos_saida)

            candidato, vehicle_row, motivo, chamadas_prioritarias, fechamentos_agendada = _buscar_melhor_fechamento_na_mesorregiao(
                pool_df=pool_df,
                perfis_elegiveis_df=perfis_elegiveis,
                mesorregiao=mesorregiao_key,
                tentativas=tentativas,
                suffix=suffix,
            )
            chamadas_prioritarias_total += int(chamadas_prioritarias)
            fechamentos_agendada_total += int(fechamentos_agendada)
            if _possui_agendada_roteirizavel(pool_df) and (candidato is None or not _possui_agendada_roteirizavel(candidato)):
                agenda_obrigatoria_tentada += 1
                cand_ag, veh_ag, _ = buscar_fechamento_com_agenda_obrigatoria_m5(
                    df_grupo=pool_df, veiculos_elegiveis=perfis_elegiveis, suffix=suffix, escopo="mesorregiao",
                    validar_fechamento_fn=lambda df_itens, vehicle_row, suffix, tolerancia_corredor, **kwargs: _validar_fechamento(
                        df_itens=df_itens, vehicle_row=vehicle_row, suffix=suffix, tolerancia_corredor=tolerancia_corredor
                    ),
                    tolerancia_corredor=TOLERANCIA_CORREDOR_MESORREGIAO,
                )
                if cand_ag is not None and veh_ag is not None:
                    agenda_obrigatoria_fechada += 1
                    if candidato is not None and not _possui_agendada_roteirizavel(candidato):
                        agenda_obrigatoria_substituiu_sem_agenda += 1
                    candidato, vehicle_row = cand_ag, veh_ag
                else:
                    agenda_obrigatoria_sem_fechamento += 1

            if candidato is None or vehicle_row is None:
                fallback_tentado += 1
                candidato_fb, vehicle_row_fb, _ = buscar_fechamento_territorial_oversized_m5(
                    df_grupo=pool_df,
                    veiculos_elegiveis=perfis_elegiveis,
                    suffix=suffix,
                    escopo="mesorregiao",
                    validar_fechamento_fn=lambda df_itens, vehicle_row, suffix, tolerancia_corredor, **kwargs: _validar_fechamento(
                        df_itens=df_itens, vehicle_row=vehicle_row, suffix=suffix, tolerancia_corredor=tolerancia_corredor
                    ),
                    tolerancia_corredor=TOLERANCIA_CORREDOR_MESORREGIAO,
                )
                if candidato_fb is not None and vehicle_row_fb is not None:
                    fallback_fechado += 1
                    candidato, vehicle_row = candidato_fb, vehicle_row_fb
                else:
                    fallback_sem_fechamento += 1
            if candidato is None or vehicle_row is None:
                tentativas.append(
                    {
                        "mesorregiao": mesorregiao_key,
                        "tentativa_idx": None,
                        "blocos_considerados": 0,
                        "veiculo_tipo_tentado": None,
                        "veiculo_perfil_tentado": None,
                        "resultado": "saldo",
                        "motivo": motivo,
                        "qtd_itens_candidato": int(len(pool_df)),
                        "qtd_paradas_candidato": _qtd_paradas_validas(pool_df),
                        "peso_total_candidato": round(peso_total(pool_df), 3),
                        "peso_kg_total_candidato": round(peso_auditoria_total(pool_df), 3),
                        "volume_total_candidato": round(volume_total(pool_df), 3),
                        "km_referencia_candidato": round(_km_referencia_manifesto(pool_df), 2),
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
                mesorregiao=mesorregiao_key,
                suffix=suffix,
                auditoria_extra=candidato.attrs.get("auditoria_m5_4", {}),
            )

            manifestos_list.append(df_manifesto)
            itens_manifestados_list.append(df_itens)

            ids_consumidos = set(candidato[f"_id_str_{suffix}"].tolist())
            saldo = saldo[~saldo[f"_id_str_{suffix}"].isin(ids_consumidos)].copy()

            if saldo.empty:
                break

            saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    df_premanifestos_m5_4 = (
        pd.concat(manifestos_list, ignore_index=True)
        if manifestos_list
        else pd.DataFrame()
    )

    df_itens_premanifestos_m5_4 = (
        pd.concat(itens_manifestados_list, ignore_index=True)
        if itens_manifestados_list
        else pd.DataFrame()
    )

    df_tentativas_m5_4 = pd.DataFrame(tentativas)
    if "tentativa_idx" in df_tentativas_m5_4.columns:
        df_tentativas_m5_4["tentativa_idx"] = pd.to_numeric(df_tentativas_m5_4["tentativa_idx"], errors="coerce")
    df_remanescente_m5_4 = _drop_internal_cols(saldo.reset_index(drop=True), suffix=suffix)
    if not df_remanescente_m5_4.empty:
        df_remanescente_m5_4["motivo_final_remanescente_m5_4"] = "sem_fechamento_m5_4"
        if "flag_agendada_roteirizavel" in df_remanescente_m5_4.columns:
            mask_ag = df_remanescente_m5_4["flag_agendada_roteirizavel"].fillna(False).astype(bool)
            df_remanescente_m5_4.loc[mask_ag, "motivo_final_remanescente_m5_4"] = "agenda_roteirizavel_sem_fechamento_viavel"

    df_pool_mesorregiao_m5_4 = pd.concat(pools_auditoria_list, ignore_index=True, sort=False) if pools_auditoria_list else pd.DataFrame()
    df_blocos_cliente_mesorregiao_m5_4 = pd.concat(blocos_auditoria_list, ignore_index=True, sort=False) if blocos_auditoria_list else pd.DataFrame()
    df_manifestos_m5_4 = df_premanifestos_m5_4.copy()
    df_itens_manifestos_m5_4 = df_itens_premanifestos_m5_4.copy()

    resumo_m5_4b = {
        "modulo": "M5.4B",
        "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
        "tipo_roteirizacao": tipo_roteirizacao,
        "linhas_entrada_m5_4": int(len(df_saldo_elegivel_composicao_m5_4)),
        "pre_manifestos_gerados_m5_4": int(df_premanifestos_m5_4["manifesto_id"].nunique()) if not df_premanifestos_m5_4.empty else 0,
        "itens_pre_manifestados_m5_4": int(len(df_itens_premanifestos_m5_4)),
        "remanescente_saida_m5_4": int(len(df_remanescente_m5_4)),
        "mesorregioes_processadas_m5_4": int(mesorregioes_processadas),
        "estrategia_m5_4": [
            "mesorregiao_por_mesorregiao",
            "solver_guiado_com_poda",
            "filtro_previo_blocos_compativeis_por_perfil",
            "poda_de_raio_por_cliente",
            "maximiza_ocupacao_e_aproveitamento",
            "multiplos_fechamentos_na_mesma_mesorregiao",
            "VERSAO_M5_4B_2026_04_14_FIX_RESTRICAO",
        ],
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    auditoria_m5_4b = {
        "total_tentativas": int(len(df_tentativas_m5_4)),
        "total_pre_manifestos": int(df_premanifestos_m5_4["manifesto_id"].nunique()) if not df_premanifestos_m5_4.empty else 0,
        "total_itens_pre_manifestados": int(len(df_itens_premanifestos_m5_4)),
        "total_remanescentes": int(len(df_remanescente_m5_4)),
        "total_mesorregioes_processadas": int(mesorregioes_processadas),
        "agendadas_chamadas_prioritariamente_m5_4b": int(chamadas_prioritarias_total),
        "agendadas_fechadas_m5_4b": int(fechamentos_agendada_total),
        "tentativas_totais_m5_4b": int(len(df_tentativas_m5_4)),
        "tentativas_totais_m5_4b_antes_prioridade": int(len(df_tentativas_m5_4)),
        "tentativas_totais_m5_4b_depois_prioridade": int(len(df_tentativas_m5_4)),
        "tempo_execucao_m5_4b_ms": round((time.perf_counter() - t0_m5_4) * 1000, 2),
        "fallback_territorial_oversized_m5_4_tentado": int(fallback_tentado),
        "fallback_territorial_oversized_m5_4_fechado": int(fallback_fechado),
        "fallback_territorial_oversized_m5_4_sem_fechamento": int(fallback_sem_fechamento),
        "agenda_obrigatoria_m5_4_tentada": int(agenda_obrigatoria_tentada),
        "agenda_obrigatoria_m5_4_fechada": int(agenda_obrigatoria_fechada),
        "agenda_obrigatoria_m5_4_sem_fechamento": int(agenda_obrigatoria_sem_fechamento),
        "agenda_obrigatoria_m5_4_substituiu_candidato_sem_agenda": int(agenda_obrigatoria_substituiu_sem_agenda),
    }

    outputs_m5_4 = {
        "df_premanifestos_m5_4": df_premanifestos_m5_4,
        "df_itens_premanifestos_m5_4": df_itens_premanifestos_m5_4,
        "df_tentativas_m5_4": df_tentativas_m5_4,
        "df_remanescente_m5_4": df_remanescente_m5_4,
        "df_pool_mesorregiao_m5_4": df_pool_mesorregiao_m5_4,
        "df_blocos_cliente_mesorregiao_m5_4": df_blocos_cliente_mesorregiao_m5_4,
        "df_manifestos_m5_4": df_manifestos_m5_4,
        "df_itens_manifestos_m5_4": df_itens_manifestos_m5_4,
    }

    meta_m5_4 = {
        "resumo_m5_4b": resumo_m5_4b,
        "auditoria_m5_4b": auditoria_m5_4b,
    }

    return outputs_m5_4, meta_m5_4


def executar_m5_composicao_mesorregioes(*args: Any, **kwargs: Any):
    return executar_m5_4b_composicao_mesorregioes(*args, **kwargs)


def processar_m5_4b_composicao_mesorregioes(*args: Any, **kwargs: Any):
    return executar_m5_4b_composicao_mesorregioes(*args, **kwargs)


def rodar_m5_4b_composicao_mesorregioes(*args: Any, **kwargs: Any):
    return executar_m5_4b_composicao_mesorregioes(*args, **kwargs)
