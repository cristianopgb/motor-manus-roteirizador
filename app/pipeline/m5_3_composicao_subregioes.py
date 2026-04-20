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
    ocupacao_perc,
    grupo_respeita_restricao_veiculo,
)


MAX_CLIENTES_BASE = 10
MAX_PREFIXOS_POR_PERFIL = 8
MAX_TROCAS_1 = 20
MAX_TROCAS_2 = 30


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


def _ordenar_subregioes_por_massa(df_saldo: pd.DataFrame) -> List[str]:
    if df_saldo.empty:
        return []

    agrupado = (
        df_saldo.groupby(["subregiao"], dropna=False, sort=False)
        .agg(
            peso_total_subregiao=("peso_calculado", "sum"),
            qtd_linhas_subregiao=("id_linha_pipeline", "count"),
        )
        .reset_index()
        .sort_values(
            by=["peso_total_subregiao", "subregiao"],
            ascending=[False, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
    )

    return [safe_text(v) for v in agrupado["subregiao"].tolist()]


def _agrupar_blocos_cliente_na_subregiao(pool_df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if pool_df.empty:
        return pd.DataFrame()

    temp = pool_df.copy()

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
        return False, "excede_max_km", candidato

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
) -> Tuple[bool, str, pd.DataFrame]:
    ok_hard, motivo_hard, candidato_ajustado = _validar_hard_constraints(
        df_itens=df_itens,
        vehicle_row=vehicle_row,
        suffix=suffix,
    )
    if not ok_hard:
        return False, motivo_hard, candidato_ajustado

    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), 70.0)
    ocup = ocupacao_perc(candidato_ajustado, vehicle_row)

    if ocup < ocup_min:
        return False, "abaixo_ocupacao_minima", candidato_ajustado

    return True, "ok", candidato_ajustado


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
    subregiao: str,
    vehicle_row: Optional[pd.Series],
    resultado: str,
    motivo: str,
    df_candidato: Optional[pd.DataFrame],
    tentativa_idx: int,
    blocos_considerados: int,
) -> Dict[str, Any]:
    candidato = df_candidato if df_candidato is not None else pd.DataFrame()

    return {
        "subregiao": subregiao,
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
        "ocupacao_perc_candidato": round(ocupacao_perc(candidato, vehicle_row), 2)
        if vehicle_row is not None and not candidato.empty
        else 0.0,
    }


def _build_manifesto_id(seq: int) -> str:
    return f"PM53_{seq:04d}"


def _build_manifesto(
    df_itens: pd.DataFrame,
    vehicle_row: pd.Series,
    manifesto_id: str,
    subregiao: str,
    suffix: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_itens_limpo = _drop_internal_cols(df_itens, suffix=suffix)

    qtd_itens = int(len(df_itens_limpo))
    qtd_ctes = int(df_itens_limpo["cte"].nunique(dropna=True)) if "cte" in df_itens_limpo.columns else qtd_itens
    qtd_cidades = int(
        df_itens_limpo["cidade"].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()
    ) if "cidade" in df_itens_limpo.columns else 0

    manifesto = {
        "manifesto_id": manifesto_id,
        "tipo_manifesto": "pre_manifesto_bloco_5_3_subregiao",
        "subregiao": subregiao,
        "veiculo_tipo": safe_text(vehicle_row.get("tipo")),
        "veiculo_perfil": safe_text(vehicle_row.get("perfil")),
        "qtd_itens": qtd_itens,
        "qtd_ctes": qtd_ctes,
        "qtd_paradas": _qtd_paradas_validas(df_itens_limpo),
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
        "origem_etapa": "m5_3_composicao_subregiao",
    }

    df_manifesto = pd.DataFrame([manifesto])

    df_itens_saida = df_itens_limpo.copy()
    for k, v in manifesto.items():
        df_itens_saida[k] = v

    return df_manifesto, df_itens_saida


def _get_eligible_vehicles_for_subregiao(
    subregiao: str,
    perfis_elegiveis_df: pd.DataFrame,
) -> pd.DataFrame:
    base = perfis_elegiveis_df[
        perfis_elegiveis_df["subregiao"].fillna("").astype(str).str.strip() == subregiao
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


def _buscar_melhor_fechamento_na_subregiao(
    pool_df: pd.DataFrame,
    perfis_elegiveis_df: pd.DataFrame,
    subregiao: str,
    tentativas: List[Dict[str, Any]],
    suffix: str,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], str]:
    if pool_df.empty:
        return None, None, "subregiao_vazia"

    vehicles_sub = _get_eligible_vehicles_for_subregiao(
        subregiao=subregiao,
        perfis_elegiveis_df=perfis_elegiveis_df,
    )
    if vehicles_sub.empty:
        tentativas.append(
            _tentativa_dict(
                subregiao=subregiao,
                vehicle_row=None,
                resultado="falhou",
                motivo="sem_perfil_elegivel_na_subregiao",
                df_candidato=pool_df,
                tentativa_idx=1,
                blocos_considerados=0,
            )
        )
        return None, None, "sem_perfil_elegivel_na_subregiao"

    blocks_df = _agrupar_blocos_cliente_na_subregiao(pool_df, suffix=suffix)
    if blocks_df.empty:
        return None, None, "sem_blocos_na_subregiao"

    cliente_key_col = f"_cliente_key_{suffix}"

    melhor_df: Optional[pd.DataFrame] = None
    melhor_vehicle: Optional[pd.Series] = None
    melhor_score: Optional[Tuple[float, float, int, float]] = None
    melhor_motivo = "nenhum_fechamento"

    tentativa_idx = 1

    for _, vehicle_row in vehicles_sub.iterrows():
        blocks_df_compativeis = _filtrar_blocos_compativeis_por_perfil(
            pool_df=pool_df,
            blocks_df=blocks_df,
            vehicle_row=vehicle_row,
            suffix=suffix,
        )

        if blocks_df_compativeis.empty:
            tentativas.append(
                _tentativa_dict(
                    subregiao=subregiao,
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
                    subregiao=subregiao,
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
            ok, motivo, candidato = _validar_fechamento(
                df_itens=candidato_bruto,
                vehicle_row=vehicle_row,
                suffix=suffix,
            )

            tentativas.append(
                _tentativa_dict(
                    subregiao=subregiao,
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

            if not ok or candidato.empty:
                continue

            score = _score_candidato(candidato, vehicle_row)

            if melhor_score is None or score > melhor_score:
                melhor_score = score
                melhor_df = candidato.copy()
                melhor_vehicle = vehicle_row.copy()

    if melhor_df is None or melhor_vehicle is None:
        return None, None, melhor_motivo

    return melhor_df, melhor_vehicle, "ok"


def executar_m5_3_composicao_subregioes(
    df_saldo_elegivel_composicao_m5_3: pd.DataFrame,
    df_perfis_elegiveis_por_subregiao_m5_3: pd.DataFrame,
    rodada_id: Optional[str] = None,
    data_base_roteirizacao: Optional[Any] = None,
    tipo_roteirizacao: str = "carteira",
    caminhos_pipeline: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    del rodada_id, kwargs

    suffix = "m5_3b"

    saldo = normalize_saldo_m5(
        df_input=df_saldo_elegivel_composicao_m5_3,
        etapa="M5.3B",
        require_geo=True,
        require_subregiao=True,
        require_mesorregiao=False,
    )

    perfis_elegiveis = (
        df_perfis_elegiveis_por_subregiao_m5_3.copy()
        if df_perfis_elegiveis_por_subregiao_m5_3 is not None
        else pd.DataFrame()
    )

    if perfis_elegiveis.empty:
        raise ValueError("M5.3B exige df_perfis_elegiveis_por_subregiao_m5_3.")

    if saldo.empty:
        outputs_vazio = {
            "df_premanifestos_m5_3": pd.DataFrame(),
            "df_itens_premanifestos_m5_3": pd.DataFrame(),
            "df_tentativas_m5_3": pd.DataFrame(),
            "df_remanescente_m5_3": pd.DataFrame(),
        }
        meta_vazio = {
            "resumo_m5_3b": {
                "modulo": "M5.3B",
                "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
                "tipo_roteirizacao": tipo_roteirizacao,
                "linhas_entrada_m5_3": 0,
                "pre_manifestos_gerados_m5_3": 0,
                "itens_pre_manifestados_m5_3": 0,
                "remanescente_saida_m5_3": 0,
                "subregioes_processadas_m5_3": 0,
                "estrategia_m5_3": [
                    "subregiao_por_subregiao",
                    "solver_guiado_com_poda",
                    "filtro_previo_blocos_compativeis_por_perfil",
                    "poda_de_raio_por_cliente",
                    "maximiza_ocupacao_e_aproveitamento",
                    "multiplos_fechamentos_na_mesma_subregiao",
                    "VERSAO_M5_3B_2026_04_15_FIX_RESTRICAO",
                ],
                "caminhos_pipeline": caminhos_pipeline or {},
            },
            "auditoria_m5_3b": {
                "total_tentativas": 0,
                "total_pre_manifestos": 0,
                "total_itens_pre_manifestados": 0,
                "total_remanescentes": 0,
                "total_subregioes_processadas": 0,
            },
        }
        return outputs_vazio, meta_vazio

    saldo = precalcular_ordenacao_m5(saldo, suffix=suffix)
    saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    manifestos_list: List[pd.DataFrame] = []
    itens_manifestados_list: List[pd.DataFrame] = []
    tentativas: List[Dict[str, Any]] = []

    manifesto_seq = 1
    subregioes_processadas = 0

    subregioes_keys = _ordenar_subregioes_por_massa(saldo)

    for subregiao_key in subregioes_keys:
        subregioes_processadas += 1

        while True:
            pool_df = saldo[
                saldo["subregiao"].fillna("").astype(str).str.strip() == subregiao_key
            ].copy()

            if pool_df.empty:
                break

            candidato, vehicle_row, motivo = _buscar_melhor_fechamento_na_subregiao(
                pool_df=pool_df,
                perfis_elegiveis_df=perfis_elegiveis,
                subregiao=subregiao_key,
                tentativas=tentativas,
                suffix=suffix,
            )

            if candidato is None or vehicle_row is None:
                tentativas.append(
                    {
                        "subregiao": subregiao_key,
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
                subregiao=subregiao_key,
                suffix=suffix,
            )

            manifestos_list.append(df_manifesto)
            itens_manifestados_list.append(df_itens)

            ids_consumidos = set(candidato[f"_id_str_{suffix}"].tolist())
            saldo = saldo[~saldo[f"_id_str_{suffix}"].isin(ids_consumidos)].copy()

            if saldo.empty:
                break

            saldo = ordenar_operacional_m5(saldo, suffix=suffix)

    df_premanifestos_m5_3 = (
        pd.concat(manifestos_list, ignore_index=True)
        if manifestos_list
        else pd.DataFrame()
    )

    df_itens_premanifestos_m5_3 = (
        pd.concat(itens_manifestados_list, ignore_index=True)
        if itens_manifestados_list
        else pd.DataFrame()
    )

    df_tentativas_m5_3 = pd.DataFrame(tentativas)
    df_remanescente_m5_3 = _drop_internal_cols(saldo.reset_index(drop=True), suffix=suffix)

    resumo_m5_3b = {
        "modulo": "M5.3B",
        "data_base_roteirizacao": str(data_base_roteirizacao) if data_base_roteirizacao is not None else None,
        "tipo_roteirizacao": tipo_roteirizacao,
        "linhas_entrada_m5_3": int(len(df_saldo_elegivel_composicao_m5_3)),
        "pre_manifestos_gerados_m5_3": int(df_premanifestos_m5_3["manifesto_id"].nunique()) if not df_premanifestos_m5_3.empty else 0,
        "itens_pre_manifestados_m5_3": int(len(df_itens_premanifestos_m5_3)),
        "remanescente_saida_m5_3": int(len(df_remanescente_m5_3)),
        "subregioes_processadas_m5_3": int(subregioes_processadas),
        "estrategia_m5_3": [
            "subregiao_por_subregiao",
            "solver_guiado_com_poda",
            "filtro_previo_blocos_compativeis_por_perfil",
            "poda_de_raio_por_cliente",
            "maximiza_ocupacao_e_aproveitamento",
            "multiplos_fechamentos_na_mesma_subregiao",
            "VERSAO_M5_3B_2026_04_15_FIX_RESTRICAO",
        ],
        "caminhos_pipeline": caminhos_pipeline or {},
    }

    auditoria_m5_3b = {
        "total_tentativas": int(len(df_tentativas_m5_3)),
        "total_pre_manifestos": int(df_premanifestos_m5_3["manifesto_id"].nunique()) if not df_premanifestos_m5_3.empty else 0,
        "total_itens_pre_manifestados": int(len(df_itens_premanifestos_m5_3)),
        "total_remanescentes": int(len(df_remanescente_m5_3)),
        "total_subregioes_processadas": int(subregioes_processadas),
    }

    outputs_m5_3 = {
        "df_premanifestos_m5_3": df_premanifestos_m5_3,
        "df_itens_premanifestos_m5_3": df_itens_premanifestos_m5_3,
        "df_tentativas_m5_3": df_tentativas_m5_3,
        "df_remanescente_m5_3": df_remanescente_m5_3,
    }

    meta_m5_3 = {
        "resumo_m5_3b": resumo_m5_3b,
        "auditoria_m5_3b": auditoria_m5_3b,
    }

    return outputs_m5_3, meta_m5_3


def executar_m5_composicao_subregioes(*args: Any, **kwargs: Any):
    return executar_m5_3_composicao_subregioes(*args, **kwargs)


def processar_m5_3_composicao_subregioes(*args: Any, **kwargs: Any):
    return executar_m5_3_composicao_subregioes(*args, **kwargs)


def rodar_m5_3_composicao_subregioes(*args: Any, **kwargs: Any):
    return executar_m5_3_composicao_subregioes(*args, **kwargs)
