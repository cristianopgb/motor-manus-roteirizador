from __future__ import annotations

import json
import re
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


OCUPACAO_MINIMA_PADRAO = 0.70
OCUPACAO_MAXIMA_PADRAO = 1.00


# ============================================================
# UTILITÁRIOS BÁSICOS
# ============================================================

def _agora() -> float:
    return time.perf_counter()


def _duracao_ms(inicio: float) -> float:
    return round((time.perf_counter() - inicio) * 1000, 2)


def _to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df is None or len(df) == 0:
        return []

    df2 = df.copy()

    for col in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[col]):
            df2[col] = df2[col].astype(str)

    df2 = df2.where(pd.notnull(df2), None)
    return df2.to_dict(orient="records")


def _scalar_safe(x: Any) -> Any:
    if isinstance(x, pd.Series):
        return x.iloc[0] if len(x) > 0 else np.nan
    if isinstance(x, np.ndarray):
        return x[0] if len(x) > 0 else np.nan
    if isinstance(x, list):
        return x[0] if len(x) > 0 else np.nan
    if isinstance(x, tuple):
        return x[0] if len(x) > 0 else np.nan
    return x


def _bool_safe(x: Any) -> bool:
    x = _scalar_safe(x)

    try:
        if pd.isna(x):
            return False
    except Exception:
        pass

    if isinstance(x, (bool, np.bool_)):
        return bool(x)

    if isinstance(x, (int, float, np.integer, np.floating)):
        try:
            if pd.isna(x):
                return False
        except Exception:
            pass
        return bool(int(x))

    txt = str(x).strip().lower()
    return txt in {"true", "1", "sim", "s", "yes", "y"}


def _num_safe(x: Any, default: float = np.nan) -> float:
    x = _scalar_safe(x)
    val = pd.to_numeric(x, errors="coerce")
    return float(val) if pd.notna(val) else default


def _int_safe(x: Any, default: int = 0) -> int:
    x = _scalar_safe(x)
    val = pd.to_numeric(x, errors="coerce")
    if pd.isna(val):
        return default
    return int(val)


def _deduplicar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df.columns) == 0:
        return df.copy()
    if not df.columns.duplicated().any():
        return df.copy()
    return df.loc[:, ~df.columns.duplicated()].copy()


def _garantir_coluna_por_alias(
    df: pd.DataFrame,
    coluna_destino: str,
    aliases: List[str],
    default: Any = None,
) -> pd.DataFrame:
    if coluna_destino in df.columns:
        return df

    for alias in aliases:
        if alias in df.columns:
            df[coluna_destino] = df[alias]
            return df

    df[coluna_destino] = default
    return df


def _normalizar_tipo_roteirizacao(valor: Any) -> str:
    txt = str(valor).strip().lower() if valor is not None else "carteira"
    if txt not in {"carteira", "frota"}:
        return "carteira"
    return txt


def _normalizar_configuracao_frota(configuracao_frota: Any) -> pd.DataFrame:
    if configuracao_frota is None:
        return pd.DataFrame(columns=["perfil", "quantidade"])

    if isinstance(configuracao_frota, pd.DataFrame):
        cfg = configuracao_frota.copy()
    else:
        rows: List[Dict[str, Any]] = []
        for item in configuracao_frota:
            if hasattr(item, "model_dump"):
                rows.append(item.model_dump(exclude_none=False))
            elif isinstance(item, dict):
                rows.append(item)
            else:
                rows.append(dict(item))
        cfg = pd.DataFrame(rows)

    if len(cfg) == 0:
        return pd.DataFrame(columns=["perfil", "quantidade"])

    if "perfil" not in cfg.columns or "quantidade" not in cfg.columns:
        return pd.DataFrame(columns=["perfil", "quantidade"])

    cfg["perfil"] = cfg["perfil"].astype(str).str.strip()
    cfg["quantidade"] = pd.to_numeric(cfg["quantidade"], errors="coerce").fillna(0).astype(int)
    cfg = cfg.loc[(cfg["perfil"] != "") & (cfg["quantidade"] > 0)].copy()

    if len(cfg) == 0:
        return pd.DataFrame(columns=["perfil", "quantidade"])

    cfg = cfg.groupby("perfil", as_index=False)["quantidade"].sum()
    return cfg.reset_index(drop=True)


def _normalizar_str(x: Any) -> str:
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip().upper()


# ============================================================
# RESTRIÇÃO DE VEÍCULO
# REGRA VALIDADA: o veículo marcado é o permitido / exigido
# ============================================================

def _normalizar_token_restricao(x: Any) -> str:
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


def _expandir_alias_restricao(token: str) -> set[str]:
    token = _normalizar_token_restricao(token)
    if token == "":
        return set()

    aliases = {
        "VUC": {"VUC"},
        "3_4": {"3_4", "TRES_QUARTOS", "TRES_QUARTO"},
        "TOCO": {"TOCO"},
        "TRUCK": {"TRUCK"},
        "CARRETA": {"CARRETA"},
        "UTILITARIO": {"UTILITARIO", "UTILITARIOS", "FIORINO", "VAN"},
        "CARRO": {"CARRO", "PASSEIO", "VEICULO_LEVE"},
    }

    for canonico, grupo in aliases.items():
        if token == canonico or token in grupo:
            return {canonico} | grupo

    return {token}


def _tokens_restricao_valor(valor: Any) -> set[str]:
    if valor is None:
        return set()

    try:
        if pd.isna(valor):
            return set()
    except Exception:
        pass

    txt = str(valor).strip()
    if txt == "":
        return set()

    partes = re.split(r"[;,|/]+", txt)
    tokens: set[str] = set()

    for parte in partes:
        token = _normalizar_token_restricao(parte)
        if token != "":
            tokens |= _expandir_alias_restricao(token)

    return tokens


def _veiculo_compativel_com_restricao(veiculo_tipo: Any, restricao_valor: Any) -> bool:
    tokens_restricao = _tokens_restricao_valor(restricao_valor)
    if len(tokens_restricao) == 0:
        return True

    tokens_veiculo = _expandir_alias_restricao(veiculo_tipo)
    return len(tokens_restricao & tokens_veiculo) > 0


def _combo_respeita_restricao_veiculo(df_combo: pd.DataFrame, veic: pd.Series) -> bool:
    if "restricao_veiculo" not in df_combo.columns:
        return True

    tipo_veiculo = veic.get("tipo")
    restricoes = df_combo["restricao_veiculo"].tolist()

    for restricao in restricoes:
        if not _veiculo_compativel_com_restricao(tipo_veiculo, restricao):
            return False

    return True


# ============================================================
# REGRAS DE LINHA / CLIENTE
# ============================================================

def _eh_exclusivo(row: pd.Series) -> bool:
    if "veiculo_exclusivo_flag" in row.index:
        return _bool_safe(row.get("veiculo_exclusivo_flag"))
    return _bool_safe(row.get("veiculo_exclusivo"))


def _cliente_key(row: pd.Series) -> str:
    return _normalizar_str(row.get("destinatario"))


def _chave_parada_df(df_: pd.DataFrame) -> pd.Series:
    return (
        df_["destinatario"].astype(str).fillna("").str.strip().str.upper()
        + "|"
        + df_["cidade"].astype(str).fillna("").str.strip().str.upper()
        + "|"
        + df_["uf"].astype(str).fillna("").str.strip().str.upper()
    )


def _obter_coluna_id_documento(df: pd.DataFrame) -> str:
    if "cte" in df.columns:
        return "cte"
    return "id_linha_pipeline"


def _score_prioridade_embarque(valor: Any) -> int:
    if valor is None:
        return 0

    try:
        if pd.isna(valor):
            return 0
    except Exception:
        pass

    try:
        num = float(valor)
        if num == 1:
            return 120
        if num == 2:
            return 90
        if num == 3:
            return 60
        if num == 4:
            return 30
        if num >= 5:
            return 10
    except Exception:
        pass

    texto = _normalizar_str(valor)
    texto = "".join(
        c for c in unicodedata.normalize("NFKD", texto)
        if not unicodedata.combining(c)
    )

    mapa = {
        "SIM": 120,
        "ALTA": 120,
        "ALTO": 120,
        "URGENTE": 120,
        "MEDIA": 60,
        "MEDIO": 60,
        "NORMAL": 30,
        "BAIXA": 10,
        "BAIXO": 10,
        "NAO": 0,
        "NÃO": 0,
    }

    return mapa.get(texto, 0)


def _score_ordem_fila(row: pd.Series) -> Tuple[Any, ...]:
    exclusivo = 0 if _eh_exclusivo(row) else 1
    prioridade_score = _score_prioridade_embarque(row.get("prioridade_embarque", np.nan))
    data_agenda = _scalar_safe(row.get("data_agenda", pd.NaT))
    folga = _num_safe(row.get("folga_dias", np.nan), default=np.nan)
    score = _num_safe(row.get("score_prioridade_preliminar", 0), default=0)
    km = _num_safe(row.get("distancia_rodoviaria_est_km", np.nan), default=np.nan)
    base_carga = _num_safe(row.get("peso_calculado", 0), default=0)

    if prioridade_score >= 120:
        grupo = 1
    elif pd.notna(data_agenda):
        grupo = 2
    elif pd.notna(folga) and folga >= 0:
        grupo = 3
    else:
        grupo = 4

    folga_ordem = folga if pd.notna(folga) else 999999
    km_ordem = km if pd.notna(km) else 999999

    return (
        exclusivo,
        grupo,
        -prioridade_score,
        folga_ordem,
        -score,
        km_ordem,
        -base_carga,
    )


def _ordenar_fila(df_: pd.DataFrame) -> pd.DataFrame:
    if len(df_) == 0:
        return df_.copy()

    return (
        df_.assign(__ord__=df_.apply(_score_ordem_fila, axis=1))
        .sort_values("__ord__")
        .drop(columns="__ord__")
        .reset_index(drop=True)
    )


def _ordenar_cliente_por_peso(df_cliente: pd.DataFrame) -> pd.DataFrame:
    if len(df_cliente) == 0:
        return df_cliente.copy()

    return (
        df_cliente.sort_values(
            by=["peso_calculado", "score_prioridade_preliminar", "distancia_rodoviaria_est_km"],
            ascending=[False, False, True],
            na_position="last",
        )
        .reset_index(drop=True)
    )


def _agrupar_clientes_ordenados(df_base: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
    if len(df_base) == 0:
        return []

    trabalho = df_base.copy()
    trabalho["cliente_key"] = trabalho.apply(_cliente_key, axis=1)

    resumo = (
        trabalho.groupby("cliente_key", as_index=False)["peso_calculado"]
        .sum()
        .sort_values("peso_calculado", ascending=False)
        .reset_index(drop=True)
    )

    grupos: List[Tuple[str, pd.DataFrame]] = []
    for cliente in resumo["cliente_key"].tolist():
        grupo = trabalho.loc[trabalho["cliente_key"] == cliente].copy()
        grupo = _ordenar_cliente_por_peso(grupo)
        grupos.append((cliente, grupo))

    return grupos


# ============================================================
# VEÍCULOS / CATÁLOGO
# ============================================================

def _resolver_coluna_tipo_veiculo(df_veiculos: pd.DataFrame) -> str:
    if "tipo" in df_veiculos.columns:
        return "tipo"
    if "perfil" in df_veiculos.columns:
        return "perfil"
    raise Exception("Faltam colunas mínimas na base de veículos: tipo ou perfil.")


def _preparar_catalogo_veiculos(
    df_veic: pd.DataFrame,
    coluna_tipo_veiculo: str,
    tipo_roteirizacao: str,
    configuracao_frota: Any,
) -> pd.DataFrame:
    cat = df_veic.copy()

    colunas_min = [
        coluna_tipo_veiculo,
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
    ]
    cat = cat.loc[cat[colunas_min].notna().all(axis=1)].copy()

    cat["tipo"] = cat[coluna_tipo_veiculo].astype(str).str.strip()
    cat["capacidade_peso_kg"] = pd.to_numeric(cat["capacidade_peso_kg"], errors="coerce")
    cat["capacidade_vol_m3"] = pd.to_numeric(cat["capacidade_vol_m3"], errors="coerce")
    cat["max_entregas"] = pd.to_numeric(cat["max_entregas"], errors="coerce")
    cat["max_km_distancia"] = pd.to_numeric(cat["max_km_distancia"], errors="coerce")
    cat["ocupacao_minima_perc"] = pd.to_numeric(cat.get("ocupacao_minima_perc", np.nan), errors="coerce")

    cat = (
        cat.groupby("tipo", as_index=False)
        .agg(
            {
                "capacidade_peso_kg": "max",
                "capacidade_vol_m3": "max",
                "max_entregas": "max",
                "max_km_distancia": "max",
                "ocupacao_minima_perc": "max",
            }
        )
        .sort_values(
            by=["capacidade_peso_kg", "capacidade_vol_m3", "max_entregas", "max_km_distancia"],
            ascending=[True, True, True, True],
        )
        .reset_index(drop=True)
    )

    cat["ocupacao_minima_perc"] = cat["ocupacao_minima_perc"].where(
        cat["ocupacao_minima_perc"].notna(),
        OCUPACAO_MINIMA_PADRAO * 100,
    )

    tipo_roteirizacao = _normalizar_tipo_roteirizacao(tipo_roteirizacao)

    if tipo_roteirizacao == "frota":
        cfg = _normalizar_configuracao_frota(configuracao_frota)
        if len(cfg) == 0:
            raise Exception("tipo_roteirizacao = 'frota', mas configuracao_frota está vazia ou inválida.")

        cat = cat.merge(cfg, how="inner", left_on="tipo", right_on="perfil")
        if len(cat) == 0:
            raise Exception("Nenhum perfil da configuracao_frota foi encontrado no catálogo de veículos.")

        cat["limite_manifestos"] = pd.to_numeric(cat["quantidade"], errors="coerce").fillna(0).astype(int)
        cat.drop(columns=["perfil", "quantidade"], inplace=True, errors="ignore")
    else:
        cat["limite_manifestos"] = np.nan

    cat["manifestos_utilizados"] = 0
    cat["ordem_porte"] = np.arange(1, len(cat) + 1)
    return cat.reset_index(drop=True)


def _obter_menor_perfil_por_capacidade(catalogo_veiculos: pd.DataFrame) -> pd.Series:
    return (
        catalogo_veiculos.sort_values(
            by=["capacidade_peso_kg", "capacidade_vol_m3", "max_entregas", "max_km_distancia"],
            ascending=[True, True, True, True],
        )
        .iloc[0]
    )


def _veiculo_disponivel_no_modo_frota(veic: pd.Series, tipo_roteirizacao: str) -> bool:
    tipo_roteirizacao = _normalizar_tipo_roteirizacao(tipo_roteirizacao)
    if tipo_roteirizacao == "carteira":
        return True

    limite = _num_safe(veic.get("limite_manifestos"), default=np.nan)
    usados = _num_safe(veic.get("manifestos_utilizados"), default=0)

    if pd.isna(limite):
        return True

    return int(usados) < int(limite)


def _consumir_veiculo_catalogo(
    catalogo_veiculos: pd.DataFrame,
    catalogo_idx: Optional[int],
    tipo_roteirizacao: str,
) -> None:
    if catalogo_idx is None:
        return

    tipo_roteirizacao = _normalizar_tipo_roteirizacao(tipo_roteirizacao)
    if tipo_roteirizacao != "frota":
        return

    if catalogo_idx not in catalogo_veiculos.index:
        return

    atual = _int_safe(catalogo_veiculos.at[catalogo_idx, "manifestos_utilizados"], default=0)
    catalogo_veiculos.at[catalogo_idx, "manifestos_utilizados"] = atual + 1


def _perfis_compativeis_por_raio(
    catalogo_veiculos: pd.DataFrame,
    km_referencia: float,
    tipo_roteirizacao: str,
) -> pd.DataFrame:
    comp = catalogo_veiculos.loc[catalogo_veiculos["max_km_distancia"] >= km_referencia].copy()

    if len(comp) == 0:
        return comp

    if _normalizar_tipo_roteirizacao(tipo_roteirizacao) == "frota":
        comp = comp.loc[
            comp.apply(lambda row: _veiculo_disponivel_no_modo_frota(row, tipo_roteirizacao), axis=1)
        ].copy()

    return comp.sort_values(
        by=["capacidade_peso_kg", "capacidade_vol_m3", "max_entregas", "max_km_distancia"],
        ascending=[False, False, False, False],
    ).reset_index()


# ============================================================
# AVALIAÇÃO DE COMBO
# ============================================================

def _obter_base_carga_oficial(df_combo: pd.DataFrame) -> float:
    return float(pd.to_numeric(df_combo["peso_calculado"], errors="coerce").fillna(0).sum())


def _calcular_qtd_paradas(df_combo: pd.DataFrame) -> int:
    return int(_chave_parada_df(df_combo).nunique())


def _calcular_km_referencia(df_combo: pd.DataFrame) -> float:
    return float(pd.to_numeric(df_combo["distancia_rodoviaria_est_km"], errors="coerce").max())


def _avaliar_combo_no_veiculo(
    df_combo: pd.DataFrame,
    veic: pd.Series,
    ignorar_ocupacao_minima: bool = False,
    ignorar_raio: bool = False,
) -> Dict[str, Any]:
    base_carga_total = _obter_base_carga_oficial(df_combo)
    peso_total_kg = float(pd.to_numeric(df_combo["peso_kg"], errors="coerce").fillna(0).sum())
    vol_total_m3 = float(pd.to_numeric(df_combo["vol_m3"], errors="coerce").fillna(0).sum())
    km_combo = _calcular_km_referencia(df_combo)

    col_doc = _obter_coluna_id_documento(df_combo)
    qtd_ctes = int(df_combo[col_doc].astype(str).nunique())
    qtd_itens = int(len(df_combo))
    qtd_paradas = _calcular_qtd_paradas(df_combo)

    cap_peso = float(veic["capacidade_peso_kg"])
    cap_vol = float(veic["capacidade_vol_m3"])
    max_entregas = int(veic["max_entregas"])
    max_km = float(veic["max_km_distancia"])

    cabe_carga_oficial = base_carga_total <= cap_peso
    cabe_paradas = qtd_paradas <= max_entregas
    cabe_vol = vol_total_m3 <= cap_vol if pd.notna(cap_vol) else True
    cabe_km = True if ignorar_raio else (km_combo <= max_km if pd.notna(km_combo) else False)
    cabe_restricao_veiculo = _combo_respeita_restricao_veiculo(df_combo=df_combo, veic=veic)

    ocupacao_oficial = base_carga_total / cap_peso if pd.notna(cap_peso) and cap_peso > 0 else np.nan

    if ignorar_ocupacao_minima:
        passa_ocupacao = True
    else:
        passa_ocupacao = (
            pd.notna(ocupacao_oficial)
            and ocupacao_oficial >= OCUPACAO_MINIMA_PADRAO
            and ocupacao_oficial < OCUPACAO_MAXIMA_PADRAO
        )

    aceito = bool(
        cabe_carga_oficial
        and cabe_paradas
        and cabe_vol
        and cabe_km
        and passa_ocupacao
        and cabe_restricao_veiculo
    )

    return {
        "veiculo_tipo": veic["tipo"],
        "capacidade_peso_kg": cap_peso,
        "capacidade_vol_m3": cap_vol,
        "max_entregas": max_entregas,
        "max_km_distancia": max_km,
        "base_carga_oficial": round(base_carga_total, 3),
        "peso_total_kg": round(peso_total_kg, 3),
        "vol_total_m3": round(vol_total_m3, 3),
        "km_referencia": round(km_combo, 2) if pd.notna(km_combo) else np.nan,
        "qtd_itens": qtd_itens,
        "qtd_ctes": qtd_ctes,
        "qtd_paradas": qtd_paradas,
        "cabe_carga_oficial": cabe_carga_oficial,
        "cabe_paradas": cabe_paradas,
        "cabe_vol": cabe_vol,
        "cabe_km": cabe_km,
        "cabe_restricao_veiculo": cabe_restricao_veiculo,
        "ocupacao_oficial_perc": round(float(ocupacao_oficial * 100), 2) if pd.notna(ocupacao_oficial) else np.nan,
        "passa_ocupacao": passa_ocupacao,
        "ignorar_ocupacao_minima": bool(ignorar_ocupacao_minima),
        "ignorar_raio": bool(ignorar_raio),
        "aceito": aceito,
    }


def _motivo_reprovacao(avaliacao: Dict[str, Any], exigir_ocupacao: bool = True, exigir_raio: bool = True) -> str:
    motivos: List[str] = []

    if not avaliacao.get("cabe_carga_oficial", True):
        motivos.append("excede_capacidade_peso_oficial")
    if not avaliacao.get("cabe_paradas", True):
        motivos.append("excede_max_entregas")
    if not avaliacao.get("cabe_vol", True):
        motivos.append("excede_capacidade_volume")
    if exigir_raio and not avaliacao.get("cabe_km", True):
        motivos.append("excede_max_km")
    if exigir_ocupacao and not avaliacao.get("passa_ocupacao", True):
        motivos.append("nao_atinge_faixa_ocupacao_70_100")
    if not avaliacao.get("cabe_restricao_veiculo", True):
        motivos.append("restricao_veiculo_incompativel")

    if len(motivos) == 0:
        return "rejeitado_sem_motivo_detalhado"

    return "|".join(motivos)


def _gerar_resumo_manifesto(
    df_combo: pd.DataFrame,
    avaliacao: Dict[str, Any],
    manifesto_id: str,
    tipo_manifesto: str,
    origem_etapa: str,
) -> Dict[str, Any]:
    linha = {
        "manifesto_id": manifesto_id,
        "tipo_manifesto": tipo_manifesto,
        "veiculo_tipo": avaliacao["veiculo_tipo"],
        "qtd_itens": avaliacao["qtd_itens"],
        "qtd_ctes": avaliacao["qtd_ctes"],
        "qtd_paradas": avaliacao["qtd_paradas"],
        "base_carga_oficial": avaliacao["base_carga_oficial"],
        "peso_total_kg": avaliacao["peso_total_kg"],
        "vol_total_m3": avaliacao["vol_total_m3"],
        "km_referencia": avaliacao["km_referencia"],
        "ocupacao_oficial_perc": avaliacao["ocupacao_oficial_perc"],
        "capacidade_peso_kg_veiculo": avaliacao["capacidade_peso_kg"],
        "capacidade_vol_m3_veiculo": avaliacao["capacidade_vol_m3"],
        "max_entregas_veiculo": avaliacao["max_entregas"],
        "max_km_distancia_veiculo": avaliacao["max_km_distancia"],
        "ignorar_ocupacao_minima": avaliacao["ignorar_ocupacao_minima"],
        "ignorar_raio": avaliacao.get("ignorar_raio", False),
        "origem_modulo": 4,
        "origem_etapa": origem_etapa,
    }

    if len(df_combo) > 0:
        linha["destinatario"] = df_combo["destinatario"].iloc[0]
        linha["cidade"] = df_combo["cidade"].iloc[0] if "cidade" in df_combo.columns else np.nan
        linha["uf"] = df_combo["uf"].iloc[0] if "uf" in df_combo.columns else np.nan
        linha["mesorregiao"] = df_combo["mesorregiao"].iloc[0] if "mesorregiao" in df_combo.columns else np.nan
        linha["subregiao"] = df_combo["subregiao"].iloc[0] if "subregiao" in df_combo.columns else np.nan

    return linha


# ============================================================
# CONTEXTO INTERNO DE EXECUÇÃO
# ============================================================

def _inicializar_contexto_execucao() -> Dict[str, Any]:
    return {
        "manifestos_fechados": [],
        "itens_manifestos_fechados": [],
        "tentativas_fechamento": [],
        "ids_alocados": set(),
        "contador_manifesto": 1,
    }


def _contabilizar_tentativa(contadores_m4: Dict[str, Any], tent: Dict[str, Any]) -> None:
    contadores_m4["qtd_tentativas_total"] += 1
    motivo = str(tent.get("motivo_reprovacao", "")).strip()

    if motivo == "perfil_sem_disponibilidade_no_modo_frota":
        contadores_m4["qtd_tentativas_sem_disponibilidade_frota"] += 1
    if "nao_atinge_faixa_ocupacao_70_100" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_ocupacao"] += 1
    if "excede_max_km" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_km"] += 1
    if "excede_max_entregas" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_paradas"] += 1
    if "excede_capacidade_peso_oficial" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_capacidade"] += 1
    if "excede_capacidade_volume" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_volume"] += 1
    if "restricao_veiculo_incompativel" in motivo:
        contadores_m4["qtd_tentativas_rejeitadas_restricao_veiculo"] += 1


def _filtrar_nao_alocados(df_base: pd.DataFrame, ids_alocados: set[str]) -> pd.DataFrame:
    if len(df_base) == 0:
        return df_base.copy().reset_index(drop=True)
    mask = ~df_base["id_linha_pipeline"].astype(str).isin(ids_alocados)
    return df_base.loc[mask].copy().reset_index(drop=True)


def _registrar_manifesto(
    ctx: Dict[str, Any],
    catalogo_veiculos: pd.DataFrame,
    tipo_roteirizacao: str,
    df_combo: pd.DataFrame,
    avaliacao: Dict[str, Any],
    origem_etapa: str,
    catalogo_idx: Optional[int],
) -> None:
    if len(df_combo) == 0:
        raise Exception("Tentativa de registrar manifesto vazio.")

    ids_combo = set(df_combo["id_linha_pipeline"].astype(str).tolist())

    if ids_combo & ctx["ids_alocados"]:
        raise Exception("Manifesto inválido: há id_linha_pipeline já alocado em outro manifesto.")

    if not avaliacao.get("ignorar_ocupacao_minima", False):
        ocup = _num_safe(avaliacao.get("ocupacao_oficial_perc"), default=np.nan)
        if pd.isna(ocup) or ocup < 70 or ocup >= 100:
            raise Exception(
                f"Manifesto inválido: ocupação fora da faixa 70% <= x < 100%. Valor encontrado: {ocup}"
            )

    manifesto_id = f"MF4_{ctx['contador_manifesto']:04d}"
    ctx["contador_manifesto"] += 1

    resumo = _gerar_resumo_manifesto(
        df_combo=df_combo,
        avaliacao=avaliacao,
        manifesto_id=manifesto_id,
        tipo_manifesto="fechado_bloco_4",
        origem_etapa=origem_etapa,
    )
    ctx["manifestos_fechados"].append(resumo)

    itens = df_combo.copy().reset_index(drop=True)
    itens["manifesto_id"] = manifesto_id
    itens["tipo_manifesto"] = "fechado_bloco_4"
    itens["veiculo_tipo"] = avaliacao["veiculo_tipo"]
    itens["capacidade_peso_kg_veiculo"] = avaliacao["capacidade_peso_kg"]
    itens["capacidade_vol_m3_veiculo"] = avaliacao["capacidade_vol_m3"]
    itens["max_entregas_veiculo"] = avaliacao["max_entregas"]
    itens["max_km_distancia_veiculo"] = avaliacao["max_km_distancia"]
    itens["base_carga_oficial_manifesto"] = avaliacao["base_carga_oficial"]
    itens["ocupacao_oficial_perc_manifesto"] = avaliacao["ocupacao_oficial_perc"]
    itens["ignorar_ocupacao_minima_manifesto"] = avaliacao["ignorar_ocupacao_minima"]
    itens["ignorar_raio_manifesto"] = avaliacao.get("ignorar_raio", False)
    itens["origem_modulo"] = 4
    itens["origem_etapa"] = origem_etapa

    ctx["itens_manifestos_fechados"].append(itens)
    ctx["ids_alocados"].update(ids_combo)
    _consumir_veiculo_catalogo(catalogo_veiculos, catalogo_idx, tipo_roteirizacao)


# ============================================================
# BLOCO 1 - PREPARAÇÃO
# ============================================================

def _preparar_input_m4(
    df_input_oficial_bloco_4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
    tipo_roteirizacao: str,
    configuracao_frota: Any,
) -> Dict[str, Any]:
    fila = _deduplicar_colunas(df_input_oficial_bloco_4.copy().reset_index(drop=True))
    veiculos = _deduplicar_colunas(df_veiculos_tratados.copy().reset_index(drop=True))

    fila = _garantir_coluna_por_alias(fila, "destinatario", ["Destinatário", "cliente"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "cidade", ["Cidade Dest.", "Cida", "cidade_dest", "cidade_destino"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "uf", ["UF"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "peso_kg", ["Peso", "peso"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "vol_m3", ["Peso C", "Peso Cub.", "peso_c", "cubagem_m3"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "peso_calculado", ["Peso Calculado", "Peso Calculo", "peso_calc"], default=np.nan)
    fila = _garantir_coluna_por_alias(
        fila,
        "veiculo_exclusivo",
        ["Veiculo Exclusivo", "Carro Dedicado", "veiculo_dedicado", "carro_dedicado"],
        default=np.nan,
    )
    fila = _garantir_coluna_por_alias(
        fila,
        "veiculo_exclusivo_flag",
        ["flag_veiculo_exclusivo", "veiculo_exclusivo_bool"],
        default=False,
    )
    fila = _garantir_coluna_por_alias(fila, "restricao_veiculo", ["Restrição Veículo", "restricao_veiculo"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "prioridade_embarque", ["Prioridade", "prioridade"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "distancia_rodoviaria_est_km", ["km_referencia", "distancia_km", "km_rota_referencia"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "status_triagem", ["status_roteirizacao", "status_fila"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "grupo_saida", ["grupo_pipeline", "grupo_status"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "data_agenda", ["Agendam.", "agenda_data", "data_agendamento"], default=pd.NaT)
    fila = _garantir_coluna_por_alias(fila, "data_leadtime", ["D.L.E.", "dle", "leadtime_data_limite_entrega"], default=pd.NaT)
    fila = _garantir_coluna_por_alias(fila, "ranking_prioridade", ["ranking_prioridade_operacional", "ranking_preliminar"], default=999999)
    fila = _garantir_coluna_por_alias(fila, "score_prioridade_preliminar", ["score_prioridade", "score_operacional"], default=0.0)
    fila = _garantir_coluna_por_alias(fila, "id_linha_pipeline", ["id", "id_linha", "hash_linha_pipeline"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "cte", ["nro_documento", "romaneio", "nro_doc", "Nro Doc."], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "mesorregiao", ["Mesoregião", "mesorregiao"], default=np.nan)
    fila = _garantir_coluna_por_alias(fila, "subregiao", ["Sub-Região", "subregiao", "sub_regiao"], default=np.nan)

    coluna_tipo_veiculo = _resolver_coluna_tipo_veiculo(veiculos)

    colunas_minimas_fila = [
        "id_linha_pipeline",
        "destinatario",
        "cidade",
        "uf",
        "peso_kg",
        "vol_m3",
        "peso_calculado",
        "distancia_rodoviaria_est_km",
        "status_triagem",
        "grupo_saida",
    ]
    colunas_minimas_veiculos = [
        coluna_tipo_veiculo,
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
    ]

    faltam_fila = [c for c in colunas_minimas_fila if c not in fila.columns]
    faltam_veiculos = [c for c in colunas_minimas_veiculos if c not in veiculos.columns]

    if faltam_fila:
        raise Exception("Faltam colunas mínimas na fila oficial do Bloco 4:\n- " + "\n- ".join(faltam_fila))

    if faltam_veiculos:
        raise Exception("Faltam colunas mínimas na base de veículos:\n- " + "\n- ".join(faltam_veiculos))

    linhas_input_invalido = fila.loc[
        (fila["status_triagem"].astype(str) != "roteirizavel")
        | (fila["grupo_saida"].astype(str) != "df_carteira_roteirizavel")
    ].copy()
    if len(linhas_input_invalido) > 0:
        raise Exception(
            "O BLOCO 4 recebeu linhas incompatíveis com o estágio. "
            "Há registros com status_triagem != 'roteirizavel' ou grupo_saida inválido."
        )

    if fila["id_linha_pipeline"].isna().any():
        qtd_nulos = int(fila["id_linha_pipeline"].isna().sum())
        raise Exception(f"O input oficial do Bloco 4 possui id_linha_pipeline nulo: {qtd_nulos}")

    if fila["id_linha_pipeline"].astype(str).duplicated().any():
        qtd_dup = int(fila["id_linha_pipeline"].astype(str).duplicated().sum())
        raise Exception(f"O input oficial do Bloco 4 possui id_linha_pipeline duplicado: {qtd_dup}")

    for col in [
        "peso_kg",
        "vol_m3",
        "peso_calculado",
        "distancia_rodoviaria_est_km",
        "ranking_prioridade",
        "score_prioridade_preliminar",
        "folga_dias",
    ]:
        if col in fila.columns:
            fila[col] = pd.to_numeric(fila[col], errors="coerce")

    for col in ["capacidade_peso_kg", "capacidade_vol_m3", "max_entregas", "max_km_distancia"]:
        veiculos[col] = pd.to_numeric(veiculos[col], errors="coerce")

    for col in ["data_agenda", "data_leadtime"]:
        if col in fila.columns:
            fila[col] = pd.to_datetime(fila[col], errors="coerce")

    fila["veiculo_exclusivo_flag"] = fila.apply(_eh_exclusivo, axis=1)

    if fila["cte"].isna().all():
        fila["cte"] = fila["id_linha_pipeline"].astype(str)
    else:
        fila["cte"] = fila["cte"].fillna(fila["id_linha_pipeline"].astype(str))

    fila["ranking_prioridade"] = pd.to_numeric(fila["ranking_prioridade"], errors="coerce").fillna(999999)
    fila["score_prioridade_preliminar"] = pd.to_numeric(fila["score_prioridade_preliminar"], errors="coerce").fillna(0.0)
    fila["peso_calculado"] = pd.to_numeric(fila["peso_calculado"], errors="coerce")
    fila["peso_kg"] = pd.to_numeric(fila["peso_kg"], errors="coerce")
    fila["vol_m3"] = pd.to_numeric(fila["vol_m3"], errors="coerce")
    fila["distancia_rodoviaria_est_km"] = pd.to_numeric(fila["distancia_rodoviaria_est_km"], errors="coerce")

    if "restricao_veiculo" not in fila.columns:
        fila["restricao_veiculo"] = np.nan

    if fila["peso_calculado"].isna().any():
        qtd_nulos = int(fila["peso_calculado"].isna().sum())
        raise Exception(
            f"O M4 recebeu {qtd_nulos} linhas sem peso_calculado. "
            "Como peso_calculado é a base oficial de ocupação/capacidade, o input do bloco 4 precisa estar completo."
        )

    catalogo_veiculos = _preparar_catalogo_veiculos(
        df_veic=veiculos,
        coluna_tipo_veiculo=coluna_tipo_veiculo,
        tipo_roteirizacao=tipo_roteirizacao,
        configuracao_frota=configuracao_frota,
    )

    fila_ordenada = _ordenar_fila(fila)

    return {
        "fila": fila,
        "fila_ordenada": fila_ordenada,
        "veiculos": veiculos,
        "catalogo_veiculos": catalogo_veiculos,
        "coluna_tipo_veiculo": coluna_tipo_veiculo,
    }


# ============================================================
# BLOCO 2 - MONTAGEM DE GRUPOS
# ============================================================

def _montar_grupos_clientes(fila_ordenada: pd.DataFrame) -> List[Tuple[str, pd.DataFrame]]:
    return _agrupar_clientes_ordenados(fila_ordenada)


# ============================================================
# BLOCO 3 - DEDICADOS
# ============================================================

def _executar_dedicados(
    ctx: Dict[str, Any],
    grupos_cliente: List[Tuple[str, pd.DataFrame]],
    catalogo_veiculos: pd.DataFrame,
    tipo_roteirizacao: str,
    contadores_m4: Dict[str, Any],
) -> None:
    grupos_dedicados = [
        (cliente, dfc)
        for cliente, dfc in grupos_cliente
        if bool(dfc["veiculo_exclusivo_flag"].any())
    ]
    contadores_m4["qtd_clientes_exclusivos"] = int(len(grupos_dedicados))

    perfis_asc = catalogo_veiculos.sort_values(
        by=["capacidade_peso_kg", "capacidade_vol_m3", "max_entregas", "max_km_distancia"],
        ascending=[True, True, True, True],
    )

    for cliente, df_cliente in grupos_dedicados:
        pool_cliente = _filtrar_nao_alocados(df_cliente, ctx["ids_alocados"])
        if len(pool_cliente) == 0:
            continue

        anchor_id = str(pool_cliente["id_linha_pipeline"].astype(str).iloc[0])
        fechado = False

        for idx, veic in perfis_asc.iterrows():
            if not _veiculo_disponivel_no_modo_frota(veic, tipo_roteirizacao):
                tent = {
                    "etapa_fechamento": "4B1_dedicados",
                    "tipo_tentativa": "cliente_dedicado",
                    "cliente_referencia": cliente,
                    "linha_ancora": anchor_id,
                    "veiculo_tipo": veic["tipo"],
                    "resultado_teste": "rejeitado",
                    "motivo_reprovacao": "perfil_sem_disponibilidade_no_modo_frota",
                }
                ctx["tentativas_fechamento"].append(tent)
                _contabilizar_tentativa(contadores_m4, tent)
                continue

            avaliacao = _avaliar_combo_no_veiculo(
                pool_cliente,
                veic=veic,
                ignorar_ocupacao_minima=True,
                ignorar_raio=True,
            )

            tent = {
                **avaliacao,
                "etapa_fechamento": "4B1_dedicados",
                "tipo_tentativa": "cliente_dedicado",
                "cliente_referencia": cliente,
                "linha_ancora": anchor_id,
                "resultado_teste": "aceito" if avaliacao["aceito"] else "rejeitado",
            }

            if not avaliacao["aceito"]:
                tent["motivo_reprovacao"] = _motivo_reprovacao(
                    avaliacao,
                    exigir_ocupacao=False,
                    exigir_raio=False,
                )

            ctx["tentativas_fechamento"].append(tent)
            _contabilizar_tentativa(contadores_m4, tent)

            if avaliacao["aceito"]:
                _registrar_manifesto(
                    ctx=ctx,
                    catalogo_veiculos=catalogo_veiculos,
                    tipo_roteirizacao=tipo_roteirizacao,
                    df_combo=pool_cliente,
                    avaliacao=avaliacao,
                    origem_etapa="4B1_dedicados",
                    catalogo_idx=idx,
                )
                contadores_m4["qtd_manifestos_exclusivos"] += 1
                fechado = True
                break

        if not fechado:
            continue


# ============================================================
# BLOCO 4 - FILTRO MÍNIMO NÃO DEDICADOS
# ============================================================

def _filtrar_clientes_minimo_nao_dedicado(
    ctx: Dict[str, Any],
    fila_ordenada: pd.DataFrame,
    catalogo_veiculos: pd.DataFrame,
    contadores_m4: Dict[str, Any],
) -> Dict[str, Any]:
    remanescente_pos_dedicados = _filtrar_nao_alocados(fila_ordenada, ctx["ids_alocados"])
    remanescente_nao_exclusivo = remanescente_pos_dedicados.loc[
        remanescente_pos_dedicados["veiculo_exclusivo_flag"] == False
    ].copy().reset_index(drop=True)

    grupos_nao_exclusivos = _agrupar_clientes_ordenados(remanescente_nao_exclusivo)
    contadores_m4["qtd_clientes_nao_exclusivos"] = int(len(grupos_nao_exclusivos))

    menor_perfil = _obter_menor_perfil_por_capacidade(catalogo_veiculos)
    peso_minimo_menor_perfil = float(menor_perfil["capacidade_peso_kg"]) * OCUPACAO_MINIMA_PADRAO

    grupos_validos: List[Tuple[str, pd.DataFrame]] = []
    grupos_eliminados_peso: List[Tuple[str, pd.DataFrame]] = []

    for cliente, df_cliente in grupos_nao_exclusivos:
        peso_total_cliente = _obter_base_carga_oficial(df_cliente)
        if peso_total_cliente < peso_minimo_menor_perfil:
            grupos_eliminados_peso.append((cliente, df_cliente))
        else:
            grupos_validos.append((cliente, df_cliente))

    contadores_m4["qtd_clientes_eliminados_peso_minimo"] = int(len(grupos_eliminados_peso))

    for cliente, df_cliente in grupos_eliminados_peso:
        tent = {
            "etapa_fechamento": "4B2_filtro_peso_minimo",
            "tipo_tentativa": "cliente_nao_exclusivo",
            "cliente_referencia": cliente,
            "linha_ancora": str(df_cliente["id_linha_pipeline"].astype(str).iloc[0]),
            "resultado_teste": "rejeitado",
            "motivo_reprovacao": "abaixo_ocupacao_minima_do_menor_perfil",
        }
        ctx["tentativas_fechamento"].append(tent)
        _contabilizar_tentativa(contadores_m4, tent)

    return {
        "grupos_validos": grupos_validos,
        "grupos_eliminados_peso": grupos_eliminados_peso,
        "remanescente_pos_dedicados": remanescente_pos_dedicados,
        "remanescente_nao_exclusivo": remanescente_nao_exclusivo,
    }


# ============================================================
# BLOCO 5 - NÃO DEDICADOS
# ============================================================

def _executar_nao_dedicados(
    ctx: Dict[str, Any],
    grupos_validos: List[Tuple[str, pd.DataFrame]],
    catalogo_veiculos: pd.DataFrame,
    tipo_roteirizacao: str,
    contadores_m4: Dict[str, Any],
) -> None:
    for cliente, df_cliente in grupos_validos:
        pool_cliente = _filtrar_nao_alocados(df_cliente, ctx["ids_alocados"])
        if len(pool_cliente) == 0:
            continue

        anchor_id = str(pool_cliente["id_linha_pipeline"].astype(str).iloc[0])
        km_cliente = _calcular_km_referencia(pool_cliente)

        perfis_compativeis = _perfis_compativeis_por_raio(
            catalogo_veiculos=catalogo_veiculos,
            km_referencia=km_cliente,
            tipo_roteirizacao=tipo_roteirizacao,
        )

        if len(perfis_compativeis) == 0:
            contadores_m4["qtd_clientes_sem_perfil_por_raio"] += 1
            tent = {
                "etapa_fechamento": "4C_nao_exclusivos",
                "tipo_tentativa": "cliente_nao_exclusivo",
                "cliente_referencia": cliente,
                "linha_ancora": anchor_id,
                "resultado_teste": "rejeitado",
                "motivo_reprovacao": "sem_perfil_compativel_por_raio",
            }
            ctx["tentativas_fechamento"].append(tent)
            _contabilizar_tentativa(contadores_m4, tent)
            continue

        fechado = False

        for _, row_veic in perfis_compativeis.iterrows():
            idx_original = int(row_veic["index"])
            veic = catalogo_veiculos.loc[idx_original]

            if not _veiculo_disponivel_no_modo_frota(veic, tipo_roteirizacao):
                tent = {
                    "etapa_fechamento": "4C_nao_exclusivos",
                    "tipo_tentativa": "cliente_nao_exclusivo",
                    "cliente_referencia": cliente,
                    "linha_ancora": anchor_id,
                    "veiculo_tipo": veic["tipo"],
                    "resultado_teste": "rejeitado",
                    "motivo_reprovacao": "perfil_sem_disponibilidade_no_modo_frota",
                }
                ctx["tentativas_fechamento"].append(tent)
                _contabilizar_tentativa(contadores_m4, tent)
                continue

            avaliacao = _avaliar_combo_no_veiculo(
                pool_cliente,
                veic=veic,
                ignorar_ocupacao_minima=False,
                ignorar_raio=False,
            )

            tent = {
                **avaliacao,
                "etapa_fechamento": "4C_nao_exclusivos",
                "tipo_tentativa": "cliente_nao_exclusivo",
                "cliente_referencia": cliente,
                "linha_ancora": anchor_id,
                "resultado_teste": "aceito" if avaliacao["aceito"] else "rejeitado",
            }

            if not avaliacao["aceito"]:
                tent["motivo_reprovacao"] = _motivo_reprovacao(
                    avaliacao,
                    exigir_ocupacao=True,
                    exigir_raio=True,
                )

            ctx["tentativas_fechamento"].append(tent)
            _contabilizar_tentativa(contadores_m4, tent)

            if avaliacao["aceito"]:
                _registrar_manifesto(
                    ctx=ctx,
                    catalogo_veiculos=catalogo_veiculos,
                    tipo_roteirizacao=tipo_roteirizacao,
                    df_combo=pool_cliente,
                    avaliacao=avaliacao,
                    origem_etapa="4C_nao_exclusivos",
                    catalogo_idx=idx_original,
                )
                contadores_m4["qtd_manifestos_nao_exclusivos"] += 1
                fechado = True
                break

        if not fechado:
            continue


# ============================================================
# BLOCO 6 - OUTPUTS / REMANESCENTE
# ============================================================

def _motivo_final_remanescente(
    id_linha: str,
    cliente: str,
    df_tentativas: pd.DataFrame,
) -> str:
    if df_tentativas is None or df_tentativas.empty:
        return "sem_tentativa_registrada"

    base = df_tentativas.copy()

    if "linha_ancora" in base.columns:
        base = base.loc[base["linha_ancora"].astype(str) == str(id_linha)].copy()

    if base.empty and "cliente_referencia" in df_tentativas.columns:
        base = df_tentativas.loc[df_tentativas["cliente_referencia"].astype(str) == str(cliente)].copy()

    if base.empty:
        return "sem_tentativa_registrada"

    if "resultado_teste" in base.columns:
        base_rej = base.loc[base["resultado_teste"].astype(str) == "rejeitado"].copy()
        if not base_rej.empty:
            base = base_rej

    motivos: List[str] = []
    if "motivo_reprovacao" in base.columns:
        motivos = [
            str(x).strip()
            for x in base["motivo_reprovacao"].dropna().astype(str).tolist()
            if str(x).strip() != ""
        ]

    if len(motivos) == 0:
        return "rejeitado_sem_motivo_detalhado"

    freq: Dict[str, int] = {}
    for motivo in motivos:
        freq[motivo] = freq.get(motivo, 0) + 1

    return max(freq.items(), key=lambda kv: kv[1])[0]


def _montar_df_nao_roteirizados_bloco_4(df_remanescente: pd.DataFrame) -> pd.DataFrame:
    if df_remanescente is None or len(df_remanescente) == 0:
        return pd.DataFrame()

    df_out = df_remanescente.copy()

    if "motivo_final_remanescente_m4" not in df_out.columns:
        df_out["motivo_final_remanescente_m4"] = "remanescente_sem_motivo_informado"

    df_out["status_roteirizacao"] = "remanescente_bloco_4"
    df_out["origem_bloco"] = "M4"
    df_out["segue_para_proximo_bloco"] = True
    df_out["motivo_nao_roteirizado"] = df_out["motivo_final_remanescente_m4"]

    colunas_prioritarias = [
        "id_linha_pipeline",
        "cte",
        "romaneio",
        "serie",
        "filial_roteirizacao",
        "filial_origem",
        "destinatario",
        "tomador",
        "ref_cliente",
        "cidade",
        "uf",
        "mesorregiao",
        "subregiao",
        "peso_kg",
        "vol_m3",
        "peso_calculado",
        "distancia_rodoviaria_est_km",
        "qtd_volumes",
        "qtd_pallet",
        "data_agenda",
        "data_leadtime",
        "data_limite_considerada",
        "tipo_data_limite",
        "folga_dias",
        "agendada",
        "prioridade_embarque",
        "prioridade_label",
        "ranking_prioridade_operacional",
        "restricao_veiculo",
        "veiculo_exclusivo",
        "veiculo_exclusivo_flag",
        "motivo_triagem",
        "motivo_final_remanescente_m4",
        "motivo_nao_roteirizado",
        "status_roteirizacao",
        "origem_bloco",
        "segue_para_proximo_bloco",
    ]

    colunas_existentes = [c for c in colunas_prioritarias if c in df_out.columns]
    demais_colunas = [c for c in df_out.columns if c not in colunas_existentes]

    return df_out[colunas_existentes + demais_colunas].copy()


def _montar_outputs_m4(
    ctx: Dict[str, Any],
    fila: pd.DataFrame,
    catalogo_veiculos: pd.DataFrame,
) -> Dict[str, pd.DataFrame]:
    df_manifestos_fechados_bloco_4 = pd.DataFrame(ctx["manifestos_fechados"])

    df_itens_manifestos_fechados_bloco_4 = (
        pd.concat(ctx["itens_manifestos_fechados"], ignore_index=True)
        if len(ctx["itens_manifestos_fechados"]) > 0
        else pd.DataFrame()
    )

    df_tentativas_fechamento_bloco_4 = pd.DataFrame(ctx["tentativas_fechamento"])

    df_remanescente_roteirizavel_bloco_4 = _filtrar_nao_alocados(fila, ctx["ids_alocados"]).copy().reset_index(drop=True)

    if len(df_itens_manifestos_fechados_bloco_4) > 0:
        if df_itens_manifestos_fechados_bloco_4["id_linha_pipeline"].astype(str).duplicated().any():
            raise Exception("Validação pós-M4 falhou: id_linha_pipeline repetido em mais de um item manifesto.")

        if len(df_manifestos_fechados_bloco_4) > 0:
            base_invalidos = df_manifestos_fechados_bloco_4.loc[
                (~df_manifestos_fechados_bloco_4["ignorar_ocupacao_minima"])
                & (
                    (pd.to_numeric(df_manifestos_fechados_bloco_4["ocupacao_oficial_perc"], errors="coerce") < 70)
                    | (pd.to_numeric(df_manifestos_fechados_bloco_4["ocupacao_oficial_perc"], errors="coerce") >= 100)
                )
            ].copy()
            if len(base_invalidos) > 0:
                raise Exception(
                    "Validação pós-M4 falhou: manifesto não exclusivo com ocupação fora de 70% <= x < 100%."
                )

    if len(df_remanescente_roteirizavel_bloco_4) > 0:
        df_remanescente_roteirizavel_bloco_4["motivo_final_remanescente_m4"] = df_remanescente_roteirizavel_bloco_4.apply(
            lambda row: _motivo_final_remanescente(
                id_linha=str(row["id_linha_pipeline"]),
                cliente=str(row["destinatario"]),
                df_tentativas=df_tentativas_fechamento_bloco_4,
            ),
            axis=1,
        )

    df_nao_roteirizados_bloco_4 = _montar_df_nao_roteirizados_bloco_4(
        df_remanescente=df_remanescente_roteirizavel_bloco_4
    )

    uso_frota = catalogo_veiculos[["tipo", "limite_manifestos", "manifestos_utilizados"]].copy()
    uso_frota["saldo_manifestos"] = uso_frota.apply(
        lambda row: (
            np.nan
            if pd.isna(row["limite_manifestos"])
            else int(row["limite_manifestos"]) - int(_int_safe(row["manifestos_utilizados"], default=0))
        ),
        axis=1,
    )

    return {
        "df_manifestos_fechados_bloco_4": df_manifestos_fechados_bloco_4,
        "df_itens_manifestos_fechados_bloco_4": df_itens_manifestos_fechados_bloco_4,
        "df_tentativas_fechamento_bloco_4": df_tentativas_fechamento_bloco_4,
        "df_remanescente_roteirizavel_bloco_4": df_remanescente_roteirizavel_bloco_4,
        "df_nao_roteirizados_bloco_4": df_nao_roteirizados_bloco_4,
        "df_uso_frota_m4": uso_frota,
    }


# ============================================================
# BLOCO 7 - PERSISTÊNCIA OPCIONAL
# ============================================================

def _persistir_artefatos_m4(
    outputs: Dict[str, pd.DataFrame],
    rodada_id: str,
    data_base_roteirizacao: pd.Timestamp,
    tipo_roteirizacao: str,
    caminhos_pipeline: Dict[str, Any],
    contadores_m4: Dict[str, Any],
    tempos_m4: Dict[str, Any],
) -> None:
    pasta_saida_base_str = caminhos_pipeline.get("pasta_saida_base")
    if pasta_saida_base_str:
        pasta_saida_base = Path(pasta_saida_base_str)
    else:
        pasta_saida_base = Path("/tmp/rec_roteirizador") / str(rodada_id)

    pasta_modulo_4 = pasta_saida_base / "bloco_4_manifestos_fechados"
    pasta_modulo_4.mkdir(parents=True, exist_ok=True)

    df_manifestos = outputs["df_manifestos_fechados_bloco_4"]
    df_itens = outputs["df_itens_manifestos_fechados_bloco_4"]
    df_tentativas = outputs["df_tentativas_fechamento_bloco_4"]
    df_remanescente = outputs["df_remanescente_roteirizavel_bloco_4"]
    df_nao_roteirizados = outputs["df_nao_roteirizados_bloco_4"]
    uso_frota = outputs["df_uso_frota_m4"]

    arq_manifestos_xlsx = pasta_modulo_4 / "df_manifestos_fechados_bloco_4.xlsx"
    arq_itens_csv = pasta_modulo_4 / "df_itens_manifestos_fechados_bloco_4.csv"
    arq_tentativas_csv = pasta_modulo_4 / "df_tentativas_fechamento_bloco_4.csv"
    arq_remanescente_csv = pasta_modulo_4 / "df_remanescente_roteirizavel_bloco_4.csv"
    arq_nao_roteirizados_csv = pasta_modulo_4 / "df_nao_roteirizados_bloco_4.csv"
    arq_resumo_xlsx = pasta_modulo_4 / "resumo_modulo_4.xlsx"
    arq_metadata_json = pasta_modulo_4 / "metadata_modulo_4.json"

    if len(df_manifestos) > 0:
        df_manifestos.to_excel(arq_manifestos_xlsx, index=False)

    if len(df_itens) > 0:
        df_itens.to_csv(arq_itens_csv, index=False, encoding="utf-8-sig")

    if len(df_tentativas) > 0:
        df_tentativas.to_csv(arq_tentativas_csv, index=False, encoding="utf-8-sig")

    if len(df_remanescente) > 0:
        df_remanescente.to_csv(arq_remanescente_csv, index=False, encoding="utf-8-sig")

    if len(df_nao_roteirizados) > 0:
        df_nao_roteirizados.to_csv(arq_nao_roteirizados_csv, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(arq_resumo_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {
                    "roteirizavel_entrada_m4": int(len(df_remanescente) + len(df_itens)),
                    "manifestos_fechados_gerados_m4": int(len(df_manifestos)),
                    "itens_manifestados_m4": int(len(df_itens)),
                    "remanescente_roteirizavel_m4": int(len(df_remanescente)),
                    "nao_roteirizados_bloco_4": int(len(df_nao_roteirizados)),
                    "tipo_roteirizacao": tipo_roteirizacao,
                }
            ]
        ).to_excel(writer, sheet_name="resumo", index=False)

        if len(uso_frota) > 0:
            uso_frota.to_excel(writer, sheet_name="uso_frota", index=False)

    metadata = {
        "modulo": "4_manifestos_fechados",
        "data_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_base_projeto": pd.Timestamp(data_base_roteirizacao).strftime("%Y-%m-%d"),
        "tipo_roteirizacao": tipo_roteirizacao,
        "regras": {
            "peso_calculado_base_oficial": True,
            "dedicado_primeiro": True,
            "dedicado_ignora_ocupacao_minima": True,
            "dedicado_ignora_raio": True,
            "nao_exclusivo_mesmo_cliente_sem_mistura": True,
            "nao_exclusivo_respeita_ocupacao_70_a_menor_que_100": True,
            "nao_exclusivo_respeita_raio": True,
            "remanescente_segue_para_proximo_bloco": True,
        },
        "contadores_m4": contadores_m4,
        "tempos_m4": tempos_m4,
        "auditoria_m4": {
            "motivos_remanescente_m4": (
                df_remanescente["motivo_final_remanescente_m4"].value_counts(dropna=False).to_dict()
                if "motivo_final_remanescente_m4" in df_remanescente.columns
                else {}
            )
        },
    }

    with open(arq_metadata_json, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4, default=str)

    caminhos_pipeline["df_manifestos_fechados_bloco_4_xlsx"] = str(arq_manifestos_xlsx)
    caminhos_pipeline["df_itens_manifestos_fechados_bloco_4_csv"] = str(arq_itens_csv)
    caminhos_pipeline["df_tentativas_fechamento_bloco_4_csv"] = str(arq_tentativas_csv)
    caminhos_pipeline["df_remanescente_roteirizavel_bloco_4_csv"] = str(arq_remanescente_csv)
    caminhos_pipeline["df_nao_roteirizados_bloco_4_csv"] = str(arq_nao_roteirizados_csv)
    caminhos_pipeline["resumo_modulo_4_xlsx"] = str(arq_resumo_xlsx)
    caminhos_pipeline["metadata_modulo_4_json"] = str(arq_metadata_json)


# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================

def executar_m4_manifestos_fechados(
    df_input_oficial_bloco_4: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
    rodada_id: str,
    data_base_roteirizacao: pd.Timestamp,
    tipo_roteirizacao: str = "carteira",
    configuracao_frota: Any = None,
    caminhos_pipeline: Dict[str, Any] | None = None,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    inicio_total = _agora()

    tempos_m4: Dict[str, float] = {}
    contadores_m4: Dict[str, Any] = {
        "qtd_clientes_input": 0,
        "qtd_clientes_exclusivos": 0,
        "qtd_clientes_nao_exclusivos": 0,
        "qtd_tentativas_total": 0,
        "qtd_tentativas_rejeitadas_ocupacao": 0,
        "qtd_tentativas_rejeitadas_km": 0,
        "qtd_tentativas_rejeitadas_paradas": 0,
        "qtd_tentativas_rejeitadas_capacidade": 0,
        "qtd_tentativas_rejeitadas_volume": 0,
        "qtd_tentativas_rejeitadas_restricao_veiculo": 0,
        "qtd_tentativas_sem_disponibilidade_frota": 0,
        "qtd_manifestos_exclusivos": 0,
        "qtd_manifestos_nao_exclusivos": 0,
        "qtd_clientes_eliminados_peso_minimo": 0,
        "qtd_clientes_sem_perfil_por_raio": 0,
    }

    caminhos_pipeline = caminhos_pipeline or {}
    tipo_roteirizacao = _normalizar_tipo_roteirizacao(tipo_roteirizacao)
    persistir_artefatos = bool(caminhos_pipeline.get("persistir_artefatos", False))
    ctx = _inicializar_contexto_execucao()

    # ------------------------------------------------------------
    # BLOCO 1 - PREPARAÇÃO
    # ------------------------------------------------------------
    t0 = _agora()
    preparo = _preparar_input_m4(
        df_input_oficial_bloco_4=df_input_oficial_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
        tipo_roteirizacao=tipo_roteirizacao,
        configuracao_frota=configuracao_frota,
    )
    tempos_m4["preparacao_validacao_ms"] = _duracao_ms(t0)

    fila = preparo["fila"]
    fila_ordenada = preparo["fila_ordenada"]
    catalogo_veiculos = preparo["catalogo_veiculos"]
    coluna_tipo_veiculo = preparo["coluna_tipo_veiculo"]

    # ------------------------------------------------------------
    # BLOCO 2 - GRUPOS
    # ------------------------------------------------------------
    t0 = _agora()
    grupos_cliente = _montar_grupos_clientes(fila_ordenada)
    contadores_m4["qtd_clientes_input"] = int(len(grupos_cliente))
    tempos_m4["montagem_indices_cliente_ms"] = _duracao_ms(t0)

    # ------------------------------------------------------------
    # BLOCO 3 - DEDICADOS
    # ------------------------------------------------------------
    t0 = _agora()
    _executar_dedicados(
        ctx=ctx,
        grupos_cliente=grupos_cliente,
        catalogo_veiculos=catalogo_veiculos,
        tipo_roteirizacao=tipo_roteirizacao,
        contadores_m4=contadores_m4,
    )
    tempos_m4["4B1_dedicados_ms"] = _duracao_ms(t0)

    # ------------------------------------------------------------
    # BLOCO 4 - FILTRO MÍNIMO NÃO DEDICADO
    # ------------------------------------------------------------
    t0 = _agora()
    filtro_nao_dedicado = _filtrar_clientes_minimo_nao_dedicado(
        ctx=ctx,
        fila_ordenada=fila_ordenada,
        catalogo_veiculos=catalogo_veiculos,
        contadores_m4=contadores_m4,
    )
    tempos_m4["4B2_filtro_peso_minimo_ms"] = _duracao_ms(t0)

    grupos_validos = filtro_nao_dedicado["grupos_validos"]

    # ------------------------------------------------------------
    # BLOCO 5 - NÃO DEDICADOS
    # ------------------------------------------------------------
    t0 = _agora()
    _executar_nao_dedicados(
        ctx=ctx,
        grupos_validos=grupos_validos,
        catalogo_veiculos=catalogo_veiculos,
        tipo_roteirizacao=tipo_roteirizacao,
        contadores_m4=contadores_m4,
    )
    tempos_m4["4C_nao_exclusivos_ms"] = _duracao_ms(t0)

    # ------------------------------------------------------------
    # BLOCO 6 - OUTPUTS
    # ------------------------------------------------------------
    t0 = _agora()
    outputs = _montar_outputs_m4(
        ctx=ctx,
        fila=fila,
        catalogo_veiculos=catalogo_veiculos,
    )
    tempos_m4["validacao_pos_m4_ms"] = _duracao_ms(t0)

    df_manifestos_fechados_bloco_4 = outputs["df_manifestos_fechados_bloco_4"]
    df_itens_manifestos_fechados_bloco_4 = outputs["df_itens_manifestos_fechados_bloco_4"]
    df_remanescente_roteirizavel_bloco_4 = outputs["df_remanescente_roteirizavel_bloco_4"]
    df_nao_roteirizados_bloco_4 = outputs["df_nao_roteirizados_bloco_4"]

    roteirizavel_entrada_m4 = len(fila)
    itens_manifestados_m4 = len(df_itens_manifestos_fechados_bloco_4)
    remanescente_roteirizavel_m4 = len(df_remanescente_roteirizavel_bloco_4)

    # ------------------------------------------------------------
    # BLOCO 7 - PERSISTÊNCIA OPCIONAL
    # ------------------------------------------------------------
    t0 = _agora()
    if persistir_artefatos:
        try:
            _persistir_artefatos_m4(
                outputs=outputs,
                rodada_id=rodada_id,
                data_base_roteirizacao=data_base_roteirizacao,
                tipo_roteirizacao=tipo_roteirizacao,
                caminhos_pipeline=caminhos_pipeline,
                contadores_m4=contadores_m4,
                tempos_m4=tempos_m4,
            )
        except Exception:
            pass
    tempos_m4["persistencia_artefatos_ms"] = _duracao_ms(t0)
    tempos_m4["tempo_total_m4_ms"] = _duracao_ms(inicio_total)

    # ------------------------------------------------------------
    # RESUMOS / META
    # ------------------------------------------------------------
    resumo_m4 = {
        "modulo": "M4",
        "data_base_roteirizacao": pd.Timestamp(data_base_roteirizacao).isoformat(),
        "coluna_tipo_veiculo_utilizada": coluna_tipo_veiculo,
        "tipo_roteirizacao": tipo_roteirizacao,
        "roteirizavel_entrada_m4": int(roteirizavel_entrada_m4),
        "manifestos_fechados_gerados_m4": int(len(df_manifestos_fechados_bloco_4)),
        "itens_manifestados_m4": int(itens_manifestados_m4),
        "remanescente_roteirizavel_m4": int(remanescente_roteirizavel_m4),
        "nao_roteirizados_bloco_4": int(len(df_nao_roteirizados_bloco_4)),
        "exclusivos_entrada_m4": int((fila["veiculo_exclusivo_flag"] == True).sum()),
        "prioridade_embarque_1_entrada_m4": int((pd.to_numeric(fila["prioridade_embarque"], errors="coerce") == 1).sum()),
        "ocupacao_minima_padrao_perc": round(OCUPACAO_MINIMA_PADRAO * 100, 2),
        "ocupacao_maxima_padrao_perc": round(OCUPACAO_MAXIMA_PADRAO * 100, 2),
        "persistiu_artefatos": persistir_artefatos,
        "caminhos_pipeline": caminhos_pipeline,
    }

    auditoria_m4 = {
        "motivos_remanescente_m4": (
            df_remanescente_roteirizavel_bloco_4["motivo_final_remanescente_m4"].value_counts(dropna=False).to_dict()
            if "motivo_final_remanescente_m4" in df_remanescente_roteirizavel_bloco_4.columns
            else {}
        )
    }

    meta = {
        "resumo_m4": resumo_m4,
        "auditoria_m4": auditoria_m4,
        "metricas_m4": {
            "contadores_m4": contadores_m4,
            "tempos_m4": tempos_m4,
        },
        "metadata_modulo_4": {
            "tipo_roteirizacao": tipo_roteirizacao,
            "catalogo_veiculos": _to_records(catalogo_veiculos),
            "uso_frota": _to_records(outputs["df_uso_frota_m4"]),
        },
    }

    return outputs, meta
