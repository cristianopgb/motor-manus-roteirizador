from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


OCUPACAO_MINIMA_PADRAO = 0.70
OCUPACAO_MAXIMA_PADRAO = 1.00


# =========================================================================================
# HELPERS BÁSICOS
# =========================================================================================
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    try:
        if pd.isna(value):
            return False
    except Exception:
        pass

    text = str(value).strip().lower()
    return text in {"1", "true", "t", "sim", "s", "yes", "y"}


def safe_text(value: Any) -> str:
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def ensure_column(df: pd.DataFrame, col: str, default: Any) -> None:
    if col not in df.columns:
        df[col] = default


def ensure_columns(df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
    out = df.copy()
    for col, default in defaults.items():
        ensure_column(out, col, default)
    return out


def coalesce_columns(
    df: pd.DataFrame,
    target: str,
    candidates: Sequence[str],
    default: Any = None,
) -> pd.DataFrame:
    out = df.copy()

    if target not in out.columns:
        out[target] = default

    for col in candidates:
        if col in out.columns:
            out[target] = out[target].where(out[target].notna(), out[col])

    if target not in out.columns:
        out[target] = default

    return out


def drop_helper_columns(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df.copy()

    cols_to_drop = [c for c in df.columns if c.startswith(prefix)]
    if not cols_to_drop:
        return df.copy()

    return df.drop(columns=cols_to_drop, errors="ignore").copy()


# =========================================================================================
# CONTRATO INTERNO DO M5
# =========================================================================================
def validate_required_columns(
    df: pd.DataFrame,
    required_columns: Sequence[str],
    etapa: str,
) -> None:
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"{etapa}: faltam colunas obrigatórias no contrato interno do M5:\n- "
            + "\n- ".join(missing)
        )


def validate_non_null_columns(
    df: pd.DataFrame,
    non_null_columns: Sequence[str],
    etapa: str,
) -> None:
    problems: List[str] = []
    for col in non_null_columns:
        if col not in df.columns:
            problems.append(f"{col} (coluna inexistente)")
            continue

        qtd_null = int(df[col].isna().sum())
        if qtd_null > 0:
            problems.append(f"{col} com {qtd_null} nulos")

    if problems:
        raise ValueError(
            f"{etapa}: o contrato interno do M5 falhou em colunas obrigatórias:\n- "
            + "\n- ".join(problems)
        )


def validate_peso_oficial_m5(df: pd.DataFrame, etapa: str) -> None:
    """
    Regra oficial do M5:
    - peso_calculado é obrigatório
    - não pode ser recriado a partir de peso_c dentro do M5
    - peso_kg é auditoria
    - vol_m3 é volume
    """
    if "peso_calculado" not in df.columns:
        raise ValueError(
            f"{etapa}: peso_calculado é obrigatório no contrato interno do M5."
        )

    qtd_null = int(pd.to_numeric(df["peso_calculado"], errors="coerce").isna().sum())
    if qtd_null > 0:
        raise ValueError(
            f"{etapa}: peso_calculado contém {qtd_null} nulos. "
            "O M5 não deve recalcular nem inferir peso a partir de peso_c."
        )


# =========================================================================================
# NORMALIZAÇÃO BASE DO SALDO DO M5
# =========================================================================================
def normalize_saldo_m5(
    df_input: pd.DataFrame,
    etapa: str,
    require_geo: bool = False,
    require_subregiao: bool = False,
    require_mesorregiao: bool = False,
    extra_defaults: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Normaliza qualquer saldo que entra em uma etapa do M5.
    Não recria peso_calculado com peso_c.
    """
    saldo = df_input.copy() if df_input is not None else pd.DataFrame()

    if saldo.empty:
        return saldo

    rename_map: Dict[str, str] = {}
    if "sub_regiao" in saldo.columns and "subregiao" not in saldo.columns:
        rename_map["sub_regiao"] = "subregiao"
    if "mesoregiao" in saldo.columns and "mesorregiao" not in saldo.columns:
        rename_map["mesoregiao"] = "mesorregiao"

    if rename_map:
        saldo = saldo.rename(columns=rename_map)

    defaults = {
        "id_linha_pipeline": None,
        "cte": pd.NA,
        "nro_documento": pd.NA,
        "cidade": "",
        "uf": "",
        "subregiao": "",
        "mesorregiao": "",
        "destinatario": "",
        "tomador": "",
        "peso_calculado": np.nan,
        "peso_kg": np.nan,
        "vol_m3": 0.0,
        "distancia_rodoviaria_est_km": 0.0,
        "restricao_veiculo": None,
        "agendada": False,
        "folga_dias": 999,
        "prioridade_embarque_num": pd.NA,
        "prioridade_embarque": pd.NA,
        "ranking_prioridade_operacional": pd.NA,
        "veiculo_exclusivo": False,
        "veiculo_exclusivo_flag": False,
        "latitude_destinatario": pd.NA,
        "longitude_destinatario": pd.NA,
        "origem_latitude": pd.NA,
        "origem_longitude": pd.NA,
    }

    if extra_defaults:
        defaults.update(extra_defaults)

    saldo = ensure_columns(saldo, defaults)

    # Compatibilidade de aliases, sem inventar peso operacional.
    saldo = coalesce_columns(
        saldo,
        "distancia_rodoviaria_est_km",
        ["distancia_rodoviaria_est_km", "distancia_km", "km_referencia", "km_rota_referencia"],
        default=0.0,
    )
    saldo = coalesce_columns(
        saldo,
        "peso_kg",
        ["peso_kg", "Peso", "peso"],
        default=np.nan,
    )
    saldo = coalesce_columns(
        saldo,
        "vol_m3",
        ["vol_m3", "Peso C", "Peso Cub.", "peso_c", "cubagem_m3"],
        default=0.0,
    )

    # NÃO FAZER:
    # saldo["peso_calculado"] = saldo["peso_c"]
    # O M5 não recalcula peso oficial.
    if "peso_calculado" not in saldo.columns:
        saldo["peso_calculado"] = np.nan

    numeric_cols = [
        "peso_calculado",
        "peso_kg",
        "vol_m3",
        "distancia_rodoviaria_est_km",
        "folga_dias",
        "prioridade_embarque_num",
        "prioridade_embarque",
        "ranking_prioridade_operacional",
        "latitude_destinatario",
        "longitude_destinatario",
        "origem_latitude",
        "origem_longitude",
    ]
    for col in numeric_cols:
        if col in saldo.columns:
            saldo[col] = pd.to_numeric(saldo[col], errors="coerce")

    bool_cols = ["agendada", "veiculo_exclusivo", "veiculo_exclusivo_flag"]
    for col in bool_cols:
        if col in saldo.columns:
            saldo[col] = saldo[col].apply(safe_bool)

    text_cols = ["cidade", "uf", "subregiao", "mesorregiao", "destinatario", "tomador"]
    for col in text_cols:
        if col in saldo.columns:
            saldo[col] = saldo[col].fillna("").astype(str).str.strip()

    required_cols = ["id_linha_pipeline", "peso_calculado", "peso_kg", "vol_m3", "distancia_rodoviaria_est_km"]
    if require_geo:
        required_cols.extend(["cidade", "uf", "destinatario"])
    if require_subregiao:
        required_cols.append("subregiao")
    if require_mesorregiao:
        required_cols.append("mesorregiao")

    validate_required_columns(saldo, required_cols, etapa=etapa)
    validate_non_null_columns(saldo, ["id_linha_pipeline"], etapa=etapa)
    validate_peso_oficial_m5(saldo, etapa=etapa)

    if saldo["id_linha_pipeline"].astype(str).duplicated().any():
        qtd_dup = int(saldo["id_linha_pipeline"].astype(str).duplicated().sum())
        raise ValueError(
            f"{etapa}: id_linha_pipeline duplicado no saldo do M5: {qtd_dup}"
        )

    return saldo.reset_index(drop=True).copy()


# =========================================================================================
# NORMALIZAÇÃO DE VEÍCULOS / PERFIS
# =========================================================================================
def normalize_veiculos_m5(
    df_veiculos: pd.DataFrame,
    etapa: str,
) -> pd.DataFrame:
    veic = df_veiculos.copy() if df_veiculos is not None else pd.DataFrame()

    if veic.empty:
        raise ValueError(f"{etapa}: base de veículos/perfis é obrigatória.")

    if "tipo" not in veic.columns and "perfil" in veic.columns:
        veic["tipo"] = veic["perfil"]
    if "perfil" not in veic.columns and "tipo" in veic.columns:
        veic["perfil"] = veic["tipo"]

    defaults = {
        "perfil": "",
        "tipo": "",
        "capacidade_peso_kg": pd.NA,
        "capacidade_vol_m3": pd.NA,
        "max_entregas": pd.NA,
        "max_km_distancia": pd.NA,
        "ocupacao_minima_perc": pd.NA,
        "ocupacao_maxima_perc": pd.NA,
    }
    veic = ensure_columns(veic, defaults)

    num_cols = [
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]
    for col in num_cols:
        veic[col] = pd.to_numeric(veic[col], errors="coerce")

    for col in ["perfil", "tipo"]:
        veic[col] = veic[col].fillna("").astype(str).str.strip()

    veic["ocupacao_minima_perc"] = veic["ocupacao_minima_perc"].where(
        veic["ocupacao_minima_perc"].notna(),
        OCUPACAO_MINIMA_PADRAO * 100,
    )
    veic["ocupacao_maxima_perc"] = veic["ocupacao_maxima_perc"].where(
        veic["ocupacao_maxima_perc"].notna(),
        OCUPACAO_MAXIMA_PADRAO * 100,
    )

    required_cols = [
        "perfil",
        "tipo",
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]
    validate_required_columns(veic, required_cols, etapa=etapa)

    veic = (
        veic[required_cols]
        .drop_duplicates()
        .reset_index(drop=True)
        .copy()
    )

    return veic


# =========================================================================================
# ORDENAÇÃO OPERACIONAL PADRÃO DO M5
# =========================================================================================
def fase_bucket(row: pd.Series) -> int:
    prioridade_embarque = pd.to_numeric(
        row.get("prioridade_embarque_num", row.get("prioridade_embarque", pd.NA)),
        errors="coerce",
    )
    agendada = safe_bool(row.get("agendada"))
    folga = safe_float(row.get("folga_dias"), 999)

    if not pd.isna(prioridade_embarque) and safe_float(prioridade_embarque, 0) > 0:
        return 0
    if agendada and folga == 0:
        return 1
    if agendada and folga == 1:
        return 2
    if (not agendada) and folga == 0:
        return 3
    if (not agendada) and folga == 1:
        return 4
    return 99


def precalcular_ordenacao_m5(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    temp = df.copy()
    temp[f"_id_str_{suffix}"] = temp["id_linha_pipeline"].astype(str)
    temp[f"_cidade_key_{suffix}"] = temp["cidade"].fillna("").astype(str).str.strip()
    temp[f"_uf_key_{suffix}"] = temp["uf"].fillna("").astype(str).str.strip()
    temp[f"_cliente_key_{suffix}"] = temp["destinatario"].fillna("").astype(str).str.strip()

    if "subregiao" in temp.columns:
        temp[f"_subregiao_key_{suffix}"] = temp["subregiao"].fillna("").astype(str).str.strip()
    if "mesorregiao" in temp.columns:
        temp[f"_mesorregiao_key_{suffix}"] = temp["mesorregiao"].fillna("").astype(str).str.strip()

    folga = pd.to_numeric(temp["folga_dias"], errors="coerce").fillna(999)
    ranking = pd.to_numeric(temp["ranking_prioridade_operacional"], errors="coerce").fillna(999)
    km = pd.to_numeric(temp["distancia_rodoviaria_est_km"], errors="coerce").fillna(999999)
    peso = pd.to_numeric(temp["peso_calculado"], errors="coerce").fillna(0.0)

    buckets: List[int] = []
    prioridade_ord: List[float] = []

    for _, row in temp.iterrows():
        buckets.append(fase_bucket(row))
        prioridade = pd.to_numeric(
            row.get("prioridade_embarque_num", row.get("prioridade_embarque", pd.NA)),
            errors="coerce",
        )
        prioridade_ord.append(safe_float(prioridade, 999.0) if not pd.isna(prioridade) else 999.0)

    temp[f"_bucket_{suffix}"] = buckets
    temp[f"_prioridade_ord_{suffix}"] = prioridade_ord
    temp[f"_folga_ord_{suffix}"] = folga
    temp[f"_ranking_ord_{suffix}"] = ranking
    temp[f"_km_ord_{suffix}"] = km
    temp[f"_peso_ord_{suffix}"] = -peso

    return temp


def ordenar_operacional_m5(df: pd.DataFrame, suffix: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    bucket_col = f"_bucket_{suffix}"
    if bucket_col not in df.columns:
        df = precalcular_ordenacao_m5(df, suffix=suffix)

    return (
        df.sort_values(
            by=[
                f"_bucket_{suffix}",
                f"_prioridade_ord_{suffix}",
                f"_folga_ord_{suffix}",
                f"_ranking_ord_{suffix}",
                f"_km_ord_{suffix}",
                f"_peso_ord_{suffix}",
                f"_id_str_{suffix}",
            ],
            ascending=[True, True, True, True, True, True, True],
            kind="mergesort",
        )
        .reset_index(drop=True)
        .copy()
    )


# =========================================================================================
# MÉTRICAS COMUNS
# =========================================================================================
def peso_total(df_itens: pd.DataFrame) -> float:
    if df_itens.empty:
        return 0.0
    return float(pd.to_numeric(df_itens["peso_calculado"], errors="coerce").fillna(0).sum())


def peso_auditoria_total(df_itens: pd.DataFrame) -> float:
    if df_itens.empty:
        return 0.0
    return float(pd.to_numeric(df_itens["peso_kg"], errors="coerce").fillna(0).sum())


def volume_total(df_itens: pd.DataFrame) -> float:
    if df_itens.empty:
        return 0.0
    return float(pd.to_numeric(df_itens["vol_m3"], errors="coerce").fillna(0).sum())


def km_referencia(df_itens: pd.DataFrame) -> float:
    if df_itens.empty:
        return 0.0
    return float(pd.to_numeric(df_itens["distancia_rodoviaria_est_km"], errors="coerce").fillna(0).max())


def qtd_paradas(df_itens: pd.DataFrame) -> int:
    if df_itens.empty:
        return 0
    return int(df_itens["destinatario"].fillna("").astype(str).nunique())


def qtd_clientes(df_itens: pd.DataFrame) -> int:
    if df_itens.empty:
        return 0
    return int(df_itens["destinatario"].fillna("").astype(str).nunique())


def qtd_cidades(df_itens: pd.DataFrame) -> int:
    if df_itens.empty:
        return 0
    return int(df_itens["cidade"].fillna("").astype(str).nunique())


def ocupacao_minima_kg(vehicle_row: pd.Series) -> float:
    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocup_min = safe_float(vehicle_row.get("ocupacao_minima_perc"), OCUPACAO_MINIMA_PADRAO * 100)
    return cap_peso * (ocup_min / 100.0)


def ocupacao_maxima_kg(vehicle_row: pd.Series) -> float:
    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    ocup_max = safe_float(vehicle_row.get("ocupacao_maxima_perc"), OCUPACAO_MAXIMA_PADRAO * 100)
    if ocup_max <= 0:
        ocup_max = OCUPACAO_MAXIMA_PADRAO * 100
    return cap_peso * (ocup_max / 100.0)


def ocupacao_perc(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> float:
    cap_peso = safe_float(vehicle_row.get("capacidade_peso_kg"), 0.0)
    if cap_peso <= 0:
        return 0.0
    return (peso_total(df_itens) / cap_peso) * 100.0


# =========================================================================================
# ORDENAÇÃO DE VEÍCULOS
# =========================================================================================
def veiculos_menor_para_maior(df_veiculos: pd.DataFrame) -> pd.DataFrame:
    temp = df_veiculos.copy()
    temp["_cap_peso_tmp"] = pd.to_numeric(temp["capacidade_peso_kg"], errors="coerce").fillna(0)
    temp["_cap_vol_tmp"] = pd.to_numeric(temp["capacidade_vol_m3"], errors="coerce").fillna(0)

    return (
        temp.sort_values(
            ["_cap_peso_tmp", "_cap_vol_tmp", "tipo", "perfil"],
            ascending=[True, True, True, True],
            kind="mergesort",
        )
        .drop(columns=["_cap_peso_tmp", "_cap_vol_tmp"], errors="ignore")
        .reset_index(drop=True)
        .copy()
    )


def veiculos_maior_para_menor(df_veiculos: pd.DataFrame) -> pd.DataFrame:
    temp = df_veiculos.copy()
    temp["_cap_peso_tmp"] = pd.to_numeric(temp["capacidade_peso_kg"], errors="coerce").fillna(0)
    temp["_cap_vol_tmp"] = pd.to_numeric(temp["capacidade_vol_m3"], errors="coerce").fillna(0)

    return (
        temp.sort_values(
            ["_cap_peso_tmp", "_cap_vol_tmp", "tipo", "perfil"],
            ascending=[False, False, True, True],
            kind="mergesort",
        )
        .drop(columns=["_cap_peso_tmp", "_cap_vol_tmp"], errors="ignore")
        .reset_index(drop=True)
        .copy()
    )


def menor_perfil_cadastrado(df_veiculos: pd.DataFrame) -> pd.Series:
    ordenado = veiculos_menor_para_maior(df_veiculos)
    if ordenado.empty:
        raise ValueError("Não há perfis de veículo cadastrados.")
    return ordenado.iloc[0].copy()


# =========================================================================================
# RESTRIÇÃO DE VEÍCULO
# REGRA VALIDADA COM VOCÊS: o veículo marcado é o permitido / exigido
# =========================================================================================
def normalizar_token_restricao(x: Any) -> str:
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


def expandir_alias_restricao(token: str) -> set[str]:
    token = normalizar_token_restricao(token)
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


def tokens_restricao_valor(valor: Any) -> set[str]:
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
        token = normalizar_token_restricao(parte)
        if token != "":
            tokens |= expandir_alias_restricao(token)

    return tokens


def veiculo_compativel_com_restricao(veiculo_tipo: Any, restricao_valor: Any) -> bool:
    tokens_restricao = tokens_restricao_valor(restricao_valor)
    if len(tokens_restricao) == 0:
        return True

    tokens_veiculo = expandir_alias_restricao(veiculo_tipo)
    return len(tokens_restricao & tokens_veiculo) > 0


def grupo_respeita_restricao_veiculo(df_itens: pd.DataFrame, vehicle_row: pd.Series) -> bool:
    if "restricao_veiculo" not in df_itens.columns:
        return True

    tipo = vehicle_row.get("tipo")
    restricoes = df_itens["restricao_veiculo"].tolist()

    for restricao in restricoes:
        if not veiculo_compativel_com_restricao(tipo, restricao):
            return False

    return True


# =========================================================================================
# CHAVES / AGRUPAMENTOS
# =========================================================================================
def cliente_key(value: Any) -> str:
    return safe_text(value).upper()


def cidade_key(cidade: Any, uf: Any) -> str:
    return f"{safe_text(cidade).upper()}|{safe_text(uf).upper()}"


def subregiao_key(subregiao: Any, uf: Any) -> str:
    return f"{safe_text(subregiao).upper()}|{safe_text(uf).upper()}"


def agrupar_saldo_por_cliente(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby(["cidade", "uf", "destinatario"], dropna=False, sort=False)
        .agg(
            peso_total_cliente=("peso_calculado", "sum"),
            peso_kg_total_cliente=("peso_kg", "sum"),
            volume_total_cliente=("vol_m3", "sum"),
            km_referencia_cliente=("distancia_rodoviaria_est_km", "max"),
            qtd_linhas_cliente=("id_linha_pipeline", "count"),
        )
        .reset_index()
    )

    return out.sort_values(
        by=["peso_total_cliente", "cidade", "uf", "destinatario"],
        ascending=[False, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def agrupar_saldo_por_cidade(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby(["cidade", "uf"], dropna=False, sort=False)
        .agg(
            peso_total_cidade=("peso_calculado", "sum"),
            peso_kg_total_cidade=("peso_kg", "sum"),
            volume_total_cidade=("vol_m3", "sum"),
            km_referencia_cidade=("distancia_rodoviaria_est_km", "max"),
            qtd_linhas_cidade=("id_linha_pipeline", "count"),
            qtd_clientes_cidade=("destinatario", "nunique"),
        )
        .reset_index()
    )

    return out.sort_values(
        by=["peso_total_cidade", "cidade", "uf"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def agrupar_saldo_por_subregiao(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    out = (
        df.groupby(["subregiao", "uf"], dropna=False, sort=False)
        .agg(
            peso_total_subregiao=("peso_calculado", "sum"),
            peso_kg_total_subregiao=("peso_kg", "sum"),
            volume_total_subregiao=("vol_m3", "sum"),
            km_referencia_subregiao=("distancia_rodoviaria_est_km", "max"),
            qtd_linhas_subregiao=("id_linha_pipeline", "count"),
            qtd_clientes_subregiao=("destinatario", "nunique"),
            qtd_cidades_subregiao=("cidade", "nunique"),
        )
        .reset_index()
    )

    return out.sort_values(
        by=["peso_total_subregiao", "subregiao", "uf"],
        ascending=[False, True, True],
        kind="mergesort",
    ).reset_index(drop=True)
