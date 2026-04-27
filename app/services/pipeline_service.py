from __future__ import annotations

import time
import unicodedata
import uuid
from typing import Any, Dict, List

import pandas as pd

from app.pipeline.m1_padronizacao import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento
from app.pipeline.m3_triagem import executar_m3_triagem
from app.pipeline.m3_1_validacao_fronteira import executar_m3_1_validacao_fronteira
from app.pipeline.m4_manifestos_fechados import executar_m4_manifestos_fechados
from app.pipeline.m5_1_triagem_cidades import executar_m5_1_triagem_cidades
from app.pipeline.m5_2_composicao_cidades import executar_m5_2_composicao_cidades
from app.pipeline.m5_3_triagem_subregioes import executar_m5_3_triagem_subregioes
from app.pipeline.m5_3_composicao_subregioes import executar_m5_3_composicao_subregioes
from app.pipeline.m5_4a_triagem_mesorregioes import executar_m5_4a_triagem_mesorregioes
from app.pipeline.m5_4b_composicao_mesorregioes import executar_m5_4b_composicao_mesorregioes
from app.pipeline.m6_1_consolidacao_manifestos import executar_m6_1_consolidacao_manifestos
from app.pipeline.m6_2_complemento_ocupacao import COLS_MANIFESTOS_OBRIGATORIAS, executar_m6_2_complemento_ocupacao
from app.pipeline.m7_sequenciamento_entregas import executar_m7_sequenciamento_entregas
from app.schemas import RoteirizacaoRequest
from app.services.auditoria_pipeline_service import persistir_snapshot_modulo_auditoria
from app.services.payload_service import PipelineContext, normalizar_payload_para_pipeline
from app.utils.json_safe import sanitizar_json_safe

PIPELINE_FLAGS = {
    "executar_m4": True,
    "executar_m5_1": True,
    "executar_m5_2": True,
    "executar_m5_3a": True,
    "executar_m5_3b": True,
    "executar_m5_4a": True,
    "executar_m5_4b": True,
    "executar_m6_1": True,
    "executar_m6_2": True,
    "executar_m7": True,
    "executar_validadores_exaustao": True,
    "executar_m4_repescagem": True,
    "executar_m5_2_repescagem": True,
    "executar_m5_3_repescagem": True,
    "executar_m5_4_repescagem": True,
}


MODO_PRODUCAO_CONTRATO_SISTEMA1 = True
PERSISTIR_AUDITORIA_MODULAR_PADRAO = False
RETORNAR_AUDITORIA_INTERNA_PADRAO = False
LOG_VERBOSE_PADRAO = False



def _agora() -> float:
    return time.perf_counter()


def _duracao_ms(inicio: float) -> float:
    return round((time.perf_counter() - inicio) * 1000, 2)


def _safe_len(obj: Any) -> int:
    try:
        return int(len(obj))
    except Exception:
        return 0


def _is_debug(payload: RoteirizacaoRequest) -> bool:
    for attr in ("modo_debug", "debug", "retornar_debug", "incluir_debug"):
        try:
            valor = getattr(payload, attr, False)
            if isinstance(valor, bool):
                return valor
            if isinstance(valor, str):
                return valor.strip().lower() in {"1", "true", "sim", "yes"}
        except Exception:
            continue
    return False


def _log(
    modulo: str,
    status: str,
    mensagem: str,
    quantidade_entrada: int | None = None,
    quantidade_saida: int | None = None,
    tempo_ms: float | None = None,
    extra: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    registro = {
        "modulo": modulo,
        "status": status,
        "mensagem": mensagem,
        "quantidade_entrada": quantidade_entrada,
        "quantidade_saida": quantidade_saida,
    }
    if tempo_ms is not None:
        registro["tempo_ms"] = tempo_ms
    if extra:
        registro["extra"] = extra
    return registro


def _snapshot_dataframe(df: pd.DataFrame, nome: str, max_colunas: int = 30) -> Dict[str, Any]:
    if df is None:
        return {
            "nome": nome,
            "linhas": 0,
            "colunas": [],
            "qtd_colunas_total": 0,
        }

    return {
        "nome": nome,
        "linhas": int(len(df)),
        "colunas": list(df.columns[:max_colunas]),
        "qtd_colunas_total": int(len(df.columns)),
    }


def _serializar_dataframe_para_records(
    df: pd.DataFrame,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []

    df2 = df.copy()

    if limit is not None:
        df2 = df2.head(limit)

    for col in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[col]):
            df2[col] = df2[col].astype(str)

    df2 = df2.where(pd.notnull(df2), None)
    records = df2.to_dict(orient="records")
    records_sanitizados, _ = sanitizar_json_safe(records)
    return records_sanitizados


def _montar_resumo_dataframe(df: pd.DataFrame, nome: str) -> Dict[str, Any]:
    return {
        "nome": nome,
        "total_linhas": _safe_len(df),
        "qtd_colunas": int(len(df.columns)) if isinstance(df, pd.DataFrame) else 0,
    }


def _tem_schema_minimo(df: Any, colunas_obrigatorias: List[str]) -> bool:
    if not isinstance(df, pd.DataFrame):
        return False
    return all(c in df.columns for c in colunas_obrigatorias)


def _copiar_ou_vazio(df: Any, colunas: List[str] | None = None) -> pd.DataFrame:
    if isinstance(df, pd.DataFrame):
        return df.copy()
    return pd.DataFrame(columns=colunas or [])


def _consolidar_remanescente_global(df_a: Any, df_b: Any) -> pd.DataFrame:
    df_a_local = _copiar_ou_vazio(df_a)
    df_b_local = _copiar_ou_vazio(df_b)

    df_consolidado = pd.concat([df_a_local, df_b_local], ignore_index=True, sort=False)
    if df_consolidado.empty:
        return df_consolidado

    for coluna_chave in ("id_linha_pipeline", "chave_linha_dataset"):
        if coluna_chave in df_consolidado.columns:
            return df_consolidado.drop_duplicates(subset=[coluna_chave], keep="first", ignore_index=True)

    return df_consolidado


def _primeira_coluna_existente(df: pd.DataFrame, candidatos: list[str]) -> str | None:
    for c in candidatos:
        if isinstance(df, pd.DataFrame) and c in df.columns:
            return c
    return None


def _coluna_peso_operacional(df: pd.DataFrame) -> str | None:
    return _primeira_coluna_existente(
        df,
        [
            "peso_calculado",
            "peso_final_m6_2",
            "peso_kg",
            "Peso Calculo",
            "Peso C",
            "Peso",
        ],
    )


def _menor_ocupacao_minima_kg(df_veiculos_tratados: pd.DataFrame) -> float:
    if not isinstance(df_veiculos_tratados, pd.DataFrame) or df_veiculos_tratados.empty:
        return 0.0

    col_cap = _primeira_coluna_existente(
        df_veiculos_tratados,
        [
            "capacidade_peso_kg",
            "capacidade_kg",
            "capacidade_peso",
        ],
    )

    col_ocup = _primeira_coluna_existente(
        df_veiculos_tratados,
        [
            "ocupacao_minima_perc",
            "ocupacao_minima",
        ],
    )

    if not col_cap:
        return 0.0

    caps = pd.to_numeric(df_veiculos_tratados[col_cap], errors="coerce")
    caps = caps[caps > 0]

    if caps.empty:
        return 0.0

    menor_capacidade = float(caps.min())

    ocupacao = 70.0
    if col_ocup:
        ocupacoes = pd.to_numeric(df_veiculos_tratados[col_ocup], errors="coerce")
        ocupacoes = ocupacoes[ocupacoes > 0]
        if not ocupacoes.empty:
            ocupacao = float(ocupacoes.min())

    if ocupacao > 1:
        ocupacao = ocupacao / 100.0

    return menor_capacidade * ocupacao


def _filtrar_remanescentes_com_potencial(
    df_remanescente: pd.DataFrame,
    chaves_grupo: list[str],
    df_veiculos_tratados: pd.DataFrame,
    nome_etapa: str,
    limite_grupos: int = 20,
) -> tuple[pd.DataFrame, dict]:
    if not isinstance(df_remanescente, pd.DataFrame) or df_remanescente.empty:
        return pd.DataFrame(), {
            "ativado": False,
            "motivo": "sem_remanescente",
            "grupos_com_potencial": 0,
            "linhas_enviadas_repescagem": 0,
        }

    peso_col = _coluna_peso_operacional(df_remanescente)
    if not peso_col:
        return pd.DataFrame(), {
            "ativado": False,
            "motivo": "sem_coluna_peso",
            "grupos_com_potencial": 0,
            "linhas_enviadas_repescagem": 0,
        }

    chaves_existentes = [c for c in chaves_grupo if c in df_remanescente.columns]
    if not chaves_existentes:
        return pd.DataFrame(), {
            "ativado": False,
            "motivo": "sem_chaves_grupo",
            "grupos_com_potencial": 0,
            "linhas_enviadas_repescagem": 0,
        }

    gatilho_kg = _menor_ocupacao_minima_kg(df_veiculos_tratados)
    if gatilho_kg <= 0:
        return pd.DataFrame(), {
            "ativado": False,
            "motivo": "sem_gatilho_veiculo",
            "grupos_com_potencial": 0,
            "linhas_enviadas_repescagem": 0,
        }

    base = df_remanescente.copy()
    base["_peso_validador"] = pd.to_numeric(base[peso_col], errors="coerce").fillna(0)

    resumo = (
        base.groupby(chaves_existentes, dropna=False)["_peso_validador"]
        .sum()
        .reset_index()
        .rename(columns={"_peso_validador": "_peso_total_grupo"})
    )

    resumo = resumo[resumo["_peso_total_grupo"] >= gatilho_kg]
    resumo = resumo.sort_values("_peso_total_grupo", ascending=False).head(limite_grupos)

    if resumo.empty:
        return pd.DataFrame(), {
            "ativado": False,
            "motivo": "sem_grupo_com_peso_minimo",
            "gatilho_kg": gatilho_kg,
            "grupos_com_potencial": 0,
            "linhas_enviadas_repescagem": 0,
        }

    df_filtrado = base.merge(
        resumo[chaves_existentes],
        on=chaves_existentes,
        how="inner",
    ).drop(columns=["_peso_validador"], errors="ignore")

    meta = {
        "ativado": True,
        "motivo": "grupos_com_potencial",
        "nome_etapa": nome_etapa,
        "gatilho_kg": gatilho_kg,
        "chaves_grupo": chaves_existentes,
        "grupos_com_potencial": int(len(resumo)),
        "linhas_enviadas_repescagem": int(len(df_filtrado)),
        "peso_total_enviado_repescagem": float(pd.to_numeric(df_filtrado[peso_col], errors="coerce").fillna(0).sum()),
    }

    print(f"[VALIDADOR EXAUSTAO] {nome_etapa}", meta)

    return df_filtrado, meta


def _remover_itens_alocados_do_remanescente(
    df_remanescente_original: pd.DataFrame,
    df_itens_alocados: pd.DataFrame,
) -> pd.DataFrame:
    if not isinstance(df_remanescente_original, pd.DataFrame) or df_remanescente_original.empty:
        return pd.DataFrame()
    if not isinstance(df_itens_alocados, pd.DataFrame) or df_itens_alocados.empty:
        return df_remanescente_original.copy()

    chave = _primeira_coluna_existente(
        df_remanescente_original,
        ["id_linha_pipeline", "chave_linha_dataset", "nro_documento", "Nro Doc."],
    )

    if not chave or chave not in df_itens_alocados.columns:
        return df_remanescente_original.copy()

    alocados = set(df_itens_alocados[chave].dropna().astype(str))
    out = df_remanescente_original.copy()
    return out[~out[chave].astype(str).isin(alocados)].copy()


def _renomear_manifestos_repescagem(
    df_manifestos: pd.DataFrame,
    df_itens: pd.DataFrame,
    prefixo: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    manifestos = _copiar_ou_vazio(df_manifestos)
    itens = _copiar_ou_vazio(df_itens)

    if manifestos.empty or "manifesto_id" not in manifestos.columns:
        return manifestos, itens

    ids_ordenados = (
        manifestos["manifesto_id"]
        .dropna()
        .astype(str)
        .drop_duplicates()
        .tolist()
    )
    if not ids_ordenados:
        return manifestos, itens

    mapa_ids = {mid: f"{prefixo}{idx:04d}" for idx, mid in enumerate(ids_ordenados, start=1)}

    manifestos["manifesto_id"] = manifestos["manifesto_id"].astype(str).map(mapa_ids).fillna(manifestos["manifesto_id"])
    if isinstance(itens, pd.DataFrame) and not itens.empty and "manifesto_id" in itens.columns:
        itens["manifesto_id"] = itens["manifesto_id"].astype(str).map(mapa_ids).fillna(itens["manifesto_id"])

    origem_por_prefixo = {
        "MF41_": "M4.1",
        "PM521_": "M5.2.1",
        "PM531_": "M5.3.1",
        "PM541_": "M5.4.1",
    }
    origem = origem_por_prefixo.get(prefixo)
    if origem:
        for df_alvo in (manifestos, itens):
            if not isinstance(df_alvo, pd.DataFrame) or df_alvo.empty:
                continue
            for col in ("origem_modulo", "origem_manifesto_modulo"):
                if col in df_alvo.columns:
                    df_alvo[col] = origem

    return manifestos, itens


def _persistir_snapshot_se_ativo(
    auditoria_ativa: bool,
    *,
    teste_id: str,
    rodada_id: str,
    upload_id: str,
    modulo: str,
    ordem_modulo: int,
    df_etapa: pd.DataFrame,
    snapshot_nome: str,
    contexto: dict,
    rastreamento: dict,
) -> int:
    if not auditoria_ativa:
        return 0
    try:
        return persistir_snapshot_modulo_auditoria(
            teste_id=teste_id,
            rodada_id=rodada_id,
            upload_id=upload_id,
            modulo=modulo,
            ordem_modulo=ordem_modulo,
            df_etapa=df_etapa,
            snapshot_nome=snapshot_nome,
            contexto=contexto,
            rastreamento=rastreamento,
        )
    except Exception as exc:
        print(f"[AUDITORIA] falha ao persistir snapshot={snapshot_nome}: {exc}")
        return 0


def _selecionar_colunas_obrigatorias_contrato(
    df: pd.DataFrame,
    colunas_obrigatorias: list[str],
    nome: str,
) -> pd.DataFrame:
    faltando = [c for c in colunas_obrigatorias if c not in df.columns]
    if faltando:
        raise Exception(f"Contrato Sistema 1 inválido: {nome} sem colunas obrigatórias: {faltando}")
    return df[colunas_obrigatorias].copy()


def _selecionar_colunas_reais_existentes(
    df: pd.DataFrame,
    colunas_desejadas: list[str],
) -> pd.DataFrame:
    existentes = [c for c in colunas_desejadas if c in df.columns]
    return df[existentes].copy()


def _extrair_dataframe_m3(outputs_m3: Dict[str, Any], chaves_candidatas: list[str]) -> pd.DataFrame:
    for chave in chaves_candidatas:
        df = outputs_m3.get(chave)
        if isinstance(df, pd.DataFrame):
            return df.copy()
    return pd.DataFrame()


def _filtrar_por_status_triagem(df: pd.DataFrame, status: str) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    if "status_triagem" not in df.columns:
        return pd.DataFrame()
    return df.loc[df["status_triagem"] == status].copy()


def _normalizar_perfil_comparacao(valor: Any) -> str:
    texto = "" if valor is None else str(valor).strip().upper()
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    return "".join(ch for ch in texto if not unicodedata.combining(ch))


def _validar_colunas_obrigatorias_veiculos(df_veiculos_tratados: pd.DataFrame) -> None:
    colunas_obrigatorias = [
        "perfil",
        "tipo",
        "qtd_eixos",
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]
    for coluna in colunas_obrigatorias:
        if coluna not in df_veiculos_tratados.columns:
            raise Exception(
                f"Cadastro de veículos inválido no Motor 2: coluna obrigatória {coluna} ausente em "
                "df_veiculos_tratados. O Sistema 1 deve enviar essa coluna no payload."
            )


def _anexar_dados_reais_veiculo_manifestos(
    df_manifestos_m7: pd.DataFrame,
    df_veiculos_tratados: pd.DataFrame,
) -> pd.DataFrame:
    df_manifestos = df_manifestos_m7.copy()

    colunas_manifesto_obrigatorias = ["manifesto_id", "perfil_final_m6_2"]
    faltando_manifestos = [c for c in colunas_manifesto_obrigatorias if c not in df_manifestos.columns]
    if faltando_manifestos:
        raise Exception(f"Contrato Sistema 1 inválido: manifestos_m7 sem colunas obrigatórias: {faltando_manifestos}")

    colunas_veiculo_obrigatorias = [
        "perfil",
        "qtd_eixos",
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]
    faltando_veiculos = [c for c in colunas_veiculo_obrigatorias if c not in df_veiculos_tratados.columns]
    if faltando_veiculos:
        raise Exception(f"Contrato Sistema 1 inválido: df_veiculos_tratados sem colunas obrigatórias: {faltando_veiculos}")

    df_manifestos["_perfil_norm"] = df_manifestos["perfil_final_m6_2"].apply(_normalizar_perfil_comparacao)

    df_veiculos_ref = df_veiculos_tratados[colunas_veiculo_obrigatorias].copy()
    df_veiculos_ref["_perfil_norm"] = df_veiculos_ref["perfil"].apply(_normalizar_perfil_comparacao)

    colunas_consistencia = [
        "qtd_eixos",
        "capacidade_peso_kg",
        "capacidade_vol_m3",
        "max_entregas",
        "max_km_distancia",
        "ocupacao_minima_perc",
        "ocupacao_maxima_perc",
    ]
    for perfil_norm, grupo in df_veiculos_ref.groupby("_perfil_norm", dropna=False):
        perfil_base = grupo["perfil"].iloc[0] if len(grupo) > 0 else perfil_norm
        if grupo[colunas_consistencia].drop_duplicates().shape[0] > 1:
            raise Exception(
                f"Cadastro de veículos inconsistente: perfil {perfil_base} possui parâmetros divergentes entre veículos."
            )

    df_veiculos_ref = (
        df_veiculos_ref.drop_duplicates(subset=["_perfil_norm"], keep="first")
        .rename(
            columns={
                "perfil": "perfil_veiculo_cadastro",
                "qtd_eixos": "qtd_eixos_cadastro",
                "capacidade_peso_kg": "capacidade_peso_kg_cadastro",
                "capacidade_vol_m3": "capacidade_vol_m3_cadastro",
                "max_entregas": "max_entregas_cadastro",
                "max_km_distancia": "max_km_distancia_cadastro",
                "ocupacao_minima_perc": "ocupacao_minima_perc_cadastro",
                "ocupacao_maxima_perc": "ocupacao_maxima_perc_cadastro",
            }
        )
    )

    df_manifestos = df_manifestos.merge(
        df_veiculos_ref,
        on="_perfil_norm",
        how="left",
        indicator=True,
    )

    perfis_disponiveis = sorted(
        df_veiculos_tratados["perfil"].dropna().astype(str).map(str.strip).replace("", pd.NA).dropna().unique().tolist()
    )
    nao_encontrados = df_manifestos[df_manifestos["_merge"] != "both"]
    if not nao_encontrados.empty:
        linha = nao_encontrados.iloc[0]
        raise Exception(
            "Contrato Sistema 1 inválido: manifesto "
            f"{linha.get('manifesto_id')} com perfil_final_m6_2={linha.get('perfil_final_m6_2')} "
            "não encontrou veículo correspondente em df_veiculos_tratados.perfil. "
            f"Perfis disponíveis: {perfis_disponiveis}."
        )

    duplicadas = df_manifestos.columns[df_manifestos.columns.duplicated()].tolist()
    if duplicadas:
        raise Exception(f"Contrato Sistema 1 inválido: colunas duplicadas após merge de veículos: {duplicadas}")

    mapeamento_destino_cadastro = {
        "qtd_eixos": "qtd_eixos_cadastro",
        "capacidade_peso_kg_veiculo": "capacidade_peso_kg_cadastro",
        "capacidade_vol_m3_veiculo": "capacidade_vol_m3_cadastro",
        "max_entregas_veiculo": "max_entregas_cadastro",
        "max_km_distancia_veiculo": "max_km_distancia_cadastro",
        "ocupacao_minima_perc_veiculo": "ocupacao_minima_perc_cadastro",
        "ocupacao_maxima_perc_veiculo": "ocupacao_maxima_perc_cadastro",
    }

    for coluna_destino, coluna_cadastro in mapeamento_destino_cadastro.items():
        if coluna_cadastro not in df_manifestos.columns:
            raise Exception(f"Contrato Sistema 1 inválido: coluna ausente após merge de veículos: {coluna_cadastro}")

        if coluna_destino in df_manifestos.columns:
            df_manifestos[coluna_destino] = df_manifestos[coluna_destino].fillna(df_manifestos[coluna_cadastro])
        else:
            df_manifestos[coluna_destino] = df_manifestos[coluna_cadastro]

    colunas_obrigatorias_pos_merge = [
        "manifesto_id",
        "perfil_final_m6_2",
        "qtd_eixos",
        "peso_final_m6_2",
        "ocupacao_final_m6_2",
        "km_total_estimado_m6_2",
        "qtd_itens_final_m6_2",
        "qtd_paradas_final_m6_2",
        "capacidade_peso_kg_veiculo",
        "max_km_distancia_veiculo",
    ]
    faltando_pos_merge = [c for c in colunas_obrigatorias_pos_merge if c not in df_manifestos.columns]
    if faltando_pos_merge:
        raise Exception(f"Contrato Sistema 1 inválido: manifestos_m7 sem colunas obrigatórias: {faltando_pos_merge}")

    for coluna in colunas_obrigatorias_pos_merge:
        if isinstance(df_manifestos[coluna], pd.DataFrame):
            raise Exception(f"Contrato Sistema 1 inválido: coluna duplicada detectada na validação: {coluna}")
        if df_manifestos[coluna].isna().any():
            manifestos_nulos = (
                df_manifestos.loc[df_manifestos[coluna].isna(), "manifesto_id"].dropna().astype(str).unique().tolist()
            )
            raise Exception(
                f"Contrato Sistema 1 inválido: coluna obrigatória {coluna} nula em manifestos {manifestos_nulos}."
            )

    colunas_auxiliares = [
        "_perfil_norm",
        "_merge",
        "perfil_veiculo_cadastro",
        "qtd_eixos_cadastro",
        "capacidade_peso_kg_cadastro",
        "capacidade_vol_m3_cadastro",
        "max_entregas_cadastro",
        "max_km_distancia_cadastro",
        "ocupacao_minima_perc_cadastro",
        "ocupacao_maxima_perc_cadastro",
    ]
    return df_manifestos.drop(columns=colunas_auxiliares, errors="ignore")


def _executar_m0_adapter(contexto: PipelineContext) -> Dict[str, Any]:
    inventario = {
        "rodada_id": contexto.rodada_id,
        "upload_id": contexto.upload_id,
        "usuario_id": contexto.usuario_id,
        "filial_id": contexto.filial_id,
        "tipo_roteirizacao": contexto.tipo_roteirizacao,
        "data_execucao": contexto.data_execucao.isoformat(),
        "data_base_roteirizacao": contexto.data_base.isoformat(),
        "filial": contexto.filial,
        "inputs": {
            "carteira": _snapshot_dataframe(contexto.df_carteira_raw, "df_carteira_raw"),
            "regionalidades": _snapshot_dataframe(contexto.df_geo_raw, "df_geo_raw"),
            "parametros": _snapshot_dataframe(contexto.df_parametros_raw, "df_parametros_raw"),
            "veiculos": _snapshot_dataframe(contexto.df_veiculos_raw, "df_veiculos_raw"),
        },
        "caminhos_pipeline": contexto.caminhos_pipeline,
    }

    return {
        "inventario": inventario,
        "df_carteira_raw": contexto.df_carteira_raw,
        "df_geo_raw": contexto.df_geo_raw,
        "df_parametros_raw": contexto.df_parametros_raw,
        "df_veiculos_raw": contexto.df_veiculos_raw,
    }


def _executar_pipeline_core(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    inicio_total = _agora()
    logs: List[Dict[str, Any]] = []
    metricas_tempo: Dict[str, float] = {}
    debug = _is_debug(payload)
    auditoria_ativa = debug or not MODO_PRODUCAO_CONTRATO_SISTEMA1
    retornar_auditoria_interna = debug or not MODO_PRODUCAO_CONTRATO_SISTEMA1
    log_verbose = debug or not MODO_PRODUCAO_CONTRATO_SISTEMA1
    teste_id_auditoria = str(uuid.uuid4())
    auditoria_por_modulo: Dict[str, int] = {}
    auditoria_por_snapshot: Dict[str, int] = {}
    auditoria_flat_rastreamento: Dict[str, Any] = {"colunas_persistidas": set()}
    auditoria_reprocessamento_etapas: Dict[str, Any] = {}
    def _print_log(*args: Any, force: bool = False) -> None:
        if force or log_verbose:
            print(*args)

    _print_log("[PIPELINE] Executando núcleo validado", force=True)
    _print_log("[AUDITORIA] teste_id:", teste_id_auditoria)
    _print_log("[PIPELINE FLAGS] executar_m4=", PIPELINE_FLAGS["executar_m4"])
    _print_log("[PIPELINE FLAGS] executar_m5_1=", PIPELINE_FLAGS["executar_m5_1"])
    _print_log("[PIPELINE FLAGS] executar_m5_2=", PIPELINE_FLAGS["executar_m5_2"])
    _print_log("[PIPELINE FLAGS] executar_m5_3a=", PIPELINE_FLAGS["executar_m5_3a"])
    _print_log("[PIPELINE FLAGS] executar_m5_3b=", PIPELINE_FLAGS["executar_m5_3b"])
    _print_log("[PIPELINE FLAGS] executar_m5_4a=", PIPELINE_FLAGS["executar_m5_4a"])
    _print_log("[PIPELINE FLAGS] executar_m5_4b=", PIPELINE_FLAGS["executar_m5_4b"])
    _print_log("[PIPELINE FLAGS] executar_m6_1=", PIPELINE_FLAGS["executar_m6_1"])
    _print_log("[PIPELINE FLAGS] executar_m6_2=", PIPELINE_FLAGS["executar_m6_2"])
    _print_log("[PIPELINE FLAGS] executar_m7=", PIPELINE_FLAGS["executar_m7"])
    _print_log("[PIPELINE FLAGS] executar_validadores_exaustao=", PIPELINE_FLAGS.get("executar_validadores_exaustao"))

    # =========================================================================================
    # PAYLOAD -> CONTEXTO
    # =========================================================================================
    t0 = _agora()
    contexto = normalizar_payload_para_pipeline(payload)
    tempo_payload = _duracao_ms(t0)
    metricas_tempo["payload_service_ms"] = tempo_payload

    logs.append(
        _log(
            modulo="payload_service",
            status="ok",
            mensagem="Payload normalizado para o contexto interno do pipeline",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(contexto.df_carteira_raw),
            tempo_ms=tempo_payload,
            extra={
                "rodada_id": contexto.rodada_id,
                "filial_id": contexto.filial_id,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
            },
        )
    )
    contexto_auditoria = {
        "filial_id": contexto.filial_id,
        "usuario_id": contexto.usuario_id,
        "tipo_roteirizacao": contexto.tipo_roteirizacao,
        "data_base_roteirizacao": contexto.data_base.isoformat(),
    }
    total_payload = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="payload_service",
        ordem_modulo=0,
        df_etapa=contexto.df_carteira_raw,
        snapshot_nome="payload_service",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["payload_service"] = auditoria_por_modulo.get("payload_service", 0) + total_payload
    auditoria_por_snapshot["payload_service"] = auditoria_por_snapshot.get("payload_service", 0) + total_payload
    _print_log(f"[AUDITORIA FLAT] snapshot=payload_service linhas={total_payload}")

    # =========================================================================================
    # M0
    # =========================================================================================
    t0 = _agora()
    resultado_m0 = _executar_m0_adapter(contexto)
    tempo_m0 = _duracao_ms(t0)
    metricas_tempo["m0_adapter_ms"] = tempo_m0

    logs.append(
        _log(
            modulo="m0_adapter",
            status="ok",
            mensagem="M0 adaptado executado com sucesso",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(contexto.df_carteira_raw),
            tempo_ms=tempo_m0,
            extra={
                "filial": contexto.filial,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
            },
        )
    )
    total_m0 = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m0_adapter",
        ordem_modulo=1,
        df_etapa=resultado_m0["df_carteira_raw"],
        snapshot_nome="m0_adapter",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m0_adapter"] = auditoria_por_modulo.get("m0_adapter", 0) + total_m0
    auditoria_por_snapshot["m0_adapter"] = auditoria_por_snapshot.get("m0_adapter", 0) + total_m0
    _print_log(f"[AUDITORIA FLAT] snapshot=m0_adapter linhas={total_m0}")

    # =========================================================================================
    # M1
    # =========================================================================================
    t0 = _agora()
    resultado_m1 = executar_m1_padronizacao(
        df_carteira_raw=resultado_m0["df_carteira_raw"],
        df_geo_raw=resultado_m0["df_geo_raw"],
        df_parametros_raw=resultado_m0["df_parametros_raw"],
        df_veiculos_raw=resultado_m0["df_veiculos_raw"],
    )
    tempo_m1 = _duracao_ms(t0)
    metricas_tempo["m1_padronizacao_ms"] = tempo_m1

    df_carteira_tratada = resultado_m1["df_carteira_tratada"]
    df_geo_tratado = resultado_m1["df_geo_tratado"]
    df_parametros_tratados = resultado_m1["df_parametros_tratados"]
    df_veiculos_tratados = resultado_m1["df_veiculos_tratados"]
    _validar_colunas_obrigatorias_veiculos(df_veiculos_tratados)

    resumo_m1 = {
        "carteira_colunas": int(len(df_carteira_tratada.columns)),
        "geo_colunas": int(len(df_geo_tratado.columns)),
        "parametros_colunas": int(len(df_parametros_tratados.columns)),
        "veiculos_colunas": int(len(df_veiculos_tratados.columns)),
    }

    logs.append(
        _log(
            modulo="m1_padronizacao",
            status="ok",
            mensagem="M1 executado com sucesso",
            quantidade_entrada=_safe_len(contexto.df_carteira_raw),
            quantidade_saida=_safe_len(df_carteira_tratada),
            tempo_ms=tempo_m1,
            extra=resumo_m1,
        )
    )
    total_m1 = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m1_padronizacao",
        ordem_modulo=2,
        df_etapa=df_carteira_tratada,
        snapshot_nome="m1_padronizacao",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m1_padronizacao"] = auditoria_por_modulo.get("m1_padronizacao", 0) + total_m1
    auditoria_por_snapshot["m1_padronizacao"] = auditoria_por_snapshot.get("m1_padronizacao", 0) + total_m1
    _print_log(f"[AUDITORIA FLAT] snapshot=m1_padronizacao linhas={total_m1}")

    # =========================================================================================
    # M2
    # =========================================================================================
    t0 = _agora()
    df_carteira_enriquecida, resumo_m2 = executar_m2_enriquecimento(
        df_carteira_tratada=df_carteira_tratada,
        df_geo_tratado=df_geo_tratado,
        df_parametros_tratados=df_parametros_tratados,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m2 = _duracao_ms(t0)
    metricas_tempo["m2_enriquecimento_ms"] = tempo_m2

    logs.append(
        _log(
            modulo="m2_enriquecimento",
            status="ok",
            mensagem="M2 executado com sucesso",
            quantidade_entrada=_safe_len(df_carteira_tratada),
            quantidade_saida=_safe_len(df_carteira_enriquecida),
            tempo_ms=tempo_m2,
            extra=resumo_m2,
        )
    )
    total_m2 = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m2_enriquecimento",
        ordem_modulo=3,
        df_etapa=df_carteira_enriquecida,
        snapshot_nome="m2_enriquecimento",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m2_enriquecimento"] = auditoria_por_modulo.get("m2_enriquecimento", 0) + total_m2
    auditoria_por_snapshot["m2_enriquecimento"] = auditoria_por_snapshot.get("m2_enriquecimento", 0) + total_m2
    _print_log(f"[AUDITORIA FLAT] snapshot=m2_enriquecimento linhas={total_m2}")

    # =========================================================================================
    # M3
    # =========================================================================================
    t0 = _agora()
    df_carteira_triagem, meta_m3 = executar_m3_triagem(
        df_carteira_enriquecida=df_carteira_enriquecida,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m3 = _duracao_ms(t0)
    metricas_tempo["m3_triagem_ms"] = tempo_m3

    outputs_m3 = meta_m3["outputs_m3"]
    resumo_m3 = meta_m3["resumo_m3"]
    output_keys_m3 = sorted(list(outputs_m3.keys()))
    _print_log(f"[M3] outputs disponíveis: {output_keys_m3}", force=True)

    df_carteira_roteirizavel = outputs_m3["df_carteira_roteirizavel"]
    df_carteira_agendamento_futuro = outputs_m3["df_carteira_agendamento_futuro"]
    df_carteira_agendas_vencidas = outputs_m3["df_carteira_agendas_vencidas"]
    df_agendamento_futuro_m3 = _extrair_dataframe_m3(
        outputs_m3, ["df_agendamento_futuro", "df_carteira_agendamento_futuro"]
    )
    df_aguardando_agendamento_m3 = _extrair_dataframe_m3(
        outputs_m3, ["df_aguardando_agendamento", "df_carteira_aguardando_agendamento"]
    )
    df_excecoes_triagem_m3 = _extrair_dataframe_m3(
        outputs_m3, ["df_excecoes_triagem", "df_carteira_excecoes_triagem"]
    )
    if df_excecoes_triagem_m3.empty:
        df_excecoes_triagem_m3 = _filtrar_por_status_triagem(df_carteira_triagem, "excecao_triagem")
    df_agenda_vencida_m3 = _extrair_dataframe_m3(
        outputs_m3, ["df_agenda_vencida", "df_agendas_vencidas", "df_carteira_agendas_vencidas"]
    )
    if df_agenda_vencida_m3.empty:
        df_agenda_vencida_m3 = _filtrar_por_status_triagem(df_carteira_triagem, "agenda_vencida")
    df_nao_roteirizaveis_m3 = _extrair_dataframe_m3(outputs_m3, ["df_nao_roteirizaveis_m3"])
    if df_nao_roteirizaveis_m3.empty:
        df_nao_roteirizaveis_m3 = _consolidar_remanescente_global(
            _consolidar_remanescente_global(
                _consolidar_remanescente_global(df_agendamento_futuro_m3, df_aguardando_agendamento_m3),
                df_excecoes_triagem_m3,
            ),
            df_agenda_vencida_m3,
        )

    logs.append(
        _log(
            modulo="m3_triagem",
            status="ok",
            mensagem="M3 executado com sucesso",
            quantidade_entrada=_safe_len(df_carteira_enriquecida),
            quantidade_saida=_safe_len(df_carteira_triagem),
            tempo_ms=tempo_m3,
            extra=resumo_m3,
        )
    )
    total_m3 = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m3_triagem",
        ordem_modulo=4,
        df_etapa=df_carteira_triagem,
        snapshot_nome="m3_triagem",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m3_triagem"] = auditoria_por_modulo.get("m3_triagem", 0) + total_m3
    auditoria_por_snapshot["m3_triagem"] = auditoria_por_snapshot.get("m3_triagem", 0) + total_m3
    _print_log(f"[AUDITORIA FLAT] snapshot=m3_triagem linhas={total_m3}")


    # =========================================================================================
    # M3.1
    # =========================================================================================
    t0 = _agora()
    df_input_oficial_bloco_4, meta_m31 = executar_m3_1_validacao_fronteira(
        df_carteira_roteirizavel=df_carteira_roteirizavel,
        data_base_roteirizacao=contexto.data_base,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m31 = _duracao_ms(t0)
    metricas_tempo["m3_1_validacao_fronteira_ms"] = tempo_m31

    resumo_m31 = meta_m31["resumo_m31"]

    logs.append(
        _log(
            modulo="m3_1_validacao_fronteira",
            status="ok",
            mensagem="M3.1 executado com sucesso e input oficial do bloco 4 foi consolidado",
            quantidade_entrada=_safe_len(df_carteira_roteirizavel),
            quantidade_saida=_safe_len(df_input_oficial_bloco_4),
            tempo_ms=tempo_m31,
            extra=resumo_m31,
        )
    )
    total_m3_1 = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m3_1_validacao_fronteira",
        ordem_modulo=5,
        df_etapa=df_input_oficial_bloco_4,
        snapshot_nome="m3_1_validacao_fronteira",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m3_1_validacao_fronteira"] = auditoria_por_modulo.get("m3_1_validacao_fronteira", 0) + total_m3_1
    auditoria_por_snapshot["m3_1_validacao_fronteira"] = auditoria_por_snapshot.get("m3_1_validacao_fronteira", 0) + total_m3_1
    _print_log(f"[AUDITORIA FLAT] snapshot=m3_1_validacao_fronteira linhas={total_m3_1}")

    if not PIPELINE_FLAGS["executar_m4"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente na etapa base de auditoria (M3.1).",
            "pipeline_real_ate": "M3.1",
            "modo_resposta": "auditoria_base_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_agendamento_futuro_m3": _safe_len(df_carteira_agendamento_futuro),
                "total_agendas_vencidas_m3": _safe_len(df_carteira_agendas_vencidas),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
            },
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M4
    # =========================================================================================
    # OBS AUDITORIA FUTURA (M4+):
    # Quando as etapas M4/M5/M6/M7 forem religadas para persistência de auditoria,
    # priorizar sempre os DATAFRAMES OFICIAIS DE ITENS de cada etapa (não apenas resumos/manifestos agregados),
    # mantendo o histórico linha a linha do dataset operacional.
    t0 = _agora()
    _print_log(f"[M4] executando M4 com input oficial do bloco 4 linhas={_safe_len(df_input_oficial_bloco_4)}")
    outputs_m4, meta_m4 = executar_m4_manifestos_fechados(
        df_input_oficial_bloco_4=df_input_oficial_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
        rodada_id=contexto.rodada_id,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        configuracao_frota=payload.configuracao_frota,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m4 = _duracao_ms(t0)
    metricas_tempo["m4_manifestos_fechados_ms"] = tempo_m4

    resumo_m4 = meta_m4["resumo_m4"]

    df_manifestos_m4 = outputs_m4.get("df_manifestos_m4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = outputs_m4.get("df_manifestos_fechados_bloco_4")
    if df_manifestos_m4 is None or not isinstance(df_manifestos_m4, pd.DataFrame):
        df_manifestos_m4 = pd.DataFrame()

    df_itens_m4 = outputs_m4.get("df_itens_m4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = outputs_m4.get("df_itens_manifestos_fechados_bloco_4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = outputs_m4.get("df_itens_manifestados_bloco_4")
    if df_itens_m4 is None or not isinstance(df_itens_m4, pd.DataFrame):
        df_itens_m4 = pd.DataFrame()

    df_remanescente_m4 = outputs_m4.get("df_remanescente_m4")
    if df_remanescente_m4 is None or not isinstance(df_remanescente_m4, pd.DataFrame):
        df_remanescente_m4 = outputs_m4.get("df_remanescente_roteirizavel_bloco_4")
    if df_remanescente_m4 is None or not isinstance(df_remanescente_m4, pd.DataFrame):
        df_remanescente_m4 = pd.DataFrame()
    df_remanescente_roteirizavel_bloco_4 = df_remanescente_m4
    df_itens_manifestados_m4 = df_itens_m4

    _print_log(f"[M4] df_manifestos_m4 linhas={_safe_len(df_manifestos_m4)}", force=not log_verbose)
    _print_log(f"[M4] df_itens_m4 linhas={_safe_len(df_itens_m4)}")
    _print_log(f"[M4] df_remanescente_m4 linhas={_safe_len(df_remanescente_m4)}")

    if PIPELINE_FLAGS.get("executar_validadores_exaustao") and PIPELINE_FLAGS.get("executar_m4_repescagem"):
        df_m4_repescagem_input, meta_m4_1 = _filtrar_remanescentes_com_potencial(
            df_remanescente_m4,
            chaves_grupo=[
                "tomador",
                "Tomador",
                "destinatario",
                "Destinatário",
                "cliente",
            ],
            df_veiculos_tratados=df_veiculos_tratados,
            nome_etapa="M4.1",
        )
        if isinstance(df_m4_repescagem_input, pd.DataFrame) and not df_m4_repescagem_input.empty:
            outputs_m4_1, _ = executar_m4_manifestos_fechados(
                df_input_oficial_bloco_4=df_m4_repescagem_input,
                df_veiculos_tratados=df_veiculos_tratados,
                rodada_id=contexto.rodada_id,
                data_base_roteirizacao=contexto.data_base,
                tipo_roteirizacao=contexto.tipo_roteirizacao,
                configuracao_frota=payload.configuracao_frota,
                caminhos_pipeline=contexto.caminhos_pipeline,
            )
            df_manifestos_m4_1 = _copiar_ou_vazio(outputs_m4_1.get("df_manifestos_m4"))
            if df_manifestos_m4_1.empty:
                df_manifestos_m4_1 = _copiar_ou_vazio(outputs_m4_1.get("df_manifestos_fechados_bloco_4"))
            df_itens_m4_1 = _copiar_ou_vazio(outputs_m4_1.get("df_itens_m4"))
            if df_itens_m4_1.empty:
                df_itens_m4_1 = _copiar_ou_vazio(outputs_m4_1.get("df_itens_manifestos_fechados_bloco_4"))
            if df_itens_m4_1.empty:
                df_itens_m4_1 = _copiar_ou_vazio(outputs_m4_1.get("df_itens_manifestados_bloco_4"))
            df_manifestos_m4_1, df_itens_m4_1 = _renomear_manifestos_repescagem(
                df_manifestos_m4_1,
                df_itens_m4_1,
                prefixo="MF41_",
            )
            df_manifestos_m4 = pd.concat([_copiar_ou_vazio(df_manifestos_m4), df_manifestos_m4_1], ignore_index=True, sort=False)
            df_itens_m4 = pd.concat([_copiar_ou_vazio(df_itens_m4), df_itens_m4_1], ignore_index=True, sort=False)
            df_itens_manifestados_m4 = df_itens_m4
            df_remanescente_m4 = _remover_itens_alocados_do_remanescente(df_remanescente_m4, df_itens_m4_1)
            df_remanescente_roteirizavel_bloco_4 = df_remanescente_m4
            auditoria_reprocessamento_etapas["m4_1"] = {
                **meta_m4_1,
                "manifestos_adicionais": _safe_len(df_manifestos_m4_1),
                "itens_recuperados": _safe_len(df_itens_m4_1),
                "remanescente_apos": _safe_len(df_remanescente_m4),
            }
        else:
            auditoria_reprocessamento_etapas["m4_1"] = {
                **meta_m4_1,
                "manifestos_adicionais": 0,
                "itens_recuperados": 0,
                "remanescente_apos": _safe_len(df_remanescente_m4),
            }
    else:
        auditoria_reprocessamento_etapas["m4_1"] = {
            "ativado": False,
            "motivo": "desligado_por_flag",
            "manifestos_adicionais": 0,
            "itens_recuperados": 0,
            "remanescente_apos": _safe_len(df_remanescente_m4),
        }

    logs.append(
        _log(
            modulo="m4_manifestos_fechados",
            status="ok",
            mensagem="M4 executado com sucesso",
            quantidade_entrada=_safe_len(df_input_oficial_bloco_4),
            quantidade_saida=_safe_len(df_remanescente_m4),
            tempo_ms=tempo_m4,
            extra={
                **resumo_m4,
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_m4),
                "total_remanescente_global_m4": _safe_len(df_remanescente_m4),
            },
        )
    )
    total_m4_manifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_manifestos_m4,
        snapshot_nome="m4_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_manifestos
    auditoria_por_snapshot["m4_manifestos"] = auditoria_por_snapshot.get("m4_manifestos", 0) + total_m4_manifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m4_manifestos linhas={total_m4_manifestos}")

    total_m4_itens = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_itens_m4,
        snapshot_nome="m4_itens",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_itens
    auditoria_por_snapshot["m4_itens"] = auditoria_por_snapshot.get("m4_itens", 0) + total_m4_itens
    _print_log(f"[AUDITORIA FLAT] snapshot=m4_itens linhas={total_m4_itens}")

    total_m4_remanescente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m4_manifestos_fechados",
        ordem_modulo=6,
        df_etapa=df_remanescente_m4,
        snapshot_nome="m4_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m4_manifestos_fechados"] = auditoria_por_modulo.get("m4_manifestos_fechados", 0) + total_m4_remanescente
    auditoria_por_snapshot["m4_remanescente"] = auditoria_por_snapshot.get("m4_remanescente", 0) + total_m4_remanescente
    _print_log(f"[AUDITORIA FLAT] snapshot=m4_remanescente linhas={total_m4_remanescente}")

    if not PIPELINE_FLAGS["executar_m5_1"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M4 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M4",
            "modo_resposta": "auditoria_m4_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_agendamento_futuro_m3": _safe_len(df_carteira_agendamento_futuro),
                "total_agendas_vencidas_m3": _safe_len(df_carteira_agendas_vencidas),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_entrada_bloco_4": int(resumo_m4.get("roteirizavel_entrada_m4", _safe_len(df_input_oficial_bloco_4))),
                "dedicados_encontrados_m4": int(meta_m4.get("metricas_m4", {}).get("contadores_m4", {}).get("qtd_manifestos_exclusivos", 0)),
                "total_manifestos_m4": int(resumo_m4.get("manifestos_fechados_gerados_m4", _safe_len(df_manifestos_m4))),
                "total_itens_manifestados_m4": int(resumo_m4.get("itens_manifestados_m4", _safe_len(df_itens_m4))),
                "total_remanescente_m4": int(resumo_m4.get("remanescente_roteirizavel_m4", _safe_len(df_remanescente_m4))),
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
            },
            "resumo_m4": {
                **resumo_m4,
                "total_entrada": int(resumo_m4.get("roteirizavel_entrada_m4", _safe_len(df_input_oficial_bloco_4))),
                "dedicados_encontrados": int(meta_m4.get("metricas_m4", {}).get("contadores_m4", {}).get("qtd_manifestos_exclusivos", 0)),
                "manifestos_fechados": int(resumo_m4.get("manifestos_fechados_gerados_m4", _safe_len(df_manifestos_m4))),
                "itens_manifestados": int(resumo_m4.get("itens_manifestados_m4", _safe_len(df_itens_m4))),
                "remanescente": int(resumo_m4.get("remanescente_roteirizavel_m4", _safe_len(df_remanescente_m4))),
            },
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M5.1
    # =========================================================================================
    t0 = _agora()
    _print_log(f"[M5.1] executando M5.1 com remanescente do M4 linhas={_safe_len(df_remanescente_roteirizavel_bloco_4)}")
    outputs_m5_1, meta_m5_1 = executar_m5_1_triagem_cidades(
        df_remanescente_roteirizavel_bloco_4=df_remanescente_roteirizavel_bloco_4,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_1 = _duracao_ms(t0)
    metricas_tempo["m5_1_triagem_cidades_ms"] = tempo_m5_1

    resumo_m5_1 = meta_m5_1["resumo_m5_1"]
    df_cidades_consolidadas_m5_1 = outputs_m5_1["df_cidades_consolidadas_m5_1"]
    df_perfis_viaveis_por_cidade_m5_1 = outputs_m5_1["df_perfis_viaveis_por_cidade_m5_1"]
    df_saldo_elegivel_composicao_m5_1 = outputs_m5_1["df_saldo_elegivel_composicao_m5_1"]
    df_saldo_nao_elegivel_m5_1 = outputs_m5_1["df_saldo_nao_elegivel_m5_1"]
    df_perfis_elegiveis_por_cidade_m5_1 = outputs_m5_1["df_perfis_elegiveis_por_cidade_m5_1"]
    df_perfis_descartados_por_cidade_m5_1 = outputs_m5_1["df_perfis_descartados_por_cidade_m5_1"]
    df_tentativas_triagem_cidades_m5_1 = outputs_m5_1["df_tentativas_triagem_cidades_m5_1"]

    _print_log(f"[M5.1] df_cidades_consolidadas_m5_1 linhas={_safe_len(df_cidades_consolidadas_m5_1)}")
    _print_log(f"[M5.1] df_perfis_elegiveis_por_cidade_m5_1 linhas={_safe_len(df_perfis_elegiveis_por_cidade_m5_1)}")
    _print_log(f"[M5.1] df_perfis_descartados_por_cidade_m5_1 linhas={_safe_len(df_perfis_descartados_por_cidade_m5_1)}")
    _print_log(f"[M5.1] df_saldo_elegivel_composicao_m5_1 linhas={_safe_len(df_saldo_elegivel_composicao_m5_1)}")
    _print_log(f"[M5.1] df_saldo_nao_elegivel_m5_1 linhas={_safe_len(df_saldo_nao_elegivel_m5_1)}")
    _print_log(f"[M5.1] df_tentativas_triagem_cidades_m5_1 linhas={_safe_len(df_tentativas_triagem_cidades_m5_1)}")

    logs.append(
        _log(
            modulo="m5_1_triagem_cidades",
            status="ok",
            mensagem="M5.1 executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_roteirizavel_bloco_4),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_1),
            tempo_ms=tempo_m5_1,
            extra=resumo_m5_1,
        )
    )
    total_m5_1_cidades_consolidadas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_cidades_consolidadas_m5_1,
        snapshot_nome="m5_1_cidades_consolidadas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_cidades_consolidadas
    auditoria_por_snapshot["m5_1_cidades_consolidadas"] = auditoria_por_snapshot.get("m5_1_cidades_consolidadas", 0) + total_m5_1_cidades_consolidadas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_1_cidades_consolidadas linhas={total_m5_1_cidades_consolidadas}")

    total_m5_1_perfis_viaveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_viaveis_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_viaveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_viaveis
    auditoria_por_snapshot["m5_1_perfis_viaveis"] = auditoria_por_snapshot.get("m5_1_perfis_viaveis", 0) + total_m5_1_perfis_viaveis

    total_m5_1_perfis_elegiveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_elegiveis_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_elegiveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_elegiveis
    auditoria_por_snapshot["m5_1_perfis_elegiveis"] = auditoria_por_snapshot.get("m5_1_perfis_elegiveis", 0) + total_m5_1_perfis_elegiveis
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_1_perfis_elegiveis linhas={total_m5_1_perfis_elegiveis}")

    total_m5_1_perfis_descartados = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_perfis_descartados_por_cidade_m5_1,
        snapshot_nome="m5_1_perfis_descartados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_perfis_descartados
    auditoria_por_snapshot["m5_1_perfis_descartados"] = auditoria_por_snapshot.get("m5_1_perfis_descartados", 0) + total_m5_1_perfis_descartados

    total_m5_1_saldo_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_saldo_elegivel_composicao_m5_1,
        snapshot_nome="m5_1_saldo_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_saldo_elegivel
    auditoria_por_snapshot["m5_1_saldo_elegivel"] = auditoria_por_snapshot.get("m5_1_saldo_elegivel", 0) + total_m5_1_saldo_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_1_saldo_elegivel linhas={total_m5_1_saldo_elegivel}")

    total_m5_1_saldo_nao_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_saldo_nao_elegivel_m5_1,
        snapshot_nome="m5_1_saldo_nao_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_saldo_nao_elegivel
    auditoria_por_snapshot["m5_1_saldo_nao_elegivel"] = auditoria_por_snapshot.get("m5_1_saldo_nao_elegivel", 0) + total_m5_1_saldo_nao_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_1_saldo_nao_elegivel linhas={total_m5_1_saldo_nao_elegivel}")

    total_m5_1_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_1_triagem_cidades",
        ordem_modulo=7,
        df_etapa=df_tentativas_triagem_cidades_m5_1,
        snapshot_nome="m5_1_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_1_triagem_cidades"] = auditoria_por_modulo.get("m5_1_triagem_cidades", 0) + total_m5_1_tentativas
    auditoria_por_snapshot["m5_1_tentativas"] = auditoria_por_snapshot.get("m5_1_tentativas", 0) + total_m5_1_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_1_tentativas linhas={total_m5_1_tentativas}")

    if not PIPELINE_FLAGS["executar_m5_2"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.1 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.1",
            "modo_resposta": "auditoria_m5_1_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_agendamento_futuro_m3": _safe_len(df_carteira_agendamento_futuro),
                "total_agendas_vencidas_m3": _safe_len(df_carteira_agendas_vencidas),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "linhas_entrada_m5_1": int(resumo_m5_1.get("linhas_entrada", 0)),
                "cidades_total_m5_1": int(resumo_m5_1.get("cidades_total", 0)),
                "cidades_elegiveis_m5_1": int(resumo_m5_1.get("cidades_elegiveis", 0)),
                "cidades_nao_elegiveis_m5_1": int(resumo_m5_1.get("cidades_nao_elegiveis", 0)),
                "perfis_testados_total_m5_1": int(resumo_m5_1.get("perfis_testados_total", 0)),
                "perfis_elegiveis_total_m5_1": int(resumo_m5_1.get("perfis_elegiveis_total", 0)),
                "perfis_descartados_total_m5_1": int(resumo_m5_1.get("perfis_descartados_total", 0)),
                "linhas_saldo_elegivel_composicao_m5_1": int(resumo_m5_1.get("linhas_saldo_elegivel_composicao_m5_1", 0)),
                "linhas_saldo_nao_elegivel_m5_1": int(resumo_m5_1.get("linhas_saldo_nao_elegivel_m5_1", 0)),
                "resumo_m3": resumo_m3,
                "resumo_m31": resumo_m31,
                "resumo_m4": resumo_m4,
            },
            "resumo_m5_1": resumo_m5_1,
            "contexto_rodada": {
                "filial": contexto.filial,
                "parametros_rodada": contexto.parametros_rodada,
            },
            "logs": logs,
        }

    # =========================================================================================
    # M5.2
    # =========================================================================================
    t0 = _agora()
    _print_log(f"[M5.2] executando M5.2 com saldo elegivel linhas={_safe_len(df_saldo_elegivel_composicao_m5_1)}")
    m5_2_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_1, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_1.empty
    m5_2_tem_perfis = isinstance(df_perfis_elegiveis_por_cidade_m5_1, pd.DataFrame) and not df_perfis_elegiveis_por_cidade_m5_1.empty
    if m5_2_tem_saldo and m5_2_tem_perfis:
        outputs_m5_2, meta_m5_2 = executar_m5_2_composicao_cidades(
            df_saldo_elegivel_composicao_m5_1=df_saldo_elegivel_composicao_m5_1,
            df_perfis_elegiveis_por_cidade_m5_1=df_perfis_elegiveis_por_cidade_m5_1,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        motivo_pulo = "sem_saldo_elegivel_m5_2" if not m5_2_tem_saldo else "sem_perfis_elegiveis_m5_2"
        outputs_m5_2 = {
            "df_premanifestos_m5_2": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_2": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_1).columns.tolist()),
            "df_remanescente_m5_2": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_1),
            "df_tentativas_m5_2": pd.DataFrame(columns=["resultado", "motivo"]),
        }
        meta_m5_2 = {
            "resumo_m5_2": {
                "modulo": "M5.2",
                "etapa_pulada": True,
                "motivo_etapa_pulada": motivo_pulo,
                "linhas_entrada_m5_2": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "linhas_saida_m5_2": 0,
                "remanescente_preservado_m5_2": _safe_len(df_saldo_elegivel_composicao_m5_1),
            }
        }
    tempo_m5_2 = _duracao_ms(t0)
    metricas_tempo["m5_2_composicao_cidades_ms"] = tempo_m5_2

    resumo_m5_2 = meta_m5_2["resumo_m5_2"]
    df_premanifestos_m5_2 = outputs_m5_2["df_premanifestos_m5_2"]
    df_itens_premanifestos_m5_2 = outputs_m5_2["df_itens_premanifestos_m5_2"]
    df_remanescente_m5_2 = outputs_m5_2["df_remanescente_m5_2"]
    df_tentativas_m5_2 = outputs_m5_2["df_tentativas_m5_2"]
    _print_log(f"[M5.2] df_premanifestos_m5_2 linhas={_safe_len(df_premanifestos_m5_2)}")
    _print_log(f"[M5.2] df_itens_premanifestos_m5_2 linhas={_safe_len(df_itens_premanifestos_m5_2)}", force=not log_verbose)
    _print_log(f"[M5.2] df_remanescente_m5_2 linhas={_safe_len(df_remanescente_m5_2)}")
    _print_log(f"[M5.2] df_tentativas_m5_2 linhas={_safe_len(df_tentativas_m5_2)}")

    if PIPELINE_FLAGS.get("executar_validadores_exaustao") and PIPELINE_FLAGS.get("executar_m5_2_repescagem"):
        chaves_m5_2 = ["cidade", "Cida", "Cidade Dest.", "cidade_dest", "uf", "UF"]
        col_cidade_m5_2 = _primeira_coluna_existente(df_remanescente_m5_2, ["cidade", "Cida", "Cidade Dest.", "cidade_dest"])
        col_uf_m5_2 = _primeira_coluna_existente(df_remanescente_m5_2, ["uf", "UF"])
        if col_cidade_m5_2 and col_uf_m5_2:
            chaves_m5_2 = [col_cidade_m5_2, col_uf_m5_2]
        df_m5_2_repescagem_input, meta_m5_2_1 = _filtrar_remanescentes_com_potencial(
            df_remanescente_m5_2,
            chaves_grupo=chaves_m5_2,
            df_veiculos_tratados=df_veiculos_tratados,
            nome_etapa="M5.2.1",
        )
        if isinstance(df_m5_2_repescagem_input, pd.DataFrame) and not df_m5_2_repescagem_input.empty:
            df_perfis_m5_2_repescagem = _copiar_ou_vazio(df_perfis_elegiveis_por_cidade_m5_1)
            col_cidade_input = _primeira_coluna_existente(df_m5_2_repescagem_input, ["cidade", "Cida", "Cidade Dest.", "cidade_dest"])
            col_cidade_perfil = _primeira_coluna_existente(df_perfis_m5_2_repescagem, ["cidade", "Cida", "Cidade Dest.", "cidade_dest"])
            if col_cidade_input and col_cidade_perfil and not df_perfis_m5_2_repescagem.empty:
                cidades_alvo = set(df_m5_2_repescagem_input[col_cidade_input].dropna().astype(str))
                df_perfis_m5_2_repescagem = df_perfis_m5_2_repescagem[
                    df_perfis_m5_2_repescagem[col_cidade_perfil].astype(str).isin(cidades_alvo)
                ].copy()
            outputs_m5_2_1, _ = executar_m5_2_composicao_cidades(
                df_saldo_elegivel_composicao_m5_1=df_m5_2_repescagem_input,
                df_perfis_elegiveis_por_cidade_m5_1=df_perfis_m5_2_repescagem,
                rodada_id=contexto.rodada_id,
                data_base_roteirizacao=contexto.data_base,
                tipo_roteirizacao=contexto.tipo_roteirizacao,
                caminhos_pipeline=contexto.caminhos_pipeline,
            )
            df_premanifestos_m5_2_1 = _copiar_ou_vazio(outputs_m5_2_1.get("df_premanifestos_m5_2"))
            df_itens_premanifestos_m5_2_1 = _copiar_ou_vazio(outputs_m5_2_1.get("df_itens_premanifestos_m5_2"))
            df_premanifestos_m5_2_1, df_itens_premanifestos_m5_2_1 = _renomear_manifestos_repescagem(
                df_premanifestos_m5_2_1,
                df_itens_premanifestos_m5_2_1,
                prefixo="PM521_",
            )
            df_premanifestos_m5_2 = pd.concat([_copiar_ou_vazio(df_premanifestos_m5_2), df_premanifestos_m5_2_1], ignore_index=True, sort=False)
            df_itens_premanifestos_m5_2 = pd.concat(
                [_copiar_ou_vazio(df_itens_premanifestos_m5_2), df_itens_premanifestos_m5_2_1],
                ignore_index=True,
                sort=False,
            )
            df_remanescente_m5_2 = _remover_itens_alocados_do_remanescente(df_remanescente_m5_2, df_itens_premanifestos_m5_2_1)
            auditoria_reprocessamento_etapas["m5_2_1"] = {
                **meta_m5_2_1,
                "manifestos_adicionais": _safe_len(df_premanifestos_m5_2_1),
                "itens_recuperados": _safe_len(df_itens_premanifestos_m5_2_1),
                "remanescente_apos": _safe_len(df_remanescente_m5_2),
            }
        else:
            auditoria_reprocessamento_etapas["m5_2_1"] = {
                **meta_m5_2_1,
                "manifestos_adicionais": 0,
                "itens_recuperados": 0,
                "remanescente_apos": _safe_len(df_remanescente_m5_2),
            }
    else:
        auditoria_reprocessamento_etapas["m5_2_1"] = {
            "ativado": False,
            "motivo": "desligado_por_flag",
            "manifestos_adicionais": 0,
            "itens_recuperados": 0,
            "remanescente_apos": _safe_len(df_remanescente_m5_2),
        }

    logs.append(
        _log(
            modulo="m5_2_composicao_cidades",
            status="ok" if m5_2_tem_saldo and m5_2_tem_perfis else "ignorado",
            mensagem="M5.2 executado com sucesso" if m5_2_tem_saldo and m5_2_tem_perfis else "M5.2 pulado por ausência de entrada válida",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_1),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_2),
            tempo_ms=tempo_m5_2,
            extra={
                **resumo_m5_2,
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
            },
        )
    )

    total_m5_2_premanifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_2_composicao_cidades",
        ordem_modulo=8,
        df_etapa=df_premanifestos_m5_2,
        snapshot_nome="m5_2_premanifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_2_composicao_cidades"] = auditoria_por_modulo.get("m5_2_composicao_cidades", 0) + total_m5_2_premanifestos
    auditoria_por_snapshot["m5_2_premanifestos"] = auditoria_por_snapshot.get("m5_2_premanifestos", 0) + total_m5_2_premanifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_2_premanifestos linhas={total_m5_2_premanifestos}")

    total_m5_2_itens_premanifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_2_composicao_cidades",
        ordem_modulo=8,
        df_etapa=df_itens_premanifestos_m5_2,
        snapshot_nome="m5_2_itens_premanifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_2_composicao_cidades"] = auditoria_por_modulo.get("m5_2_composicao_cidades", 0) + total_m5_2_itens_premanifestos
    auditoria_por_snapshot["m5_2_itens_premanifestos"] = auditoria_por_snapshot.get("m5_2_itens_premanifestos", 0) + total_m5_2_itens_premanifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_2_itens_premanifestos linhas={total_m5_2_itens_premanifestos}")

    total_m5_2_remanescente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_2_composicao_cidades",
        ordem_modulo=8,
        df_etapa=df_remanescente_m5_2,
        snapshot_nome="m5_2_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_2_composicao_cidades"] = auditoria_por_modulo.get("m5_2_composicao_cidades", 0) + total_m5_2_remanescente
    auditoria_por_snapshot["m5_2_remanescente"] = auditoria_por_snapshot.get("m5_2_remanescente", 0) + total_m5_2_remanescente
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_2_remanescente linhas={total_m5_2_remanescente}")

    total_m5_2_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_2_composicao_cidades",
        ordem_modulo=8,
        df_etapa=df_tentativas_m5_2,
        snapshot_nome="m5_2_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_2_composicao_cidades"] = auditoria_por_modulo.get("m5_2_composicao_cidades", 0) + total_m5_2_tentativas
    auditoria_por_snapshot["m5_2_tentativas"] = auditoria_por_snapshot.get("m5_2_tentativas", 0) + total_m5_2_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_2_tentativas linhas={total_m5_2_tentativas}")

    if not PIPELINE_FLAGS["executar_m5_3a"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.2 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.2",
            "modo_resposta": "auditoria_m5_2_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "logs": logs,
        }

    # =========================================================================================
    # M5.3A
    # =========================================================================================
    df_remanescente_global_ate_m5_2 = _consolidar_remanescente_global(
        df_saldo_nao_elegivel_m5_1,
        df_remanescente_m5_2,
    )
    t0 = _agora()
    _print_log(
        "[M5.3A] executando M5.3A com remanescente global ate M5.2 "
        f"linhas={_safe_len(df_remanescente_global_ate_m5_2)}"
    )
    outputs_m5_3a, meta_m5_3a = executar_m5_3_triagem_subregioes(
        df_remanescente_m5_2=df_remanescente_global_ate_m5_2,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_3a = _duracao_ms(t0)
    metricas_tempo["m5_3_triagem_subregioes_ms"] = tempo_m5_3a

    resumo_m5_3a = meta_m5_3a["resumo_m5_3"]

    df_subregioes_consolidadas_m5_3 = outputs_m5_3a["df_subregioes_consolidadas_m5_3"]
    df_perfis_viaveis_por_subregiao_m5_3 = outputs_m5_3a["df_perfis_viaveis_por_subregiao_m5_3"]
    df_perfis_elegiveis_por_subregiao_m5_3 = outputs_m5_3a["df_perfis_elegiveis_por_subregiao_m5_3"]
    df_perfis_descartados_por_subregiao_m5_3 = outputs_m5_3a["df_perfis_descartados_por_subregiao_m5_3"]
    df_saldo_elegivel_composicao_m5_3 = outputs_m5_3a["df_saldo_elegivel_composicao_m5_3"]
    df_saldo_nao_elegivel_m5_3 = outputs_m5_3a["df_saldo_nao_elegivel_m5_3"]
    df_tentativas_triagem_subregioes_m5_3 = outputs_m5_3a["df_tentativas_triagem_subregioes_m5_3"]
    _print_log(f"[M5.3A] df_subregioes_consolidadas_m5_3 linhas={_safe_len(df_subregioes_consolidadas_m5_3)}")
    _print_log(f"[M5.3A] df_perfis_elegiveis_por_subregiao_m5_3 linhas={_safe_len(df_perfis_elegiveis_por_subregiao_m5_3)}")
    _print_log(f"[M5.3A] df_perfis_descartados_por_subregiao_m5_3 linhas={_safe_len(df_perfis_descartados_por_subregiao_m5_3)}")
    _print_log(f"[M5.3A] df_saldo_elegivel_composicao_m5_3 linhas={_safe_len(df_saldo_elegivel_composicao_m5_3)}")
    _print_log(f"[M5.3A] df_saldo_nao_elegivel_m5_3 linhas={_safe_len(df_saldo_nao_elegivel_m5_3)}")
    _print_log(f"[M5.3A] df_tentativas_triagem_subregioes_m5_3 linhas={_safe_len(df_tentativas_triagem_subregioes_m5_3)}")

    logs.append(
        _log(
            modulo="m5_3_triagem_subregioes",
            status="ok",
            mensagem="M5.3A executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_global_ate_m5_2),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_3),
            tempo_ms=tempo_m5_3a,
            extra={
                **resumo_m5_3a,
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_remanescente_global_ate_m5_2": _safe_len(df_remanescente_global_ate_m5_2),
                "total_subregioes_consolidadas_m5_3": _safe_len(df_subregioes_consolidadas_m5_3),
                "total_tentativas_triagem_subregioes_m5_3": _safe_len(df_tentativas_triagem_subregioes_m5_3),
            },
        )
    )
    total_m5_3a_subregioes_consolidadas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_subregioes_consolidadas_m5_3,
        snapshot_nome="m5_3a_subregioes_consolidadas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_subregioes_consolidadas
    auditoria_por_snapshot["m5_3a_subregioes_consolidadas"] = auditoria_por_snapshot.get("m5_3a_subregioes_consolidadas", 0) + total_m5_3a_subregioes_consolidadas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_subregioes_consolidadas linhas={total_m5_3a_subregioes_consolidadas}")

    total_m5_3a_perfis_viaveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_perfis_viaveis_por_subregiao_m5_3,
        snapshot_nome="m5_3a_perfis_viaveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_perfis_viaveis
    auditoria_por_snapshot["m5_3a_perfis_viaveis"] = auditoria_por_snapshot.get("m5_3a_perfis_viaveis", 0) + total_m5_3a_perfis_viaveis
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_perfis_viaveis linhas={total_m5_3a_perfis_viaveis}")

    total_m5_3a_perfis_elegiveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_perfis_elegiveis_por_subregiao_m5_3,
        snapshot_nome="m5_3a_perfis_elegiveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_perfis_elegiveis
    auditoria_por_snapshot["m5_3a_perfis_elegiveis"] = auditoria_por_snapshot.get("m5_3a_perfis_elegiveis", 0) + total_m5_3a_perfis_elegiveis
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_perfis_elegiveis linhas={total_m5_3a_perfis_elegiveis}")

    total_m5_3a_perfis_descartados = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_perfis_descartados_por_subregiao_m5_3,
        snapshot_nome="m5_3a_perfis_descartados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_perfis_descartados
    auditoria_por_snapshot["m5_3a_perfis_descartados"] = auditoria_por_snapshot.get("m5_3a_perfis_descartados", 0) + total_m5_3a_perfis_descartados
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_perfis_descartados linhas={total_m5_3a_perfis_descartados}")

    total_m5_3a_saldo_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_saldo_elegivel_composicao_m5_3,
        snapshot_nome="m5_3a_saldo_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_saldo_elegivel
    auditoria_por_snapshot["m5_3a_saldo_elegivel"] = auditoria_por_snapshot.get("m5_3a_saldo_elegivel", 0) + total_m5_3a_saldo_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_saldo_elegivel linhas={total_m5_3a_saldo_elegivel}")

    total_m5_3a_saldo_nao_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_saldo_nao_elegivel_m5_3,
        snapshot_nome="m5_3a_saldo_nao_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_saldo_nao_elegivel
    auditoria_por_snapshot["m5_3a_saldo_nao_elegivel"] = auditoria_por_snapshot.get("m5_3a_saldo_nao_elegivel", 0) + total_m5_3a_saldo_nao_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_saldo_nao_elegivel linhas={total_m5_3a_saldo_nao_elegivel}")

    total_m5_3a_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3a_triagem_subregioes",
        ordem_modulo=9,
        df_etapa=df_tentativas_triagem_subregioes_m5_3,
        snapshot_nome="m5_3a_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3a_triagem_subregioes"] = auditoria_por_modulo.get("m5_3a_triagem_subregioes", 0) + total_m5_3a_tentativas
    auditoria_por_snapshot["m5_3a_tentativas"] = auditoria_por_snapshot.get("m5_3a_tentativas", 0) + total_m5_3a_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3a_tentativas linhas={total_m5_3a_tentativas}")

    if not PIPELINE_FLAGS["executar_m5_3b"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.3A para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.3A",
            "modo_resposta": "auditoria_m5_3a_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_remanescente_global_ate_m5_2": _safe_len(df_remanescente_global_ate_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "linhas_entrada_m5_3": resumo_m5_3a.get("linhas_entrada", 0),
                "subregioes_total_m5_3": resumo_m5_3a.get("subregioes_total", 0),
                "subregioes_elegiveis_m5_3": resumo_m5_3a.get("subregioes_elegiveis", 0),
                "subregioes_nao_elegiveis_m5_3": resumo_m5_3a.get("subregioes_nao_elegiveis", 0),
                "perfis_testados_total_m5_3": resumo_m5_3a.get("perfis_testados_total", 0),
                "perfis_elegiveis_total_m5_3": resumo_m5_3a.get("perfis_elegiveis_total", 0),
                "perfis_descartados_total_m5_3": resumo_m5_3a.get("perfis_descartados_total", 0),
                "linhas_saldo_elegivel_composicao_m5_3": resumo_m5_3a.get("linhas_saldo_elegivel_composicao_m5_3", 0),
                "linhas_saldo_nao_elegivel_m5_3": resumo_m5_3a.get("linhas_saldo_nao_elegivel_m5_3", 0),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3": resumo_m5_3a,
            "logs": logs,
        }

    # =========================================================================================
    # M5.3B
    # =========================================================================================
    t0 = _agora()
    m5_3b_habilitado = PIPELINE_FLAGS["executar_m5_3b"]
    m5_3b_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_3, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_3.empty
    m5_3b_tem_perfis = isinstance(df_perfis_elegiveis_por_subregiao_m5_3, pd.DataFrame) and not df_perfis_elegiveis_por_subregiao_m5_3.empty
    _print_log(f"[M5.3B] executando M5.3B com saldo elegivel linhas={_safe_len(df_saldo_elegivel_composicao_m5_3)}")
    if m5_3b_habilitado and m5_3b_tem_saldo and m5_3b_tem_perfis:
        outputs_m5_3b, meta_m5_3b = executar_m5_3_composicao_subregioes(
            df_saldo_elegivel_composicao_m5_3=df_saldo_elegivel_composicao_m5_3,
            df_perfis_elegiveis_por_subregiao_m5_3=df_perfis_elegiveis_por_subregiao_m5_3,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        if not m5_3b_habilitado:
            motivo_pulo = "m5_3b_desligado_por_flag"
        else:
            motivo_pulo = "sem_saldo_elegivel_m5_3b" if not m5_3b_tem_saldo else "sem_perfis_elegiveis_m5_3b"
        outputs_m5_3b = {
            "df_premanifestos_m5_3": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_3": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_3).columns.tolist()),
            "df_tentativas_m5_3": pd.DataFrame(columns=["resultado", "motivo"]),
            "df_remanescente_m5_3": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_3),
            "df_pool_subregiao_m5_3": pd.DataFrame(),
            "df_blocos_cliente_subregiao_m5_3": pd.DataFrame(),
            "df_manifestos_m5_3": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_manifestos_m5_3": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_3).columns.tolist()),
        }
        meta_m5_3b = {"resumo_m5_3b": {"modulo": "M5.3B", "etapa_pulada": True, "motivo_etapa_pulada": motivo_pulo, "linhas_entrada_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3), "linhas_saida_m5_3": 0, "remanescente_preservado_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3)}}
    tempo_m5_3b = _duracao_ms(t0)
    metricas_tempo["m5_3b_composicao_subregioes_ms"] = tempo_m5_3b

    resumo_m5_3b = meta_m5_3b["resumo_m5_3b"]
    df_premanifestos_m5_3 = outputs_m5_3b["df_premanifestos_m5_3"]
    df_itens_premanifestos_m5_3 = outputs_m5_3b["df_itens_premanifestos_m5_3"]
    df_tentativas_m5_3 = outputs_m5_3b["df_tentativas_m5_3"]
    df_remanescente_m5_3 = outputs_m5_3b["df_remanescente_m5_3"]
    df_pool_subregiao_m5_3 = outputs_m5_3b.get("df_pool_subregiao_m5_3", pd.DataFrame())
    df_blocos_cliente_subregiao_m5_3 = outputs_m5_3b.get("df_blocos_cliente_subregiao_m5_3", pd.DataFrame())
    df_manifestos_m5_3 = outputs_m5_3b.get("df_manifestos_m5_3", df_premanifestos_m5_3)
    df_itens_manifestos_m5_3 = outputs_m5_3b.get("df_itens_manifestos_m5_3", df_itens_premanifestos_m5_3)
    _print_log(f"[M5.3B] df_premanifestos_m5_3 linhas={_safe_len(df_premanifestos_m5_3)}")
    _print_log(f"[M5.3B] df_itens_premanifestos_m5_3 linhas={_safe_len(df_itens_premanifestos_m5_3)}")
    _print_log(f"[M5.3B] df_remanescente_m5_3 linhas={_safe_len(df_remanescente_m5_3)}")
    _print_log(f"[M5.3B] df_tentativas_m5_3 linhas={_safe_len(df_tentativas_m5_3)}")
    _print_log(f"[M5.3] df_pool_subregiao_m5_3 linhas={_safe_len(df_pool_subregiao_m5_3)}")
    _print_log(f"[M5.3] df_blocos_cliente_subregiao_m5_3 linhas={_safe_len(df_blocos_cliente_subregiao_m5_3)}")
    _print_log(f"[M5.3] df_tentativas_m5_3 linhas={_safe_len(df_tentativas_m5_3)}")
    _print_log(f"[M5.3] df_manifestos_m5_3 linhas={_safe_len(df_manifestos_m5_3)}")
    _print_log(f"[M5.3] df_itens_manifestos_m5_3 linhas={_safe_len(df_itens_manifestos_m5_3)}")
    _print_log(f"[M5.3] df_remanescente_m5_3 linhas={_safe_len(df_remanescente_m5_3)}")

    if PIPELINE_FLAGS.get("executar_validadores_exaustao") and PIPELINE_FLAGS.get("executar_m5_3_repescagem"):
        df_m5_3_repescagem_input, meta_m5_3_1 = _filtrar_remanescentes_com_potencial(
            df_remanescente_m5_3,
            chaves_grupo=[
                "subregiao",
                "sub_regiao",
                "Sub-Região",
                "mesorregiao",
                "Mesoregião",
                "uf",
                "UF",
            ],
            df_veiculos_tratados=df_veiculos_tratados,
            nome_etapa="M5.3.1",
        )
        if isinstance(df_m5_3_repescagem_input, pd.DataFrame) and not df_m5_3_repescagem_input.empty:
            df_perfis_m5_3_repescagem = _copiar_ou_vazio(df_perfis_elegiveis_por_subregiao_m5_3)
            col_sub_input = _primeira_coluna_existente(df_m5_3_repescagem_input, ["subregiao", "sub_regiao", "Sub-Região"])
            col_sub_perfil = _primeira_coluna_existente(df_perfis_m5_3_repescagem, ["subregiao", "sub_regiao", "Sub-Região"])
            if col_sub_input and col_sub_perfil and not df_perfis_m5_3_repescagem.empty:
                subregioes_alvo = set(df_m5_3_repescagem_input[col_sub_input].dropna().astype(str))
                df_perfis_m5_3_repescagem = df_perfis_m5_3_repescagem[
                    df_perfis_m5_3_repescagem[col_sub_perfil].astype(str).isin(subregioes_alvo)
                ].copy()
            outputs_m5_3_1, _ = executar_m5_3_composicao_subregioes(
                df_saldo_elegivel_composicao_m5_3=df_m5_3_repescagem_input,
                df_perfis_elegiveis_por_subregiao_m5_3=df_perfis_m5_3_repescagem,
                rodada_id=contexto.rodada_id,
                data_base_roteirizacao=contexto.data_base,
                tipo_roteirizacao=contexto.tipo_roteirizacao,
                caminhos_pipeline=contexto.caminhos_pipeline,
            )
            df_premanifestos_m5_3_1 = _copiar_ou_vazio(outputs_m5_3_1.get("df_premanifestos_m5_3"))
            df_itens_premanifestos_m5_3_1 = _copiar_ou_vazio(outputs_m5_3_1.get("df_itens_premanifestos_m5_3"))
            df_manifestos_m5_3_1 = _copiar_ou_vazio(outputs_m5_3_1.get("df_manifestos_m5_3"))
            df_itens_manifestos_m5_3_1 = _copiar_ou_vazio(outputs_m5_3_1.get("df_itens_manifestos_m5_3"))
            df_premanifestos_m5_3_1, df_itens_premanifestos_m5_3_1 = _renomear_manifestos_repescagem(
                df_premanifestos_m5_3_1,
                df_itens_premanifestos_m5_3_1,
                prefixo="PM531_",
            )
            df_manifestos_m5_3_1, df_itens_manifestos_m5_3_1 = _renomear_manifestos_repescagem(
                df_manifestos_m5_3_1,
                df_itens_manifestos_m5_3_1,
                prefixo="PM531_",
            )
            df_premanifestos_m5_3 = pd.concat([_copiar_ou_vazio(df_premanifestos_m5_3), df_premanifestos_m5_3_1], ignore_index=True, sort=False)
            df_itens_premanifestos_m5_3 = pd.concat(
                [_copiar_ou_vazio(df_itens_premanifestos_m5_3), df_itens_premanifestos_m5_3_1],
                ignore_index=True,
                sort=False,
            )
            if isinstance(df_manifestos_m5_3_1, pd.DataFrame) and not df_manifestos_m5_3_1.empty:
                df_manifestos_m5_3 = pd.concat([_copiar_ou_vazio(df_manifestos_m5_3), df_manifestos_m5_3_1], ignore_index=True, sort=False)
            if isinstance(df_itens_manifestos_m5_3_1, pd.DataFrame) and not df_itens_manifestos_m5_3_1.empty:
                df_itens_manifestos_m5_3 = pd.concat(
                    [_copiar_ou_vazio(df_itens_manifestos_m5_3), df_itens_manifestos_m5_3_1],
                    ignore_index=True,
                    sort=False,
                )
            df_remanescente_m5_3 = _remover_itens_alocados_do_remanescente(df_remanescente_m5_3, df_itens_premanifestos_m5_3_1)
            auditoria_reprocessamento_etapas["m5_3_1"] = {
                **meta_m5_3_1,
                "manifestos_adicionais": _safe_len(df_premanifestos_m5_3_1),
                "itens_recuperados": _safe_len(df_itens_premanifestos_m5_3_1),
                "remanescente_apos": _safe_len(df_remanescente_m5_3),
            }
        else:
            auditoria_reprocessamento_etapas["m5_3_1"] = {
                **meta_m5_3_1,
                "manifestos_adicionais": 0,
                "itens_recuperados": 0,
                "remanescente_apos": _safe_len(df_remanescente_m5_3),
            }
    else:
        auditoria_reprocessamento_etapas["m5_3_1"] = {
            "ativado": False,
            "motivo": "desligado_por_flag",
            "manifestos_adicionais": 0,
            "itens_recuperados": 0,
            "remanescente_apos": _safe_len(df_remanescente_m5_3),
        }

    logs.append(
        _log(
            modulo="m5_3b_composicao_subregioes",
            status="ok" if m5_3b_habilitado and m5_3b_tem_saldo and m5_3b_tem_perfis else "ignorado",
            mensagem="M5.3B executado com sucesso" if m5_3b_habilitado and m5_3b_tem_saldo and m5_3b_tem_perfis else "M5.3B pulado por ausência de entrada válida",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_3),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_3),
            tempo_ms=tempo_m5_3b,
            extra={
                **resumo_m5_3b,
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
            },
        )
    )
    total_m5_3_pool_subregiao = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_pool_subregiao_m5_3,
        snapshot_nome="m5_3_pool_subregiao",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_pool_subregiao
    auditoria_por_snapshot["m5_3_pool_subregiao"] = auditoria_por_snapshot.get("m5_3_pool_subregiao", 0) + total_m5_3_pool_subregiao
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_pool_subregiao linhas={total_m5_3_pool_subregiao}")

    total_m5_3_blocos_cliente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_blocos_cliente_subregiao_m5_3,
        snapshot_nome="m5_3_blocos_cliente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_blocos_cliente
    auditoria_por_snapshot["m5_3_blocos_cliente"] = auditoria_por_snapshot.get("m5_3_blocos_cliente", 0) + total_m5_3_blocos_cliente
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_blocos_cliente linhas={total_m5_3_blocos_cliente}")

    total_m5_3_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_tentativas_m5_3,
        snapshot_nome="m5_3_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_tentativas
    auditoria_por_snapshot["m5_3_tentativas"] = auditoria_por_snapshot.get("m5_3_tentativas", 0) + total_m5_3_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_tentativas linhas={total_m5_3_tentativas}")

    total_m5_3_manifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_manifestos_m5_3,
        snapshot_nome="m5_3_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_manifestos
    auditoria_por_snapshot["m5_3_manifestos"] = auditoria_por_snapshot.get("m5_3_manifestos", 0) + total_m5_3_manifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_manifestos linhas={total_m5_3_manifestos}")

    total_m5_3_itens_manifestados = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_itens_manifestos_m5_3,
        snapshot_nome="m5_3_itens_manifestados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_itens_manifestados
    auditoria_por_snapshot["m5_3_itens_manifestados"] = auditoria_por_snapshot.get("m5_3_itens_manifestados", 0) + total_m5_3_itens_manifestados
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_itens_manifestados linhas={total_m5_3_itens_manifestados}")

    total_m5_3_remanescente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_3b_composicao_subregioes",
        ordem_modulo=10,
        df_etapa=df_remanescente_m5_3,
        snapshot_nome="m5_3_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_3b_composicao_subregioes"] = auditoria_por_modulo.get("m5_3b_composicao_subregioes", 0) + total_m5_3_remanescente
    auditoria_por_snapshot["m5_3_remanescente"] = auditoria_por_snapshot.get("m5_3_remanescente", 0) + total_m5_3_remanescente
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_3_remanescente linhas={total_m5_3_remanescente}")

    df_remanescente_global_ate_m5_3 = _consolidar_remanescente_global(
        df_saldo_nao_elegivel_m5_3,
        df_remanescente_m5_3,
    )
    _print_log(f"[M5.3B] df_remanescente_global_ate_m5_3 linhas={_safe_len(df_remanescente_global_ate_m5_3)}")

    if not PIPELINE_FLAGS["executar_m5_4a"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.3B para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.3B",
            "modo_resposta": "auditoria_m5_3b_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_subregioes_m5_3a": _safe_len(df_subregioes_consolidadas_m5_3),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_itens_premanifestados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "logs": logs,
        }

    # =========================================================================================
    # M5.4A
    # =========================================================================================
    t0 = _agora()
    _print_log(
        "[M5.4A] executando M5.4A com remanescente global ate M5.3 "
        f"linhas={_safe_len(df_remanescente_global_ate_m5_3)}"
    )
    outputs_m5_4a, meta_m5_4a = executar_m5_4a_triagem_mesorregioes(
        df_remanescente_m5_3=df_remanescente_global_ate_m5_3,
        df_veiculos_tratados=df_veiculos_tratados,
    )
    tempo_m5_4a = _duracao_ms(t0)
    metricas_tempo["m5_4a_triagem_mesorregioes_ms"] = tempo_m5_4a

    resumo_m5_4a = meta_m5_4a["resumo_m5_4a"]

    df_mesorregioes_consolidadas_m5_4 = outputs_m5_4a["df_mesorregioes_consolidadas_m5_4"]
    df_perfis_viaveis_por_mesorregiao_m5_4 = outputs_m5_4a["df_perfis_viaveis_por_mesorregiao_m5_4"]
    df_perfis_elegiveis_por_mesorregiao_m5_4 = outputs_m5_4a["df_perfis_elegiveis_por_mesorregiao_m5_4"]
    df_perfis_descartados_por_mesorregiao_m5_4 = outputs_m5_4a["df_perfis_descartados_por_mesorregiao_m5_4"]
    df_saldo_elegivel_composicao_m5_4 = outputs_m5_4a["df_saldo_elegivel_composicao_m5_4"]
    df_saldo_nao_elegivel_m5_4 = outputs_m5_4a["df_saldo_nao_elegivel_m5_4"]
    df_tentativas_triagem_mesorregioes_m5_4 = outputs_m5_4a["df_tentativas_triagem_mesorregioes_m5_4"]

    _print_log(f"[M5.4A] df_mesorregioes_consolidadas_m5_4 linhas={_safe_len(df_mesorregioes_consolidadas_m5_4)}")
    _print_log(f"[M5.4A] df_perfis_elegiveis_por_mesorregiao_m5_4 linhas={_safe_len(df_perfis_elegiveis_por_mesorregiao_m5_4)}")
    _print_log(f"[M5.4A] df_perfis_descartados_por_mesorregiao_m5_4 linhas={_safe_len(df_perfis_descartados_por_mesorregiao_m5_4)}")
    _print_log(f"[M5.4A] df_saldo_elegivel_composicao_m5_4 linhas={_safe_len(df_saldo_elegivel_composicao_m5_4)}")
    _print_log(f"[M5.4A] df_saldo_nao_elegivel_m5_4 linhas={_safe_len(df_saldo_nao_elegivel_m5_4)}")
    _print_log(f"[M5.4A] df_tentativas_triagem_mesorregioes_m5_4 linhas={_safe_len(df_tentativas_triagem_mesorregioes_m5_4)}")

    logs.append(
        _log(
            modulo="m5_4a_triagem_mesorregioes",
            status="ok",
            mensagem="M5.4A executado com sucesso",
            quantidade_entrada=_safe_len(df_remanescente_global_ate_m5_3),
            quantidade_saida=_safe_len(df_saldo_elegivel_composicao_m5_4),
            tempo_ms=tempo_m5_4a,
            extra={
                **resumo_m5_4a,
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
                "total_mesorregioes_m5_4a": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_saldo_elegivel_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4),
                "total_saldo_nao_elegivel_m5_4": _safe_len(df_saldo_nao_elegivel_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
            },
        )
    )

    total_m5_4a_mesorregioes_consolidadas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_mesorregioes_consolidadas_m5_4,
        snapshot_nome="m5_4a_mesorregioes_consolidadas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_mesorregioes_consolidadas
    auditoria_por_snapshot["m5_4a_mesorregioes_consolidadas"] = auditoria_por_snapshot.get("m5_4a_mesorregioes_consolidadas", 0) + total_m5_4a_mesorregioes_consolidadas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_mesorregioes_consolidadas linhas={total_m5_4a_mesorregioes_consolidadas}")

    total_m5_4a_perfis_viaveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_perfis_viaveis_por_mesorregiao_m5_4,
        snapshot_nome="m5_4a_perfis_viaveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_perfis_viaveis
    auditoria_por_snapshot["m5_4a_perfis_viaveis"] = auditoria_por_snapshot.get("m5_4a_perfis_viaveis", 0) + total_m5_4a_perfis_viaveis
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_perfis_viaveis linhas={total_m5_4a_perfis_viaveis}")

    total_m5_4a_perfis_elegiveis = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_perfis_elegiveis_por_mesorregiao_m5_4,
        snapshot_nome="m5_4a_perfis_elegiveis",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_perfis_elegiveis
    auditoria_por_snapshot["m5_4a_perfis_elegiveis"] = auditoria_por_snapshot.get("m5_4a_perfis_elegiveis", 0) + total_m5_4a_perfis_elegiveis
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_perfis_elegiveis linhas={total_m5_4a_perfis_elegiveis}")

    total_m5_4a_perfis_descartados = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_perfis_descartados_por_mesorregiao_m5_4,
        snapshot_nome="m5_4a_perfis_descartados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_perfis_descartados
    auditoria_por_snapshot["m5_4a_perfis_descartados"] = auditoria_por_snapshot.get("m5_4a_perfis_descartados", 0) + total_m5_4a_perfis_descartados
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_perfis_descartados linhas={total_m5_4a_perfis_descartados}")

    total_m5_4a_saldo_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_saldo_elegivel_composicao_m5_4,
        snapshot_nome="m5_4a_saldo_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_saldo_elegivel
    auditoria_por_snapshot["m5_4a_saldo_elegivel"] = auditoria_por_snapshot.get("m5_4a_saldo_elegivel", 0) + total_m5_4a_saldo_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_saldo_elegivel linhas={total_m5_4a_saldo_elegivel}")

    total_m5_4a_saldo_nao_elegivel = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_saldo_nao_elegivel_m5_4,
        snapshot_nome="m5_4a_saldo_nao_elegivel",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_saldo_nao_elegivel
    auditoria_por_snapshot["m5_4a_saldo_nao_elegivel"] = auditoria_por_snapshot.get("m5_4a_saldo_nao_elegivel", 0) + total_m5_4a_saldo_nao_elegivel
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_saldo_nao_elegivel linhas={total_m5_4a_saldo_nao_elegivel}")

    total_m5_4a_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4a_triagem_mesorregioes",
        ordem_modulo=11,
        df_etapa=df_tentativas_triagem_mesorregioes_m5_4,
        snapshot_nome="m5_4a_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4a_triagem_mesorregioes"] = auditoria_por_modulo.get("m5_4a_triagem_mesorregioes", 0) + total_m5_4a_tentativas
    auditoria_por_snapshot["m5_4a_tentativas"] = auditoria_por_snapshot.get("m5_4a_tentativas", 0) + total_m5_4a_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4a_tentativas linhas={total_m5_4a_tentativas}")

    if not PIPELINE_FLAGS["executar_m5_4b"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execucao encerrada propositalmente apos o M5.4A para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.4A",
            "modo_resposta": "auditoria_m5_4a_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_itens_premanifestados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
                "total_mesorregioes_m5_4a": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_saldo_elegivel_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4),
                "total_saldo_nao_elegivel_m5_4": _safe_len(df_saldo_nao_elegivel_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "resumo_m5_4a": resumo_m5_4a,
            "logs": logs,
        }

    # =========================================================================================
    # M5.4B
    # =========================================================================================
    t0 = _agora()
    _print_log(f"[M5.4B] executando M5.4B com saldo elegível linhas={_safe_len(df_saldo_elegivel_composicao_m5_4)}")
    m5_4b_tem_saldo = isinstance(df_saldo_elegivel_composicao_m5_4, pd.DataFrame) and not df_saldo_elegivel_composicao_m5_4.empty
    m5_4b_tem_perfis = isinstance(df_perfis_elegiveis_por_mesorregiao_m5_4, pd.DataFrame) and not df_perfis_elegiveis_por_mesorregiao_m5_4.empty
    if m5_4b_tem_saldo and m5_4b_tem_perfis:
        outputs_m5_4b, meta_m5_4b = executar_m5_4b_composicao_mesorregioes(
            df_saldo_elegivel_composicao_m5_4=df_saldo_elegivel_composicao_m5_4,
            df_perfis_elegiveis_por_mesorregiao_m5_4=df_perfis_elegiveis_por_mesorregiao_m5_4,
            rodada_id=contexto.rodada_id,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
        )
    else:
        motivo_pulo = "sem_saldo_elegivel_m5_4b" if not m5_4b_tem_saldo else "sem_perfis_elegiveis_m5_4b"
        outputs_m5_4b = {
            "df_premanifestos_m5_4": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_premanifestos_m5_4": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4).columns.tolist()),
            "df_tentativas_m5_4": pd.DataFrame(columns=["resultado", "motivo"]),
            "df_remanescente_m5_4": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4),
            "df_pool_mesorregiao_m5_4": _copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4),
            "df_blocos_cliente_mesorregiao_m5_4": pd.DataFrame(),
            "df_manifestos_m5_4": pd.DataFrame(columns=["manifesto_id", "tipo_manifesto"]),
            "df_itens_manifestos_m5_4": pd.DataFrame(columns=_copiar_ou_vazio(df_saldo_elegivel_composicao_m5_4).columns.tolist()),
        }
        meta_m5_4b = {"resumo_m5_4b": {"modulo": "M5.4B", "etapa_pulada": True, "motivo_etapa_pulada": motivo_pulo, "linhas_entrada_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4), "linhas_saida_m5_4": 0, "remanescente_preservado_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4)}}
    tempo_m5_4b = _duracao_ms(t0)
    metricas_tempo["m5_4b_composicao_mesorregioes_ms"] = tempo_m5_4b

    resumo_m5_4b = meta_m5_4b["resumo_m5_4b"]
    df_premanifestos_m5_4 = outputs_m5_4b["df_premanifestos_m5_4"]
    df_itens_premanifestos_m5_4 = outputs_m5_4b["df_itens_premanifestos_m5_4"]
    df_tentativas_m5_4 = outputs_m5_4b["df_tentativas_m5_4"]
    df_remanescente_m5_4 = outputs_m5_4b["df_remanescente_m5_4"]
    df_pool_mesorregiao_m5_4 = outputs_m5_4b.get("df_pool_mesorregiao_m5_4", pd.DataFrame())
    df_blocos_cliente_mesorregiao_m5_4 = outputs_m5_4b.get("df_blocos_cliente_mesorregiao_m5_4", pd.DataFrame())
    df_manifestos_m5_4 = outputs_m5_4b.get("df_manifestos_m5_4", df_premanifestos_m5_4)
    df_itens_manifestos_m5_4 = outputs_m5_4b.get("df_itens_manifestos_m5_4", df_itens_premanifestos_m5_4)
    if isinstance(df_premanifestos_m5_4, pd.DataFrame) and ("manifesto_id" in df_premanifestos_m5_4.columns):
        df_premanifestos_m5_4["manifesto_id"] = (
            df_premanifestos_m5_4["manifesto_id"].astype(str).str.replace("PM53_", "PM54_", regex=False)
        )
    if isinstance(df_itens_premanifestos_m5_4, pd.DataFrame) and ("manifesto_id" in df_itens_premanifestos_m5_4.columns):
        df_itens_premanifestos_m5_4["manifesto_id"] = (
            df_itens_premanifestos_m5_4["manifesto_id"].astype(str).str.replace("PM53_", "PM54_", regex=False)
        )

    _print_log(f"[M5.4] df_pool_mesorregiao_m5_4 linhas={_safe_len(df_pool_mesorregiao_m5_4)}")
    _print_log(f"[M5.4] df_blocos_cliente_mesorregiao_m5_4 linhas={_safe_len(df_blocos_cliente_mesorregiao_m5_4)}")
    _print_log(f"[M5.4] df_tentativas_m5_4 linhas={_safe_len(df_tentativas_m5_4)}")
    _print_log(f"[M5.4] df_manifestos_m5_4 linhas={_safe_len(df_manifestos_m5_4)}")
    _print_log(f"[M5.4] df_itens_manifestos_m5_4 linhas={_safe_len(df_itens_manifestos_m5_4)}")
    _print_log(f"[M5.4] df_remanescente_m5_4 linhas={_safe_len(df_remanescente_m5_4)}")

    if PIPELINE_FLAGS.get("executar_validadores_exaustao") and PIPELINE_FLAGS.get("executar_m5_4_repescagem"):
        chaves_m5_4 = ["mesorregiao", "Mesoregião", "corredor_30g", "uf", "UF"]
        col_meso_m5_4 = _primeira_coluna_existente(df_remanescente_m5_4, ["mesorregiao", "Mesoregião"])
        col_corredor_m5_4 = _primeira_coluna_existente(df_remanescente_m5_4, ["corredor_30g"])
        col_uf_m5_4 = _primeira_coluna_existente(df_remanescente_m5_4, ["uf", "UF"])
        if col_meso_m5_4 and col_corredor_m5_4:
            chaves_m5_4 = [col_meso_m5_4, col_corredor_m5_4]
            if col_uf_m5_4:
                chaves_m5_4.append(col_uf_m5_4)
        elif col_meso_m5_4 and col_uf_m5_4:
            chaves_m5_4 = [col_meso_m5_4, col_uf_m5_4]
        df_m5_4_repescagem_input, meta_m5_4_1 = _filtrar_remanescentes_com_potencial(
            df_remanescente_m5_4,
            chaves_grupo=chaves_m5_4,
            df_veiculos_tratados=df_veiculos_tratados,
            nome_etapa="M5.4.1",
        )
        if isinstance(df_m5_4_repescagem_input, pd.DataFrame) and not df_m5_4_repescagem_input.empty:
            df_perfis_m5_4_repescagem = _copiar_ou_vazio(df_perfis_elegiveis_por_mesorregiao_m5_4)
            col_meso_input = _primeira_coluna_existente(df_m5_4_repescagem_input, ["mesorregiao", "Mesoregião"])
            col_meso_perfil = _primeira_coluna_existente(df_perfis_m5_4_repescagem, ["mesorregiao", "Mesoregião"])
            col_corredor_input = _primeira_coluna_existente(df_m5_4_repescagem_input, ["corredor_30g"])
            col_corredor_perfil = _primeira_coluna_existente(df_perfis_m5_4_repescagem, ["corredor_30g"])
            if col_meso_input and col_meso_perfil and not df_perfis_m5_4_repescagem.empty:
                meso_alvo = set(df_m5_4_repescagem_input[col_meso_input].dropna().astype(str))
                filtro = df_perfis_m5_4_repescagem[col_meso_perfil].astype(str).isin(meso_alvo)
                if col_corredor_input and col_corredor_perfil:
                    corredor_alvo = set(df_m5_4_repescagem_input[col_corredor_input].dropna().astype(str))
                    filtro = filtro & df_perfis_m5_4_repescagem[col_corredor_perfil].astype(str).isin(corredor_alvo)
                df_perfis_m5_4_repescagem = df_perfis_m5_4_repescagem[filtro].copy()
            outputs_m5_4_1, _ = executar_m5_4b_composicao_mesorregioes(
                df_saldo_elegivel_composicao_m5_4=df_m5_4_repescagem_input,
                df_perfis_elegiveis_por_mesorregiao_m5_4=df_perfis_m5_4_repescagem,
                rodada_id=contexto.rodada_id,
                data_base_roteirizacao=contexto.data_base,
                tipo_roteirizacao=contexto.tipo_roteirizacao,
                caminhos_pipeline=contexto.caminhos_pipeline,
            )
            df_premanifestos_m5_4_1 = _copiar_ou_vazio(outputs_m5_4_1.get("df_premanifestos_m5_4"))
            df_itens_premanifestos_m5_4_1 = _copiar_ou_vazio(outputs_m5_4_1.get("df_itens_premanifestos_m5_4"))
            df_manifestos_m5_4_1 = _copiar_ou_vazio(outputs_m5_4_1.get("df_manifestos_m5_4"))
            df_itens_manifestos_m5_4_1 = _copiar_ou_vazio(outputs_m5_4_1.get("df_itens_manifestos_m5_4"))
            df_premanifestos_m5_4_1, df_itens_premanifestos_m5_4_1 = _renomear_manifestos_repescagem(
                df_premanifestos_m5_4_1,
                df_itens_premanifestos_m5_4_1,
                prefixo="PM541_",
            )
            df_manifestos_m5_4_1, df_itens_manifestos_m5_4_1 = _renomear_manifestos_repescagem(
                df_manifestos_m5_4_1,
                df_itens_manifestos_m5_4_1,
                prefixo="PM541_",
            )
            df_premanifestos_m5_4 = pd.concat([_copiar_ou_vazio(df_premanifestos_m5_4), df_premanifestos_m5_4_1], ignore_index=True, sort=False)
            df_itens_premanifestos_m5_4 = pd.concat(
                [_copiar_ou_vazio(df_itens_premanifestos_m5_4), df_itens_premanifestos_m5_4_1],
                ignore_index=True,
                sort=False,
            )
            if isinstance(df_manifestos_m5_4_1, pd.DataFrame) and not df_manifestos_m5_4_1.empty:
                df_manifestos_m5_4 = pd.concat([_copiar_ou_vazio(df_manifestos_m5_4), df_manifestos_m5_4_1], ignore_index=True, sort=False)
            if isinstance(df_itens_manifestos_m5_4_1, pd.DataFrame) and not df_itens_manifestos_m5_4_1.empty:
                df_itens_manifestos_m5_4 = pd.concat(
                    [_copiar_ou_vazio(df_itens_manifestos_m5_4), df_itens_manifestos_m5_4_1],
                    ignore_index=True,
                    sort=False,
                )
            df_remanescente_m5_4 = _remover_itens_alocados_do_remanescente(df_remanescente_m5_4, df_itens_premanifestos_m5_4_1)
            auditoria_reprocessamento_etapas["m5_4_1"] = {
                **meta_m5_4_1,
                "manifestos_adicionais": _safe_len(df_premanifestos_m5_4_1),
                "itens_recuperados": _safe_len(df_itens_premanifestos_m5_4_1),
                "remanescente_apos": _safe_len(df_remanescente_m5_4),
            }
        else:
            auditoria_reprocessamento_etapas["m5_4_1"] = {
                **meta_m5_4_1,
                "manifestos_adicionais": 0,
                "itens_recuperados": 0,
                "remanescente_apos": _safe_len(df_remanescente_m5_4),
            }
    else:
        auditoria_reprocessamento_etapas["m5_4_1"] = {
            "ativado": False,
            "motivo": "desligado_por_flag",
            "manifestos_adicionais": 0,
            "itens_recuperados": 0,
            "remanescente_apos": _safe_len(df_remanescente_m5_4),
        }

    df_remanescente_global_final_roteirizacao = _consolidar_remanescente_global(
        df_saldo_nao_elegivel_m5_4,
        df_remanescente_m5_4,
    )
    _print_log(
        "[M5.4B] df_remanescente_global_final_roteirizacao "
        f"linhas={_safe_len(df_remanescente_global_final_roteirizacao)}"
    )

    logs.append(
        _log(
            modulo="m5_4b_composicao_mesorregioes",
            status="ok" if m5_4b_tem_saldo and m5_4b_tem_perfis else "ignorado",
            mensagem="M5.4B executado com sucesso" if m5_4b_tem_saldo and m5_4b_tem_perfis else "M5.4B pulado por ausência de entrada válida",
            quantidade_entrada=_safe_len(df_saldo_elegivel_composicao_m5_4),
            quantidade_saida=_safe_len(df_itens_premanifestos_m5_4),
            tempo_ms=tempo_m5_4b,
            extra={
                **resumo_m5_4b,
                "total_remanescente_global_final_roteirizacao": _safe_len(df_remanescente_global_final_roteirizacao),
                "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_m5_4),
            },
        )
    )

    total_m5_4_pool_mesorregiao = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_pool_mesorregiao_m5_4,
        snapshot_nome="m5_4_pool_mesorregiao",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_pool_mesorregiao
    auditoria_por_snapshot["m5_4_pool_mesorregiao"] = auditoria_por_snapshot.get("m5_4_pool_mesorregiao", 0) + total_m5_4_pool_mesorregiao
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_pool_mesorregiao linhas={total_m5_4_pool_mesorregiao}")

    total_m5_4_blocos_cliente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_blocos_cliente_mesorregiao_m5_4,
        snapshot_nome="m5_4_blocos_cliente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_blocos_cliente
    auditoria_por_snapshot["m5_4_blocos_cliente"] = auditoria_por_snapshot.get("m5_4_blocos_cliente", 0) + total_m5_4_blocos_cliente
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_blocos_cliente linhas={total_m5_4_blocos_cliente}")

    total_m5_4_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_tentativas_m5_4,
        snapshot_nome="m5_4_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_tentativas
    auditoria_por_snapshot["m5_4_tentativas"] = auditoria_por_snapshot.get("m5_4_tentativas", 0) + total_m5_4_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_tentativas linhas={total_m5_4_tentativas}")

    total_m5_4_manifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_manifestos_m5_4,
        snapshot_nome="m5_4_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_manifestos
    auditoria_por_snapshot["m5_4_manifestos"] = auditoria_por_snapshot.get("m5_4_manifestos", 0) + total_m5_4_manifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_manifestos linhas={total_m5_4_manifestos}")

    total_m5_4_itens_manifestados = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_itens_manifestos_m5_4,
        snapshot_nome="m5_4_itens_manifestados",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_itens_manifestados
    auditoria_por_snapshot["m5_4_itens_manifestados"] = auditoria_por_snapshot.get("m5_4_itens_manifestados", 0) + total_m5_4_itens_manifestados
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_itens_manifestados linhas={total_m5_4_itens_manifestados}")

    total_m5_4_remanescente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m5_4b_composicao_mesorregioes",
        ordem_modulo=12,
        df_etapa=df_remanescente_m5_4,
        snapshot_nome="m5_4_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m5_4b_composicao_mesorregioes"] = auditoria_por_modulo.get("m5_4b_composicao_mesorregioes", 0) + total_m5_4_remanescente
    auditoria_por_snapshot["m5_4_remanescente"] = auditoria_por_snapshot.get("m5_4_remanescente", 0) + total_m5_4_remanescente
    _print_log(f"[AUDITORIA FLAT] snapshot=m5_4_remanescente linhas={total_m5_4_remanescente}")

    if not PIPELINE_FLAGS["executar_m6_1"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M5.4B para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M5.4B",
            "modo_resposta": "auditoria_m5_4b_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_itens_premanifestados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
                "total_mesorregioes_m5_4a": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_saldo_elegivel_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4),
                "total_saldo_nao_elegivel_m5_4": _safe_len(df_saldo_nao_elegivel_m5_4),
                "total_tentativas_m5_4a": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
                "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
                "total_itens_premanifestados_m5_4": _safe_len(df_itens_premanifestos_m5_4),
                "total_remanescente_m5_4": _safe_len(df_remanescente_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_m5_4),
                "total_remanescente_global_final_roteirizacao": _safe_len(df_remanescente_global_final_roteirizacao),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "resumo_m5_4a": resumo_m5_4a,
            "resumo_m5_4b": resumo_m5_4b,
            "logs": logs,
        }

    # =========================================================================================
    # M6.1
    # =========================================================================================
    df_manifestos_m4_m6_1 = _copiar_ou_vazio(df_manifestos_m4)
    df_itens_manifestados_m4_m6_1 = _copiar_ou_vazio(df_itens_manifestados_m4)
    df_premanifestos_m5_2_m6_1 = _copiar_ou_vazio(df_premanifestos_m5_2)
    df_itens_premanifestos_m5_2_m6_1 = _copiar_ou_vazio(df_itens_premanifestos_m5_2)
    df_premanifestos_m5_3_m6_1 = _copiar_ou_vazio(df_premanifestos_m5_3)
    df_itens_premanifestos_m5_3_m6_1 = _copiar_ou_vazio(df_itens_premanifestos_m5_3)
    df_premanifestos_m5_4_m6_1 = _copiar_ou_vazio(df_premanifestos_m5_4)
    df_itens_premanifestos_m5_4_m6_1 = _copiar_ou_vazio(df_itens_premanifestos_m5_4)
    _print_log("[M6.1] executando M6.1 com:")
    _print_log(f"[M6.1] df_manifestos_m4 linhas={_safe_len(df_manifestos_m4_m6_1)}")
    _print_log(f"[M6.1] df_itens_manifestados_m4 linhas={_safe_len(df_itens_manifestados_m4_m6_1)}")
    _print_log(f"[M6.1] df_premanifestos_m5_2 linhas={_safe_len(df_premanifestos_m5_2_m6_1)}")
    _print_log(f"[M6.1] df_itens_premanifestos_m5_2 linhas={_safe_len(df_itens_premanifestos_m5_2_m6_1)}")
    _print_log(f"[M6.1] df_premanifestos_m5_3 linhas={_safe_len(df_premanifestos_m5_3_m6_1)}")
    _print_log(f"[M6.1] df_itens_premanifestos_m5_3 linhas={_safe_len(df_itens_premanifestos_m5_3_m6_1)}")
    _print_log(f"[M6.1] df_premanifestos_m5_4 linhas={_safe_len(df_premanifestos_m5_4_m6_1)}")
    _print_log(f"[M6.1] df_itens_premanifestos_m5_4 linhas={_safe_len(df_itens_premanifestos_m5_4_m6_1)}")
    t0 = _agora()
    outputs_m6_1, meta_m6_1 = executar_m6_1_consolidacao_manifestos(
        df_manifestos_m4=df_manifestos_m4_m6_1,
        df_itens_manifestados_m4=df_itens_manifestados_m4_m6_1,
        df_premanifestos_m5_2=df_premanifestos_m5_2_m6_1,
        df_itens_premanifestos_m5_2=df_itens_premanifestos_m5_2_m6_1,
        df_premanifestos_m5_3=df_premanifestos_m5_3_m6_1,
        df_itens_premanifestos_m5_3=df_itens_premanifestos_m5_3_m6_1,
        df_premanifestos_m5_4=df_premanifestos_m5_4_m6_1,
        df_itens_premanifestos_m5_4=df_itens_premanifestos_m5_4_m6_1,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
    )
    tempo_m6_1 = _duracao_ms(t0)
    metricas_tempo["m6_1_consolidacao_manifestos_ms"] = tempo_m6_1

    resumo_m6_1 = meta_m6_1["resumo_m6_1"]
    df_manifestos_base_m6 = outputs_m6_1["df_manifestos_base_m6"]
    df_itens_manifestos_base_m6 = outputs_m6_1["df_itens_manifestos_base_m6"]
    df_estatisticas_manifestos_antes_m6 = outputs_m6_1["df_estatisticas_manifestos_antes_m6"]
    df_pares_elegiveis_otimizacao_m6 = outputs_m6_1["df_pares_elegiveis_otimizacao_m6"]
    _print_log(f"[M6.1] df_manifestos_base_m6 linhas={_safe_len(df_manifestos_base_m6)}")
    _print_log(f"[M6.1] df_itens_manifestos_base_m6 linhas={_safe_len(df_itens_manifestos_base_m6)}")
    _print_log(f"[M6.1] df_estatisticas_manifestos_antes_m6 linhas={_safe_len(df_estatisticas_manifestos_antes_m6)}")
    _print_log(f"[M6.1] df_pares_elegiveis_otimizacao_m6 linhas={_safe_len(df_pares_elegiveis_otimizacao_m6)}")

    logs.append(
        _log(
            modulo="m6_1_consolidacao_manifestos",
            status="ok",
            mensagem="M6.1 executado com sucesso",
            quantidade_entrada=(
                _safe_len(df_manifestos_m4)
                + _safe_len(df_premanifestos_m5_2)
                + _safe_len(df_premanifestos_m5_3)
                + _safe_len(df_premanifestos_m5_4)
            ),
            quantidade_saida=_safe_len(df_manifestos_base_m6),
            tempo_ms=tempo_m6_1,
            extra={
                **resumo_m6_1,
                "total_itens_manifestos_base_m6": _safe_len(df_itens_manifestos_base_m6),
                "total_estatisticas_manifestos_antes_m6": _safe_len(df_estatisticas_manifestos_antes_m6),
                "total_pares_elegiveis_otimizacao_m6": _safe_len(df_pares_elegiveis_otimizacao_m6),
            },
        )
    )

    total_m6_1_manifestos_base = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_1_consolidacao_manifestos",
        ordem_modulo=13,
        df_etapa=df_manifestos_base_m6,
        snapshot_nome="m6_1_manifestos_base",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_1_consolidacao_manifestos"] = auditoria_por_modulo.get("m6_1_consolidacao_manifestos", 0) + total_m6_1_manifestos_base
    auditoria_por_snapshot["m6_1_manifestos_base"] = auditoria_por_snapshot.get("m6_1_manifestos_base", 0) + total_m6_1_manifestos_base
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_1_manifestos_base linhas={total_m6_1_manifestos_base}")

    total_m6_1_itens_manifestos_base = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_1_consolidacao_manifestos",
        ordem_modulo=13,
        df_etapa=df_itens_manifestos_base_m6,
        snapshot_nome="m6_1_itens_manifestos_base",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_1_consolidacao_manifestos"] = auditoria_por_modulo.get("m6_1_consolidacao_manifestos", 0) + total_m6_1_itens_manifestos_base
    auditoria_por_snapshot["m6_1_itens_manifestos_base"] = auditoria_por_snapshot.get("m6_1_itens_manifestos_base", 0) + total_m6_1_itens_manifestos_base
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_1_itens_manifestos_base linhas={total_m6_1_itens_manifestos_base}")

    total_m6_1_estatisticas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_1_consolidacao_manifestos",
        ordem_modulo=13,
        df_etapa=df_estatisticas_manifestos_antes_m6,
        snapshot_nome="m6_1_estatisticas_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_1_consolidacao_manifestos"] = auditoria_por_modulo.get("m6_1_consolidacao_manifestos", 0) + total_m6_1_estatisticas
    auditoria_por_snapshot["m6_1_estatisticas_manifestos"] = auditoria_por_snapshot.get("m6_1_estatisticas_manifestos", 0) + total_m6_1_estatisticas
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_1_estatisticas_manifestos linhas={total_m6_1_estatisticas}")

    total_m6_1_pares = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_1_consolidacao_manifestos",
        ordem_modulo=13,
        df_etapa=df_pares_elegiveis_otimizacao_m6,
        snapshot_nome="m6_1_pares_elegiveis_otimizacao",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_1_consolidacao_manifestos"] = auditoria_por_modulo.get("m6_1_consolidacao_manifestos", 0) + total_m6_1_pares
    auditoria_por_snapshot["m6_1_pares_elegiveis_otimizacao"] = auditoria_por_snapshot.get("m6_1_pares_elegiveis_otimizacao", 0) + total_m6_1_pares
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_1_pares_elegiveis_otimizacao linhas={total_m6_1_pares}")

    if not PIPELINE_FLAGS["executar_m6_2"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execucao encerrada propositalmente apos o M6.1 para auditoria da consolidacao de manifestos.",
            "pipeline_real_ate": "M6.1",
            "modo_resposta": "auditoria_m6_1_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_itens_premanifestados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
                "total_mesorregioes_m5_4a": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_saldo_elegivel_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4),
                "total_saldo_nao_elegivel_m5_4": _safe_len(df_saldo_nao_elegivel_m5_4),
                "total_tentativas_m5_4a": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
                "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
                "total_itens_premanifestados_m5_4": _safe_len(df_itens_premanifestos_m5_4),
                "total_remanescente_m5_4": _safe_len(df_remanescente_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_m5_4),
                "total_remanescente_global_final_roteirizacao": _safe_len(df_remanescente_global_final_roteirizacao),
                "total_manifestos_base_m6": _safe_len(df_manifestos_base_m6),
                "total_itens_manifestos_base_m6": _safe_len(df_itens_manifestos_base_m6),
                "total_estatisticas_manifestos_m6": _safe_len(df_estatisticas_manifestos_antes_m6),
                "total_pares_elegiveis_otimizacao_m6": _safe_len(df_pares_elegiveis_otimizacao_m6),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "resumo_m5_4a": resumo_m5_4a,
            "resumo_m5_4b": resumo_m5_4b,
            "resumo_m6_1": resumo_m6_1,
            "logs": logs,
        }

    # =========================================================================================
    # M6.2
    # =========================================================================================
    df_manifestos_base_m6_input = _copiar_ou_vazio(df_manifestos_base_m6, colunas=COLS_MANIFESTOS_OBRIGATORIAS)
    df_estatisticas_manifestos_antes_m6_input = _copiar_ou_vazio(df_estatisticas_manifestos_antes_m6)
    df_itens_manifestos_base_m6_input = _copiar_ou_vazio(df_itens_manifestos_base_m6)
    df_remanescente_global_final_roteirizacao_input = _copiar_ou_vazio(df_remanescente_global_final_roteirizacao)

    _print_log("[M6.2] executando M6.2 com:")
    _print_log(f"[M6.2] df_manifestos_base_m6 linhas={_safe_len(df_manifestos_base_m6_input)}")
    _print_log(f"[M6.2] df_estatisticas_manifestos_antes_m6 linhas={_safe_len(df_estatisticas_manifestos_antes_m6_input)}")
    _print_log(f"[M6.2] df_itens_manifestos_base_m6 linhas={_safe_len(df_itens_manifestos_base_m6_input)}")
    _print_log(f"[M6.2] df_remanescente_global_final_roteirizacao linhas={_safe_len(df_remanescente_global_final_roteirizacao_input)}")

    t0 = _agora()
    manifesto_schema_valido = _tem_schema_minimo(df_manifestos_base_m6_input, COLS_MANIFESTOS_OBRIGATORIAS)
    manifesto_tem_id = isinstance(df_manifestos_base_m6_input, pd.DataFrame) and ("manifesto_id" in df_manifestos_base_m6_input.columns)
    manifestos_base_validos_m6_2 = (
        isinstance(df_manifestos_base_m6_input, pd.DataFrame)
        and manifesto_schema_valido
        and manifesto_tem_id
        and (not df_manifestos_base_m6_input.empty)
    )

    if manifestos_base_validos_m6_2:
        resultado_m6_2 = executar_m6_2_complemento_ocupacao(
            df_manifestos_base_m6=df_manifestos_base_m6_input,
            df_estatisticas_manifestos_antes_m6=df_estatisticas_manifestos_antes_m6_input,
            df_itens_manifestos_base_m6=df_itens_manifestos_base_m6_input,
            df_remanescente_m5_4=df_remanescente_global_final_roteirizacao_input,
            data_base_roteirizacao=contexto.data_base,
            tipo_roteirizacao=contexto.tipo_roteirizacao,
            caminhos_pipeline=contexto.caminhos_pipeline,
            ocupacao_alvo_perc=85.0,
        )
    else:
        resultado_m6_2 = {
            "outputs_m6_2": {
                "df_manifestos_m6_2": (
                    df_manifestos_base_m6_input.copy()
                    if isinstance(df_manifestos_base_m6_input, pd.DataFrame)
                    else pd.DataFrame(columns=COLS_MANIFESTOS_OBRIGATORIAS)
                ),
                "df_itens_manifestos_m6_2": (
                    df_itens_manifestos_base_m6_input.copy()
                    if isinstance(df_itens_manifestos_base_m6_input, pd.DataFrame)
                    else pd.DataFrame()
                ),
                "df_remanescente_m6_2": df_remanescente_global_final_roteirizacao_input.copy() if isinstance(df_remanescente_global_final_roteirizacao_input, pd.DataFrame) else pd.DataFrame(),
                "df_remanescente_m5_original_m6_2": df_remanescente_global_final_roteirizacao_input.copy() if isinstance(df_remanescente_global_final_roteirizacao_input, pd.DataFrame) else pd.DataFrame(),
                "df_tentativas_m6_2": pd.DataFrame(),
                "df_movimentos_aceitos_m6_2": pd.DataFrame(),
            },
            "resumo_m6_2": {
                "modulo": "M6.2",
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "ocupacao_alvo_perc": 85.0,
                "etapa_pulada": True,
                "motivo_etapa_pulada": "sem_manifestos_base_m6_2",
                "manifestos_base_total_m6_1": _safe_len(df_manifestos_base_m6_input),
                "itens_manifestos_base_total_m6_1": _safe_len(df_itens_manifestos_base_m6_input),
                "remanescente_m5_original_total": _safe_len(df_remanescente_global_final_roteirizacao_input),
                "movimentos_aceitos_m6_2": 0,
                "tentativas_total_m6_2": 0,
                "linhas_entrada_m6_2": _safe_len(df_manifestos_base_m6_input),
                "linhas_saida_m6_2": 0,
                "remanescente_preservado_m6_2": _safe_len(df_remanescente_global_final_roteirizacao_input),
                "caminhos_pipeline": contexto.caminhos_pipeline or {},
            },
        }
    tempo_m6_2 = _duracao_ms(t0)
    metricas_tempo["m6_2_complemento_ocupacao_ms"] = tempo_m6_2

    outputs_m6_2 = resultado_m6_2["outputs_m6_2"]
    resumo_m6_2 = resultado_m6_2["resumo_m6_2"]

    df_manifestos_m6_2 = outputs_m6_2["df_manifestos_m6_2"]
    df_itens_manifestos_m6_2 = outputs_m6_2["df_itens_manifestos_m6_2"]
    df_remanescente_m6_2 = outputs_m6_2["df_remanescente_m6_2"]
    df_remanescente_m5_original_m6_2 = outputs_m6_2["df_remanescente_m5_original_m6_2"]
    df_tentativas_m6_2 = outputs_m6_2["df_tentativas_m6_2"]
    df_movimentos_aceitos_m6_2 = outputs_m6_2["df_movimentos_aceitos_m6_2"]
    _print_log(f"[M6.2] df_manifestos_m6_2 linhas={_safe_len(df_manifestos_m6_2)}")
    _print_log(f"[M6.2] df_itens_manifestos_m6_2 linhas={_safe_len(df_itens_manifestos_m6_2)}", force=not log_verbose)
    _print_log(f"[M6.2] df_remanescente_m6_2 linhas={_safe_len(df_remanescente_m6_2)}")
    _print_log(f"[M6.2] df_remanescente_m5_original_m6_2 linhas={_safe_len(df_remanescente_m5_original_m6_2)}")
    _print_log(f"[M6.2] df_tentativas_m6_2 linhas={_safe_len(df_tentativas_m6_2)}")
    _print_log(f"[M6.2] df_movimentos_aceitos_m6_2 linhas={_safe_len(df_movimentos_aceitos_m6_2)}")

    logs.append(
        _log(
            modulo="m6_2_complemento_ocupacao",
            status="ok" if manifestos_base_validos_m6_2 else "ignorado",
            mensagem=(
                "M6.2 executado com sucesso"
                if manifestos_base_validos_m6_2
                else "M6.2 ignorado: não há manifestos base válidos para complemento de ocupação"
            ),
            quantidade_entrada=_safe_len(df_manifestos_base_m6),
            quantidade_saida=_safe_len(df_manifestos_m6_2),
            tempo_ms=tempo_m6_2,
            extra={
                **resumo_m6_2,
                "total_itens_manifestos_m6_2": _safe_len(df_itens_manifestos_m6_2),
                "total_remanescente_m6_2": _safe_len(df_remanescente_m6_2),
                "total_remanescente_m5_original_m6_2": _safe_len(df_remanescente_m5_original_m6_2),
                "total_tentativas_m6_2": _safe_len(df_tentativas_m6_2),
                "total_movimentos_aceitos_m6_2": _safe_len(df_movimentos_aceitos_m6_2),
            },
        )
    )

    total_m6_2_manifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_manifestos_m6_2,
        snapshot_nome="m6_2_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_manifestos
    auditoria_por_snapshot["m6_2_manifestos"] = auditoria_por_snapshot.get("m6_2_manifestos", 0) + total_m6_2_manifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_manifestos linhas={total_m6_2_manifestos}")

    total_m6_2_itens_manifestos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_itens_manifestos_m6_2,
        snapshot_nome="m6_2_itens_manifestos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_itens_manifestos
    auditoria_por_snapshot["m6_2_itens_manifestos"] = auditoria_por_snapshot.get("m6_2_itens_manifestos", 0) + total_m6_2_itens_manifestos
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_itens_manifestos linhas={total_m6_2_itens_manifestos}")

    total_m6_2_remanescente = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_remanescente_m6_2,
        snapshot_nome="m6_2_remanescente",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_remanescente
    auditoria_por_snapshot["m6_2_remanescente"] = auditoria_por_snapshot.get("m6_2_remanescente", 0) + total_m6_2_remanescente
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_remanescente linhas={total_m6_2_remanescente}")

    total_m6_2_remanescente_original = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_remanescente_m5_original_m6_2,
        snapshot_nome="m6_2_remanescente_original",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_remanescente_original
    auditoria_por_snapshot["m6_2_remanescente_original"] = auditoria_por_snapshot.get("m6_2_remanescente_original", 0) + total_m6_2_remanescente_original
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_remanescente_original linhas={total_m6_2_remanescente_original}")

    total_m6_2_tentativas = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_tentativas_m6_2,
        snapshot_nome="m6_2_tentativas",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_tentativas
    auditoria_por_snapshot["m6_2_tentativas"] = auditoria_por_snapshot.get("m6_2_tentativas", 0) + total_m6_2_tentativas
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_tentativas linhas={total_m6_2_tentativas}")

    total_m6_2_movimentos_aceitos = _persistir_snapshot_se_ativo(auditoria_ativa, 
        teste_id=teste_id_auditoria,
        rodada_id=contexto.rodada_id,
        upload_id=contexto.upload_id,
        modulo="m6_2_complemento_ocupacao",
        ordem_modulo=10,
        df_etapa=df_movimentos_aceitos_m6_2,
        snapshot_nome="m6_2_movimentos_aceitos",
        contexto=contexto_auditoria,
        rastreamento=auditoria_flat_rastreamento,
    )
    auditoria_por_modulo["m6_2_complemento_ocupacao"] = auditoria_por_modulo.get("m6_2_complemento_ocupacao", 0) + total_m6_2_movimentos_aceitos
    auditoria_por_snapshot["m6_2_movimentos_aceitos"] = auditoria_por_snapshot.get("m6_2_movimentos_aceitos", 0) + total_m6_2_movimentos_aceitos
    _print_log(f"[AUDITORIA FLAT] snapshot=m6_2_movimentos_aceitos linhas={total_m6_2_movimentos_aceitos}")

    if not PIPELINE_FLAGS["executar_m7"]:
        tempo_total = _duracao_ms(inicio_total)
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
        _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")
        return {
            "status": "ok",
            "mensagem": "Execução encerrada propositalmente após o M6.2 para auditoria operacional desta etapa.",
            "pipeline_real_ate": "M6.2",
            "modo_resposta": "auditoria_m6_2_modular",
            "resposta_truncada": False,
            "teste_id_auditoria": teste_id_auditoria,
            "auditoria_modular": {
                "teste_id_auditoria": teste_id_auditoria,
                "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                "snapshots": [{"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()],
                "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
            },
            "resumo_execucao": {
                "rodada_id": contexto.rodada_id,
                "upload_id": contexto.upload_id,
                "usuario_id": contexto.usuario_id,
                "filial_id": contexto.filial_id,
                "tipo_roteirizacao": contexto.tipo_roteirizacao,
                "data_base_roteirizacao": contexto.data_base.isoformat(),
                "tempos_ms": metricas_tempo,
            },
            "resumo_negocio": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_enriquecida_m2": _safe_len(df_carteira_enriquecida),
                "total_triagem_m3": _safe_len(df_carteira_triagem),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "total_input_bloco_4": _safe_len(df_input_oficial_bloco_4),
                "total_manifestos_m4": _safe_len(df_manifestos_m4),
                "total_itens_manifestados_m4": _safe_len(df_itens_manifestados_m4),
                "total_remanescente_m4": _safe_len(df_remanescente_roteirizavel_bloco_4),
                "total_saldo_elegivel_m5_1": _safe_len(df_saldo_elegivel_composicao_m5_1),
                "total_saldo_nao_elegivel_m5_1": _safe_len(df_saldo_nao_elegivel_m5_1),
                "total_premanifestos_m5_2": _safe_len(df_premanifestos_m5_2),
                "total_itens_premanifestados_m5_2": _safe_len(df_itens_premanifestos_m5_2),
                "total_remanescente_m5_2": _safe_len(df_remanescente_m5_2),
                "total_tentativas_m5_2": _safe_len(df_tentativas_m5_2),
                "total_saldo_elegivel_m5_3": _safe_len(df_saldo_elegivel_composicao_m5_3),
                "total_saldo_nao_elegivel_m5_3": _safe_len(df_saldo_nao_elegivel_m5_3),
                "total_premanifestos_m5_3": _safe_len(df_premanifestos_m5_3),
                "total_itens_premanifestados_m5_3": _safe_len(df_itens_premanifestos_m5_3),
                "total_remanescente_m5_3": _safe_len(df_remanescente_m5_3),
                "total_tentativas_m5_3": _safe_len(df_tentativas_m5_3),
                "total_remanescente_global_ate_m5_3": _safe_len(df_remanescente_global_ate_m5_3),
                "total_mesorregioes_m5_4a": _safe_len(df_mesorregioes_consolidadas_m5_4),
                "total_saldo_elegivel_m5_4": _safe_len(df_saldo_elegivel_composicao_m5_4),
                "total_saldo_nao_elegivel_m5_4": _safe_len(df_saldo_nao_elegivel_m5_4),
                "total_tentativas_m5_4a": _safe_len(df_tentativas_triagem_mesorregioes_m5_4),
                "total_premanifestos_m5_4": _safe_len(df_premanifestos_m5_4),
                "total_itens_premanifestados_m5_4": _safe_len(df_itens_premanifestos_m5_4),
                "total_remanescente_m5_4": _safe_len(df_remanescente_m5_4),
                "total_tentativas_m5_4": _safe_len(df_tentativas_m5_4),
                "total_remanescente_global_final_roteirizacao": _safe_len(df_remanescente_global_final_roteirizacao),
                "total_manifestos_base_m6": _safe_len(df_manifestos_base_m6),
                "total_itens_manifestos_base_m6": _safe_len(df_itens_manifestos_base_m6),
                "total_estatisticas_manifestos_m6": _safe_len(df_estatisticas_manifestos_antes_m6),
                "total_pares_elegiveis_otimizacao_m6": _safe_len(df_pares_elegiveis_otimizacao_m6),
                "total_manifestos_m6_2": _safe_len(df_manifestos_m6_2),
                "total_itens_manifestos_m6_2": _safe_len(df_itens_manifestos_m6_2),
                "total_remanescente_m6_2": _safe_len(df_remanescente_m6_2),
                "total_remanescente_original_m6_2": _safe_len(df_remanescente_m5_original_m6_2),
                "total_tentativas_m6_2": _safe_len(df_tentativas_m6_2),
                "total_movimentos_aceitos_m6_2": _safe_len(df_movimentos_aceitos_m6_2),
            },
            "resumo_m4": resumo_m4,
            "resumo_m5_1": resumo_m5_1,
            "resumo_m5_2": resumo_m5_2,
            "resumo_m5_3a": resumo_m5_3a,
            "resumo_m5_3b": resumo_m5_3b,
            "resumo_m5_4a": resumo_m5_4a,
            "resumo_m5_4b": resumo_m5_4b,
            "resumo_m6_1": resumo_m6_1,
            "resumo_m6_2": resumo_m6_2,
            "logs": logs,
        }

    # =========================================================================================
    # M7
    # =========================================================================================
    df_manifestos_m6_2_input_m7 = _copiar_ou_vazio(df_manifestos_m6_2, colunas=COLS_MANIFESTOS_OBRIGATORIAS)
    df_itens_manifestos_m6_2_input_m7 = _copiar_ou_vazio(df_itens_manifestos_m6_2)
    df_geo_tratado_input_m7 = _copiar_ou_vazio(df_geo_tratado)
    df_geo_raw_input_m7 = _copiar_ou_vazio(contexto.df_geo_raw)

    _print_log("[M7] executando M7 com:")
    _print_log(f"- df_manifestos_m6_2 linhas={_safe_len(df_manifestos_m6_2_input_m7)}")
    _print_log(f"- df_itens_manifestos_m6_2 linhas={_safe_len(df_itens_manifestos_m6_2_input_m7)}")
    _print_log(f"- df_geo_tratado linhas={_safe_len(df_geo_tratado_input_m7)}")
    _print_log(f"- df_geo_raw linhas={_safe_len(df_geo_raw_input_m7)}")

    t0 = _agora()
    outputs_m7, meta_m7 = executar_m7_sequenciamento_entregas(
        df_manifestos_m6_2=df_manifestos_m6_2_input_m7,
        df_itens_manifestos_m6_2=df_itens_manifestos_m6_2_input_m7,
        df_geo_tratado=df_geo_tratado_input_m7,
        df_geo_raw=df_geo_raw_input_m7,
        data_base_roteirizacao=contexto.data_base,
        tipo_roteirizacao=contexto.tipo_roteirizacao,
        caminhos_pipeline=contexto.caminhos_pipeline,
        filial_contexto=contexto.filial,
    )
    tempo_m7 = _duracao_ms(t0)
    metricas_tempo["m7_sequenciamento_entregas_ms"] = tempo_m7

    if not isinstance(outputs_m7, dict):
        outputs_m7 = {}

    output_keys_m7 = sorted(list(outputs_m7.keys()))
    _print_log(f"[M7] output real recebido: {output_keys_m7}")

    resumo_m7 = meta_m7.get("resumo_m7", {}) if isinstance(meta_m7, dict) else {}
    auditoria_m7 = meta_m7.get("auditoria_m7", {}) if isinstance(meta_m7, dict) else {}

    snapshots_m7_map = {
        "m7_manifestos": ["df_manifestos_m7"],
        "m7_itens_sequenciados": ["df_itens_manifestos_sequenciados_m7"],
        "m7_paradas": ["df_paradas_m7"],
        "m7_auditoria": ["df_auditoria_m7"],
        "m7_resumo_sequenciamento": ["df_manifestos_sequenciamento_resumo_m7"],
        "m7_tentativas": ["df_tentativas_sequenciamento_m7"],
        "m7_diagnostico_coordenadas": ["df_diagnostico_recuperacao_coordenadas_m7"],
    }

    snapshots_m7_df: Dict[str, pd.DataFrame] = {}
    for snapshot_nome, chaves_contrato in snapshots_m7_map.items():
        df_snapshot = None
        chave_encontrada = None
        for chave in chaves_contrato:
            candidato = outputs_m7.get(chave)
            if isinstance(candidato, pd.DataFrame):
                df_snapshot = candidato
                chave_encontrada = chave
                break

        if df_snapshot is None:
            _print_log(f"[M7] snapshot {snapshot_nome} não retornado pelo módulo ativo")
            snapshots_m7_df[snapshot_nome] = pd.DataFrame()
            continue

        snapshots_m7_df[snapshot_nome] = df_snapshot
        _print_log(f"[M7] snapshot {snapshot_nome} mapeado da chave {chave_encontrada}")

    df_manifestos_m7 = snapshots_m7_df["m7_manifestos"]
    df_itens_manifestos_sequenciados_m7 = snapshots_m7_df["m7_itens_sequenciados"]
    df_paradas_m7 = snapshots_m7_df["m7_paradas"]
    df_auditoria_m7 = snapshots_m7_df["m7_auditoria"]
    df_manifestos_sequenciamento_resumo_m7 = snapshots_m7_df["m7_resumo_sequenciamento"]
    df_tentativas_sequenciamento_m7 = snapshots_m7_df["m7_tentativas"]
    df_diagnostico_recuperacao_coordenadas_m7 = snapshots_m7_df["m7_diagnostico_coordenadas"]

    _print_log(f"[M7] df_manifestos_m7 linhas={_safe_len(df_manifestos_m7)}")
    _print_log(f"[M7] df_itens_sequenciados_m7 linhas={_safe_len(df_itens_manifestos_sequenciados_m7)}")
    _print_log(f"[M7] df_paradas_m7 linhas={_safe_len(df_paradas_m7)}")
    _print_log(f"[M7] df_auditoria_m7 linhas={_safe_len(df_auditoria_m7)}")
    _print_log(f"[M7] df_resumo_sequenciamento_m7 linhas={_safe_len(df_manifestos_sequenciamento_resumo_m7)}")
    _print_log(f"[M7] df_tentativas_m7 linhas={_safe_len(df_tentativas_sequenciamento_m7)}")
    _print_log(f"[M7] df_diagnostico_coordenadas_m7 linhas={_safe_len(df_diagnostico_recuperacao_coordenadas_m7)}")

    status_log_m7 = "ok" if _safe_len(df_itens_manifestos_m6_2_input_m7) > 0 else "ignorado"
    mensagem_log_m7 = "M7 executado com sucesso" if status_log_m7 == "ok" else "M7 executado com entrada vazia e saída controlada"

    logs.append(
        _log(
            modulo="m7_sequenciamento_entregas",
            status=status_log_m7,
            mensagem=mensagem_log_m7,
            quantidade_entrada=_safe_len(df_itens_manifestos_m6_2_input_m7),
            quantidade_saida=_safe_len(df_itens_manifestos_sequenciados_m7),
            tempo_ms=tempo_m7,
            extra={
                **resumo_m7,
                "output_real_chaves_m7": output_keys_m7,
                "total_manifestos_m7": _safe_len(df_manifestos_m7),
                "total_itens_sequenciados_m7": _safe_len(df_itens_manifestos_sequenciados_m7),
                "total_paradas_m7": _safe_len(df_paradas_m7),
                "total_auditoria_m7": _safe_len(df_auditoria_m7),
                "total_resumo_sequenciamento_m7": _safe_len(df_manifestos_sequenciamento_resumo_m7),
                "total_tentativas_m7": _safe_len(df_tentativas_sequenciamento_m7),
                "total_diagnostico_coordenadas_m7": _safe_len(df_diagnostico_recuperacao_coordenadas_m7),
            },
        )
    )

    m7_snapshot_ordem = [
        "m7_manifestos",
        "m7_itens_sequenciados",
        "m7_paradas",
        "m7_auditoria",
        "m7_resumo_sequenciamento",
        "m7_tentativas",
        "m7_diagnostico_coordenadas",
    ]
    for snapshot_nome in m7_snapshot_ordem:
        df_snapshot = snapshots_m7_df.get(snapshot_nome)
        if not isinstance(df_snapshot, pd.DataFrame):
            _print_log(f"[M7] snapshot {snapshot_nome} não retornado pelo módulo ativo")
            continue
        total_m7_snapshot = _persistir_snapshot_se_ativo(auditoria_ativa, 
            teste_id=teste_id_auditoria,
            rodada_id=contexto.rodada_id,
            upload_id=contexto.upload_id,
            modulo="m7_sequenciamento_entregas",
            ordem_modulo=11,
            df_etapa=df_snapshot,
            snapshot_nome=snapshot_nome,
            contexto=contexto_auditoria,
            rastreamento=auditoria_flat_rastreamento,
        )
        auditoria_por_modulo["m7_sequenciamento_entregas"] = auditoria_por_modulo.get("m7_sequenciamento_entregas", 0) + total_m7_snapshot
        auditoria_por_snapshot[snapshot_nome] = auditoria_por_snapshot.get(snapshot_nome, 0) + total_m7_snapshot
        _print_log(f"[AUDITORIA FLAT] snapshot={snapshot_nome} linhas={total_m7_snapshot}")

    tempo_total = _duracao_ms(inicio_total)
    metricas_tempo["tempo_total_pipeline_ms"] = tempo_total
    _print_log(f"[AUDITORIA FLAT] total_colunas_persistidas={len(auditoria_flat_rastreamento.get('colunas_persistidas', set()))}")

    if "tempo_total_pipeline_ms" not in metricas_tempo:
        metricas_tempo["tempo_total_pipeline_ms"] = tempo_total

    manifestos_m7 = _serializar_dataframe_para_records(df_manifestos_m7, limit=None)
    itens_manifestos_sequenciados_m7 = _serializar_dataframe_para_records(df_itens_manifestos_sequenciados_m7, limit=None)
    manifestos_sequenciamento_resumo_m7 = _serializar_dataframe_para_records(
        df_manifestos_sequenciamento_resumo_m7, limit=None
    )
    paradas_m7 = _serializar_dataframe_para_records(df_paradas_m7, limit=None)
    saldo_final_roteirizacao = _serializar_dataframe_para_records(df_remanescente_m6_2, limit=None)

    nao_roteirizaveis_m3 = _serializar_dataframe_para_records(df_nao_roteirizaveis_m3, limit=None)
    agendamento_futuro = _serializar_dataframe_para_records(df_agendamento_futuro_m3, limit=None)
    aguardando_agendamento = _serializar_dataframe_para_records(df_aguardando_agendamento_m3, limit=None)
    excecoes_triagem = _serializar_dataframe_para_records(df_excecoes_triagem_m3, limit=None)
    agenda_vencida = _serializar_dataframe_para_records(df_agenda_vencida_m3, limit=None)

    manifestos_fechados: List[Dict[str, Any]] = []
    manifestos_compostos: List[Dict[str, Any]] = []
    for manifesto in manifestos_m7:
        origem_tipo = str(manifesto.get("origem_manifesto_tipo", "")).strip().lower()
        origem_modulo = str(manifesto.get("origem_manifesto_modulo", "")).strip().upper()
        if "manifesto_fechado" in origem_tipo or origem_modulo == "M4":
            manifestos_fechados.append(manifesto)
        else:
            manifestos_compostos.append(manifesto)

    print("[RESPONSE] total manifestos_m7 serializados:", len(manifestos_m7))
    print("[RESPONSE] total itens_manifestos_sequenciados_m7 serializados:", len(itens_manifestos_sequenciados_m7))
    print(
        "[RESPONSE] total manifestos_sequenciamento_resumo_m7 serializados:",
        len(manifestos_sequenciamento_resumo_m7),
    )
    print("[RESPONSE] total paradas_m7 serializadas:", len(paradas_m7))
    print("[RESPONSE] total remanescentes saldo_final_roteirizacao:", len(saldo_final_roteirizacao))
    print(
        "[CONTRATO TRIAGEM]",
        {
            "saldo_final_roteirizacao": len(saldo_final_roteirizacao),
            "nao_roteirizaveis_m3": len(nao_roteirizaveis_m3),
            "agendamento_futuro": len(agendamento_futuro),
            "aguardando_agendamento": len(aguardando_agendamento),
            "excecoes_triagem": len(excecoes_triagem),
            "agenda_vencida": len(agenda_vencida),
        },
    )
    print("[VALIDADOR EXAUSTAO] resumo", auditoria_reprocessamento_etapas)

    resposta: Dict[str, Any] = {
        "status": "ok",
        "mensagem": "Motor executou com sucesso até o M7 sequenciamento de entregas.",
        "pipeline_real_ate": "M7",
        "modo_resposta": "auditoria_m7_sequenciamento_entregas",
        "resposta_truncada": False,
        "resumo_execucao": {
            "rodada_id": contexto.rodada_id,
            "upload_id": contexto.upload_id,
            "usuario_id": contexto.usuario_id,
            "filial_id": contexto.filial_id,
            "tipo_roteirizacao": contexto.tipo_roteirizacao,
            "data_base_roteirizacao": contexto.data_base.isoformat(),
            "tempos_ms": metricas_tempo,
        },
        "resumo_negocio": {
            "total_carteira": _safe_len(contexto.df_carteira_raw),
            "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
            "total_manifestos_m7": _safe_len(df_manifestos_m7),
            "total_itens_manifestos_sequenciados_m7": _safe_len(df_itens_manifestos_sequenciados_m7),
            "total_remanescente_m6_2": _safe_len(df_remanescente_m6_2),
            "total_paradas_m7": _safe_len(df_paradas_m7),
            "triagem": {
                "total_carteira": _safe_len(contexto.df_carteira_raw),
                "total_roteirizavel_m3": _safe_len(df_carteira_roteirizavel),
                "agendamento_futuro": _safe_len(df_agendamento_futuro_m3),
                "aguardando_agendamento": _safe_len(df_aguardando_agendamento_m3),
                "excecoes_triagem": _safe_len(df_excecoes_triagem_m3),
                "nao_roteirizaveis_m3": _safe_len(df_nao_roteirizaveis_m3),
                "saldo_final_roteirizacao": _safe_len(df_remanescente_m6_2),
            },
        },
        "manifestos_m7": manifestos_m7,
        "manifestos_fechados": manifestos_fechados,
        "manifestos_compostos": manifestos_compostos,
        "itens_manifestos_sequenciados_m7": itens_manifestos_sequenciados_m7,
        "manifestos_sequenciamento_resumo_m7": manifestos_sequenciamento_resumo_m7,
        "paradas_m7": paradas_m7,
        "nao_roteirizados": saldo_final_roteirizacao,
        "cargas_agendamento_futuro": agendamento_futuro,
        "cargas_agenda_vencida": agenda_vencida,
        "cargas_excecao_triagem": excecoes_triagem,
        "cargas_nao_alocadas": nao_roteirizaveis_m3,
        "remanescentes": {
            "nao_roteirizaveis_m3": nao_roteirizaveis_m3,
            "saldo_final_roteirizacao": saldo_final_roteirizacao,
            "agendamento_futuro": agendamento_futuro,
            "aguardando_agendamento": aguardando_agendamento,
            "excecoes_triagem": excecoes_triagem,
            "agenda_vencida": agenda_vencida,
        },
        "total_carteira": _safe_len(contexto.df_carteira_raw),
        "total_roteirizado": _safe_len(df_itens_manifestos_sequenciados_m7),
        "total_nao_roteirizado": _safe_len(df_remanescente_m6_2),
        "auditoria_reprocessamento_etapas": auditoria_reprocessamento_etapas,
    }

    if retornar_auditoria_interna:
        resposta.update(
            {
                "teste_id_auditoria": teste_id_auditoria,
                "resumo_m4": resumo_m4,
                "resumo_m5_1": resumo_m5_1,
                "resumo_m5_2": resumo_m5_2,
                "resumo_m5_3a": resumo_m5_3a,
                "resumo_m5_3b": resumo_m5_3b,
                "resumo_m5_4a": resumo_m5_4a,
                "resumo_m5_4b": resumo_m5_4b,
                "resumo_m6_1": resumo_m6_1,
                "resumo_m6_2": resumo_m6_2,
                "auditoria_modular": {
                    "teste_id_auditoria": teste_id_auditoria,
                    "modulos": [{"modulo": modulo, "linhas_gravadas": linhas} for modulo, linhas in auditoria_por_modulo.items()],
                    "snapshots": [
                        {"snapshot_nome": snapshot_nome, "linhas_gravadas": linhas} for snapshot_nome, linhas in auditoria_por_snapshot.items()
                    ],
                    "colunas_persistidas": sorted(list(auditoria_flat_rastreamento.get("colunas_persistidas", set()))),
                },
                "auditoria_m7": _serializar_dataframe_para_records(df_auditoria_m7),
                "auditoria_m7_meta": auditoria_m7,
                "tentativas_sequenciamento_m7": _serializar_dataframe_para_records(df_tentativas_sequenciamento_m7),
                "diagnostico_recuperacao_coordenadas_m7": _serializar_dataframe_para_records(
                    df_diagnostico_recuperacao_coordenadas_m7
                ),
                "logs": logs,
            }
        )

    _print_log(
        f"[CONTRATO SISTEMA1] manifestos_m7={len(manifestos_m7)} "
        f"itens_m7={len(itens_manifestos_sequenciados_m7)} "
        f"remanescentes_saldo={len(saldo_final_roteirizacao)}",
        force=True,
    )
    _print_log("[PIPELINE] Execução concluída", force=True)
    return resposta


def executar_pipeline(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    try:
        return _executar_pipeline_core(payload)
    except Exception as exc:
        erro_tecnico = str(exc)
        logs = [
            _log(
                modulo="pipeline_service",
                status="erro",
                mensagem="Falha durante execução do pipeline",
                extra={"erro_tecnico": erro_tecnico},
            )
        ]
        return {
            "status": "erro",
            "mensagem": erro_tecnico,
            "pipeline_real_ate": "ERRO",
            "modo_resposta": "contrato_sistema1_m7_erro",
            "resposta_truncada": False,
            "teste_id_auditoria": None,
            "resumo_execucao": {
                "rodada_id": getattr(payload, "rodada_id", None),
                "upload_id": getattr(payload, "upload_id", None),
                "usuario_id": getattr(payload, "usuario_id", None),
                "filial_id": getattr(payload, "filial_id", None),
                "tipo_roteirizacao": getattr(payload, "tipo_roteirizacao", None),
                "data_base_roteirizacao": getattr(payload, "data_base_roteirizacao", None),
                "tempos_ms": {},
            },
            "resumo_negocio": {},
            "contexto_rodada": {
                "filial": getattr(payload, "filial", None),
                "parametros_rodada": {},
            },
            "manifestos_m7": [],
            "itens_manifestos_sequenciados_m7": [],
            "manifestos_sequenciamento_resumo_m7": [],
            "paradas_m7": [],
            "auditoria_m7": [],
            "tentativas_sequenciamento_m7": [],
            "diagnostico_recuperacao_coordenadas_m7": [],
            "nao_roteirizados": [],
            "cargas_agendamento_futuro": [],
            "cargas_agenda_vencida": [],
            "cargas_excecao_triagem": [],
            "cargas_nao_alocadas": [],
            "remanescentes": {
                "nao_roteirizaveis_m3": [],
                "saldo_final_roteirizacao": [],
                "agendamento_futuro": [],
                "aguardando_agendamento": [],
                "excecoes_triagem": [],
                "agenda_vencida": [],
            },
            "auditoria_serializacao": {
                "manifestos_m7_total": 0,
                "manifestos_m7_retornado": 0,
                "itens_manifestos_sequenciados_m7_total": 0,
                "itens_manifestos_sequenciados_m7_retornado": 0,
                "manifestos_sequenciamento_resumo_m7_total": 0,
                "manifestos_sequenciamento_resumo_m7_retornado": 0,
                "paradas_m7_total": 0,
                "paradas_m7_retornado": 0,
                "auditoria_m7_total": 0,
                "auditoria_m7_retornado": 0,
                "tentativas_sequenciamento_m7_total": 0,
                "tentativas_sequenciamento_m7_retornado": 0,
                "diagnostico_recuperacao_coordenadas_m7_total": 0,
                "diagnostico_recuperacao_coordenadas_m7_retornado": 0,
                "remanescentes_nao_roteirizaveis_m3_total": 0,
                "remanescentes_nao_roteirizaveis_m3_retornado": 0,
                "remanescentes_saldo_final_roteirizacao_total": 0,
                "remanescentes_saldo_final_roteirizacao_retornado": 0,
            },
            "auditoria_m7_meta": {},
            "erro_tecnico": erro_tecnico,
            "logs": logs,
        }
