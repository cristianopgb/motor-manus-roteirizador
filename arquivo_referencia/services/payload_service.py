from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from app.schemas import RoteirizacaoRequest


@dataclass
class PipelineContext:
    rodada_id: str
    upload_id: str
    usuario_id: str
    filial_id: str
    tipo_roteirizacao: str
    data_execucao: datetime
    data_base: datetime

    filial: Dict[str, Any]
    parametros_rodada: Dict[str, Any]
    metadados_rodada: Dict[str, Any]
    caminhos_pipeline: Dict[str, str]
    configuracao_frota: List[Dict[str, Any]]

    df_carteira_raw: pd.DataFrame
    df_geo_raw: pd.DataFrame
    df_parametros_raw: pd.DataFrame
    df_veiculos_raw: pd.DataFrame


def _parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_dataframe(items: List[Any]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for item in items:
        if hasattr(item, "model_dump"):
            rows.append(item.model_dump(by_alias=True, exclude_none=False))
        elif isinstance(item, dict):
            rows.append(item)
        else:
            rows.append(dict(item))

    return pd.DataFrame(rows)


def _normalizar_configuracao_frota(payload: RoteirizacaoRequest) -> List[Dict[str, Any]]:
    configuracao: List[Dict[str, Any]] = []

    for item in payload.configuracao_frota or []:
        if hasattr(item, "model_dump"):
            registro = item.model_dump(exclude_none=False)
        elif isinstance(item, dict):
            registro = dict(item)
        else:
            registro = dict(item)

        configuracao.append(
            {
                "perfil": registro.get("perfil"),
                "quantidade": registro.get("quantidade"),
            }
        )

    return configuracao


def _normalizar_parametros(payload: RoteirizacaoRequest) -> Dict[str, Any]:
    """
    Monta o dicionário de parâmetros que alimenta o pipeline.

    Regras:
    - preservar parametros vindos do Sistema 1
    - sobrescrever com os metadados oficiais da rodada quando necessário
    - manter compatibilidade com os módulos legados do pipeline
    """
    parametros = dict(payload.parametros or {})

    # ============================================================
    # METADADOS OFICIAIS DA RODADA
    # ============================================================
    parametros["rodada_id"] = payload.rodada_id
    parametros["upload_id"] = payload.upload_id
    parametros["usuario_id"] = payload.usuario_id
    parametros["filial_id"] = payload.filial.id
    parametros["tipo_roteirizacao"] = payload.tipo_roteirizacao
    parametros["data_base_roteirizacao"] = payload.data_base_roteirizacao

    # ============================================================
    # DADOS DA FILIAL / ORIGEM OPERACIONAL
    # ============================================================
    parametros["filial_nome"] = payload.filial.nome
    parametros["filial_cidade"] = payload.filial.cidade
    parametros["filial_uf"] = payload.filial.uf
    parametros["filial_latitude"] = float(payload.filial.latitude)
    parametros["filial_longitude"] = float(payload.filial.longitude)

    # Compatibilidade com módulos antigos do pipeline
    parametros["origem_cidade"] = payload.filial.cidade
    parametros["origem_uf"] = payload.filial.uf
    parametros["origem_latitude"] = float(payload.filial.latitude)
    parametros["origem_longitude"] = float(payload.filial.longitude)
    parametros["data_corte_referencia"] = payload.data_base_roteirizacao

    # ============================================================
    # REGRAS OPERACIONAIS BASE
    # ============================================================
    # Mantidas aqui como padrão do motor enquanto não vierem
    # parametrizadas formalmente do Sistema 1.
    parametros.setdefault("velocidade_media_km_h", 50)
    parametros.setdefault("horas_direcao_dia", 8)
    parametros.setdefault("km_dia_operacional", 400)

    # Compatibilidade com possíveis leituras antigas
    parametros.setdefault("fator_km_rodoviario", 1.25)
    parametros.setdefault("km_dia_max", parametros.get("km_dia_operacional", 400))

    # ============================================================
    # NOVO DATASET V2 - SINALIZAÇÃO DE CONTRATO
    # ============================================================
    parametros["layout_carteira_versao"] = "v2"
    parametros["possui_campos_v2"] = True

    # Campos críticos do novo layout ficam documentados no contexto.
    # A padronização da carteira em si continua no M1.
    parametros["campos_v2_criticos"] = [
        "Peso Calculo",
        "Prioridade",
        "Restrição Veículo",
        "Carro Dedicado",
        "Inicio Ent.",
        "Fim En",
    ]

    return parametros


def _parametros_dict_para_dataframe(parametros: Dict[str, Any]) -> pd.DataFrame:
    rows = [{"parametro": k, "valor": v} for k, v in parametros.items()]
    return pd.DataFrame(rows)


def _montar_caminhos_pipeline(rodada_id: str) -> Dict[str, str]:
    pasta_base = Path("/tmp") / "rec_roteirizador" / rodada_id

    return {
        "pasta_saida_base": str(pasta_base),
        "rodada_id": rodada_id,
    }


def normalizar_payload_para_pipeline(payload: RoteirizacaoRequest) -> PipelineContext:
    data_base = _parse_iso_datetime(payload.data_base_roteirizacao)
    data_execucao = datetime.now(timezone.utc)

    df_carteira_raw = _to_dataframe(payload.carteira)
    df_geo_raw = _to_dataframe(payload.regionalidades)
    df_veiculos_raw = _to_dataframe(payload.veiculos)

    configuracao_frota = _normalizar_configuracao_frota(payload)
    parametros_rodada = _normalizar_parametros(payload)
    df_parametros_raw = _parametros_dict_para_dataframe(parametros_rodada)

    filial = {
        "id": payload.filial.id,
        "nome": payload.filial.nome,
        "cidade": payload.filial.cidade,
        "uf": payload.filial.uf,
        "latitude": float(payload.filial.latitude),
        "longitude": float(payload.filial.longitude),
    }

    caminhos_pipeline = _montar_caminhos_pipeline(payload.rodada_id)

    metadados_rodada = {
        "rodada_id": payload.rodada_id,
        "upload_id": payload.upload_id,
        "usuario_id": payload.usuario_id,
        "filial_id": payload.filial_id,
        "tipo_roteirizacao": payload.tipo_roteirizacao,
        "data_base_roteirizacao": payload.data_base_roteirizacao,
        "data_execucao_utc": data_execucao.isoformat(),
        "filial": filial,
        "configuracao_frota": configuracao_frota,
        "layout_carteira_versao": "v2",
        "totais_entrada": {
            "carteira": int(len(df_carteira_raw)),
            "veiculos": int(len(df_veiculos_raw)),
            "regionalidades": int(len(df_geo_raw)),
            "configuracao_frota": int(len(configuracao_frota)),
        },
        "campos_v2_criticos": [
            "Peso Calculo",
            "Prioridade",
            "Restrição Veículo",
            "Carro Dedicado",
            "Inicio Ent.",
            "Fim En",
        ],
    }

    return PipelineContext(
        rodada_id=payload.rodada_id,
        upload_id=payload.upload_id,
        usuario_id=payload.usuario_id,
        filial_id=payload.filial_id,
        tipo_roteirizacao=payload.tipo_roteirizacao,
        data_execucao=data_execucao,
        data_base=data_base,
        filial=filial,
        parametros_rodada=parametros_rodada,
        metadados_rodada=metadados_rodada,
        caminhos_pipeline=caminhos_pipeline,
        configuracao_frota=configuracao_frota,
        df_carteira_raw=df_carteira_raw,
        df_geo_raw=df_geo_raw,
        df_parametros_raw=df_parametros_raw,
        df_veiculos_raw=df_veiculos_raw,
    )
