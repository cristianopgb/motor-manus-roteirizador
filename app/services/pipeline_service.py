"""
Pipeline Service — Orquestrador do Motor de Roteirização REC
Encadeia M1 → M2 → M3 → M4 → M5 e monta a resposta final.
"""
from __future__ import annotations
import math
from typing import List, Dict, Any, Tuple

from app.pipeline import (
    m1_padronizacao,
    m2_triagem,
    m3_distancias,
    m4_consolidacao,
    m5_sequenciamento,
)
from app.models.schemas import (
    RoteirizacaoRequest,
    RoteirizacaoResponse,
    ManifestoSchema,
    EntregaManifestoSchema,
    CargaNaoRoteirizadaSchema,
    LogEtapaSchema,
    ResumoSchema,
)


def _veiculo_to_dict(v) -> Dict:
    return {
        "id": v.id,
        "perfil": v.perfil,
        "placa": v.placa,
        "qtd_eixos": v.qtd_eixos,
        "capacidade_peso_kg": v.capacidade_peso_kg,
        "capacidade_vol_m3": v.capacidade_vol_m3,
        "max_entregas": v.max_entregas,
        "max_km_distancia": v.max_km_distancia,
        "ocupacao_minima_perc": v.ocupacao_minima_perc,
        "tipo_frota": v.tipo_frota,
    }


def _manifesto_to_schema(m: Dict) -> ManifestoSchema:
    cap_peso = m["cap_peso"] if m["cap_peso"] > 0 else 1
    cap_vol = m["cap_vol"] if m["cap_vol"] > 0 else 1

    # Tipo de carga predominante
    tipos = [e.get("tipo_carga", "") for e in m["entregas"] if e.get("tipo_carga")]
    tipo_predominante = max(set(tipos), key=tipos.count) if tipos else None

    entregas_schema = []
    for e in m["entregas"]:
        entregas_schema.append(EntregaManifestoSchema(
            sequencia=e.get("sequencia", 0),
            nro_doc=e.get("nro_doc"),
            romane=e.get("romane"),
            destin=e.get("destin"),
            cidade=e.get("cidade"),
            uf=e.get("uf"),
            bairro=e.get("bairro"),
            latitude=e.get("latitude"),
            longitude=e.get("longitude"),
            peso=e.get("peso_calculo"),
            peso_cubico=e.get("peso_cubico"),
            vlr_merc=e.get("vlr_merc"),
            qtd=e.get("qtd"),
            tipo_carga=e.get("tipo_carga"),
            prioridade=e.get("prioridade"),
            data_des=e.get("data_des"),
            dle=e.get("dle"),
            agendam=e.get("agendam"),
            km_origem=e.get("km_origem"),
            km_acumulado=e.get("km_acumulado"),
        ))

    return ManifestoSchema(
        manifesto_id=m["manifesto_id"],
        tipo=m["tipo"],
        veiculo_id=m["veiculo_id"],
        veiculo_perfil=m["veiculo_perfil"],
        veiculo_placa=m.get("veiculo_placa"),
        qtd_eixos=m["qtd_eixos"],
        qtd_entregas=len(m["entregas"]),
        peso_total_kg=round(m["peso_alocado"], 2),
        volume_total_m3=round(m["vol_alocado"], 3),
        ocupacao_peso_perc=round((m["peso_alocado"] / cap_peso) * 100, 1),
        ocupacao_volume_perc=round((m["vol_alocado"] / cap_vol) * 100, 1),
        km_total_estimado=round(m.get("km_total_estimado", 0.0), 2),
        tipo_carga_predominante=tipo_predominante,
        entregas=entregas_schema,
    )


def executar_pipeline(req: RoteirizacaoRequest) -> RoteirizacaoResponse:
    logs: List[LogEtapaSchema] = []
    nao_roteirizados_total: List[CargaNaoRoteirizadaSchema] = []

    carteira_raw = [item.model_dump() for item in req.carteira]
    veiculos = [_veiculo_to_dict(v) for v in req.veiculos]
    filial_lat = req.filial.latitude
    filial_lon = req.filial.longitude
    tipo_roteirizacao = req.tipo_roteirizacao
    configuracao_frota = [cfg.model_dump() for cfg in req.configuracao_frota]

    # -----------------------------------------------------------------------
    # M1 — Padronização
    # -----------------------------------------------------------------------
    try:
        df, log_m1, descartados_m1 = m1_padronizacao.executar(carteira_raw)
        logs.append(LogEtapaSchema(**log_m1))
        for d in descartados_m1:
            nao_roteirizados_total.append(CargaNaoRoteirizadaSchema(**d))
    except Exception as e:
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Falha na etapa M1 (Padronização): {str(e)}",
            rodada_id=req.rodada_id,
        )

    if df.empty:
        return RoteirizacaoResponse(
            status="erro",
            mensagem="Nenhuma carga válida após padronização. Verifique coordenadas e pesos.",
            rodada_id=req.rodada_id,
            encadeamento=logs,
        )

    # -----------------------------------------------------------------------
    # M2 — Triagem Operacional
    # -----------------------------------------------------------------------
    try:
        df_ded, df_rest, df_liv, log_m2, sem_veiculo = m2_triagem.executar(df, veiculos)
        logs.append(LogEtapaSchema(**log_m2))
        for d in sem_veiculo:
            nao_roteirizados_total.append(CargaNaoRoteirizadaSchema(**d))
    except Exception as e:
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Falha na etapa M2 (Triagem): {str(e)}",
            rodada_id=req.rodada_id,
            encadeamento=logs,
        )

    # -----------------------------------------------------------------------
    # M3 — Distâncias
    # -----------------------------------------------------------------------
    try:
        df_ded, log_m3a = m3_distancias.executar(df_ded, filial_lat, filial_lon)
        df_rest, _ = m3_distancias.executar(df_rest, filial_lat, filial_lon)
        df_liv, _ = m3_distancias.executar(df_liv, filial_lat, filial_lon)
        logs.append(LogEtapaSchema(**log_m3a))
    except Exception as e:
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Falha na etapa M3 (Distâncias): {str(e)}",
            rodada_id=req.rodada_id,
            encadeamento=logs,
        )

    # -----------------------------------------------------------------------
    # M4 — Consolidação
    # -----------------------------------------------------------------------
    try:
        manifestos, nao_alocados, log_m4 = m4_consolidacao.executar(
            df_ded, df_rest, df_liv,
            veiculos, tipo_roteirizacao, configuracao_frota
        )
        logs.append(LogEtapaSchema(**log_m4))
        for d in nao_alocados:
            nao_roteirizados_total.append(CargaNaoRoteirizadaSchema(**d))
    except Exception as e:
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Falha na etapa M4 (Consolidação): {str(e)}",
            rodada_id=req.rodada_id,
            encadeamento=logs,
        )

    if not manifestos:
        return RoteirizacaoResponse(
            status="parcial",
            mensagem="Nenhum manifesto gerado. Verifique capacidade dos veículos e volume da carteira.",
            rodada_id=req.rodada_id,
            nao_roteirizados=nao_roteirizados_total,
            encadeamento=logs,
        )

    # -----------------------------------------------------------------------
    # M5 — Sequenciamento
    # -----------------------------------------------------------------------
    try:
        manifestos, log_m5 = m5_sequenciamento.executar(manifestos, filial_lat, filial_lon)
        logs.append(LogEtapaSchema(**log_m5))
    except Exception as e:
        return RoteirizacaoResponse(
            status="erro",
            mensagem=f"Falha na etapa M5 (Sequenciamento): {str(e)}",
            rodada_id=req.rodada_id,
            encadeamento=logs,
        )

    # -----------------------------------------------------------------------
    # Montagem da resposta final
    # -----------------------------------------------------------------------
    manifestos_schema = [_manifesto_to_schema(m) for m in manifestos]

    total_carteira = len(carteira_raw)
    total_roteirizado = sum(m.qtd_entregas for m in manifestos_schema)
    total_nao_rot = len(nao_roteirizados_total)
    total_fechados = sum(1 for m in manifestos_schema if m.tipo == "fechado")
    total_compostos = sum(1 for m in manifestos_schema if m.tipo == "composto")

    ocup_pesos = [m.ocupacao_peso_perc for m in manifestos_schema]
    ocup_vols = [m.ocupacao_volume_perc for m in manifestos_schema]
    ocup_media_peso = round(sum(ocup_pesos) / len(ocup_pesos), 1) if ocup_pesos else 0.0
    ocup_media_vol = round(sum(ocup_vols) / len(ocup_vols), 1) if ocup_vols else 0.0
    km_total = round(sum(m.km_total_estimado for m in manifestos_schema), 2)

    resumo = ResumoSchema(
        total_carteira=total_carteira,
        total_roteirizado=total_roteirizado,
        total_nao_roteirizado=total_nao_rot,
        total_manifestos=len(manifestos_schema),
        total_manifestos_fechados=total_fechados,
        total_manifestos_compostos=total_compostos,
        ocupacao_media_peso_perc=ocup_media_peso,
        ocupacao_media_volume_perc=ocup_media_vol,
        km_total_estimado=km_total,
    )

    status = "sucesso" if total_nao_rot == 0 else "parcial"
    mensagem = (
        f"Roteirização concluída. {total_roteirizado} cargas roteirizadas em "
        f"{len(manifestos_schema)} manifestos."
        + (f" {total_nao_rot} cargas não roteirizadas." if total_nao_rot > 0 else "")
    )

    return RoteirizacaoResponse(
        status=status,
        mensagem=mensagem,
        rodada_id=req.rodada_id,
        resumo=resumo,
        manifestos=manifestos_schema,
        nao_roteirizados=nao_roteirizados_total,
        encadeamento=logs,
    )
