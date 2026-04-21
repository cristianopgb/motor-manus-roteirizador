from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.schemas import RoteirizacaoRequest
from app.services.pipeline_service import executar_pipeline
from app.services.response_service import montar_resposta_erro, montar_resposta_sucesso
from app.services.validation_service import validar_payload
from app.utils.json_safe import sanitizar_json_safe

logger = logging.getLogger("motor.api")
router = APIRouter()


@router.post("/roteirizar", summary="Executa pipeline de roteirização validado")
async def roteirizar(payload: RoteirizacaoRequest):
    try:
        validar_payload(payload)
        print("[PIPELINE] Executando núcleo validado")
        resultado_pipeline = executar_pipeline(payload)
        print("[PIPELINE] Execução concluída")

        resposta = montar_resposta_sucesso(resultado_pipeline)
        resposta["rodada_id"] = payload.rodada_id
        resposta["upload_id"] = payload.upload_id
        resposta["resumo_execucao"] = {
            "tipo_roteirizacao": payload.tipo_roteirizacao,
            "filial_id": payload.filial_id,
            "data_base_roteirizacao": payload.data_base_roteirizacao,
        }
        resposta["resultado_roteirizacao"] = {
            "manifestos_fechados": resposta.get("manifestos_fechados", []),
            "manifestos_compostos": resposta.get("manifestos_compostos", []),
            "nao_roteirizados": resposta.get("nao_roteirizados", []),
        }
        resposta["logs_pipeline"] = resposta.get("logs", [])
        resposta_sanitizada, substituicoes = sanitizar_json_safe(resposta)
        if substituicoes > 0:
            logger.info(
                "Sanitização JSON-safe aplicada antes do retorno: %s valores NaN/Infinity/-Infinity substituídos por None",
                substituicoes,
            )
        return resposta_sanitizada
    except ValueError as exc:
        logger.warning("Payload inválido: %s", exc)
        erro = montar_resposta_erro(str(exc), tipo_erro="VALIDACAO")
        erro_sanitizado, _ = sanitizar_json_safe(erro)
        return erro_sanitizado
    except Exception as exc:
        logger.exception("Erro fatal na roteirização")
        raise HTTPException(status_code=500, detail=str(exc))
