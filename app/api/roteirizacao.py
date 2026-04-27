from __future__ import annotations

import logging

from fastapi import APIRouter

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
        resultado_pipeline = executar_pipeline(payload)

        resposta = montar_resposta_sucesso(resultado_pipeline)
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
        erro = montar_resposta_erro(str(exc), tipo_erro="ERRO_INTERNO")
        erro_sanitizado, _ = sanitizar_json_safe(erro)
        return erro_sanitizado
