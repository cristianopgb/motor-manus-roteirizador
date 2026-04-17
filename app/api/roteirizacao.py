from __future__ import annotations
from fastapi import APIRouter, HTTPException, Request
from app.models.schemas import RoteirizacaoPayload
from app.services.pipeline_service import executar_pipeline_completo
from app.services.response_service import montar_resposta_sistema1
import logging

logger = logging.getLogger("motor.api")
router = APIRouter()

@router.post("/roteirizar", summary="Executa pipeline de roteirização M1→M7")
async def roteirizar(payload: RoteirizacaoPayload):
    try:
        payload_dict = payload.model_dump()
        resultado_bruto = executar_pipeline_completo(payload_dict)
        resposta = montar_resposta_sistema1(resultado_bruto)
        resposta["rodada_id"] = payload.rodada_id
        return resposta
    except Exception as e:
        logger.exception("Erro fatal na roteirização")
        raise HTTPException(status_code=500, detail=str(e))
