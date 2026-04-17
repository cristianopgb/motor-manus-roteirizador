from __future__ import annotations
import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.roteirizacao import router as rot_router
from app.models.schemas import HealthResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("motor")

VERSAO = "2.0.0"

app = FastAPI(
    title="Motor de Roteirização REC Transportes",
    description="Pipeline M1→M7: Padronização → Enriquecimento → Triagem → Manifestos → Composição Territorial → Consolidação → Sequenciamento",
    version=VERSAO,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rot_router, prefix="/api/v1", tags=["Roteirização"])

@app.get("/health", response_model=HealthResponse, tags=["Sistema"])
async def health():
    return {
        "status": "ok",
        "versao": VERSAO,
        "modulos": ["M1_padronizacao","M2_enriquecimento","M3_triagem","M3_1_validacao_fronteira",
                    "M4_manifestos_fechados","M5_1_triagem_cidades","M5_2_composicao_cidades",
                    "M5_3_composicao_subregioes","M5_4_composicao_mesorregioes",
                    "M6_1_consolidacao","M6_2_complemento_ocupacao","M7_sequenciamento"],
    }

@app.get("/", tags=["Sistema"])
async def root():
    return {"motor": "REC Transportes", "versao": VERSAO, "docs": "/docs"}
