"""
Schemas Pydantic — Contrato de entrada e saída do Motor de Roteirização REC.
"""
from __future__ import annotations
from typing import Any, List, Optional
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# ENTRADA
# ---------------------------------------------------------------------------

class FilialSchema(BaseModel):
    id: str
    nome: str
    cidade: str
    uf: str
    latitude: float
    longitude: float


class VeiculoSchema(BaseModel):
    id: str
    perfil: str
    placa: Optional[str] = None
    qtd_eixos: int = Field(..., ge=2, le=9)
    capacidade_peso_kg: float = Field(..., gt=0)
    capacidade_vol_m3: float = Field(..., gt=0)
    max_entregas: int = Field(..., gt=0)
    max_km_distancia: float = Field(..., gt=0)
    ocupacao_minima_perc: float = Field(default=0.0, ge=0.0, le=100.0)
    tipo_frota: Optional[str] = None
    ativo: bool = True


class RegionalidadeSchema(BaseModel):
    cidade: str
    uf: str
    mesorregiao: str
    microrregiao: str


class CarteiraItemSchema(BaseModel):
    # Identificação
    romane: Optional[Any] = None
    nro_doc: Optional[Any] = None
    filial_r: Optional[Any] = None
    filial_d: Optional[Any] = None
    serie: Optional[Any] = None

    # Destinatário
    destin: Optional[str] = None
    tomad: Optional[str] = None
    bairro: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Carga
    peso: Optional[float] = None
    peso_cubico: Optional[float] = None
    peso_calculo: Optional[float] = None
    vlr_merc: Optional[float] = None
    qtd: Optional[Any] = None
    tipo_carga: Optional[str] = None
    tipo_ca: Optional[str] = None

    # Datas
    data_des: Optional[str] = None
    data_nf: Optional[str] = None
    dle: Optional[str] = None
    agendam: Optional[str] = None

    # Restrições operacionais
    prioridade: Optional[str] = None          # URGENTE, ALTA, MEDIA, BAIXA, NORMAL
    restricao_veiculo: Optional[str] = None   # TRUCK, VUC, CARRETA, TOCO, etc.
    carro_dedicado: Optional[bool] = False
    inicio_entrega: Optional[str] = None      # HH:MM
    fim_entrega: Optional[str] = None         # HH:MM

    # Classificação
    classif: Optional[str] = None
    mesoregiao: Optional[str] = None
    sub_regiao: Optional[str] = None
    status_r: Optional[str] = None
    ocorrencias_nf: Optional[str] = None
    observacao: Optional[str] = None
    remetente: Optional[str] = None
    ref_cliente: Optional[str] = None

    class Config:
        extra = "allow"  # aceita campos extras sem quebrar


class ConfiguracaoFrotaSchema(BaseModel):
    perfil: str
    quantidade: int = Field(..., ge=1)


class ParametrosSchema(BaseModel):
    usuario_id: str
    usuario_nome: str
    filial_id: str
    filial_nome: str
    upload_id: str
    rodada_id: str
    data_execucao: str
    data_base_roteirizacao: str
    origem_sistema: str = "sistema_1"
    modelo_roteirizacao: str = "padrao"
    tipo_roteirizacao: str = "carteira"       # carteira | frota
    configuracao_frota: List[ConfiguracaoFrotaSchema] = []
    filtros_aplicados: dict = {}


class RoteirizacaoRequest(BaseModel):
    rodada_id: str
    upload_id: str
    usuario_id: str
    filial_id: str
    data_base_roteirizacao: str
    tipo_roteirizacao: str = "carteira"
    filial: FilialSchema
    carteira: List[CarteiraItemSchema]
    veiculos: List[VeiculoSchema]
    regionalidades: List[RegionalidadeSchema] = []
    parametros: ParametrosSchema
    configuracao_frota: List[ConfiguracaoFrotaSchema] = []


# ---------------------------------------------------------------------------
# SAÍDA
# ---------------------------------------------------------------------------

class EntregaManifestoSchema(BaseModel):
    """Uma entrega dentro de um manifesto (linha da carteira alocada)."""
    sequencia: int
    nro_doc: Optional[Any] = None
    romane: Optional[Any] = None
    destin: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    bairro: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    peso: Optional[float] = None
    peso_cubico: Optional[float] = None
    vlr_merc: Optional[float] = None
    qtd: Optional[Any] = None
    tipo_carga: Optional[str] = None
    prioridade: Optional[str] = None
    data_des: Optional[str] = None
    dle: Optional[str] = None
    agendam: Optional[str] = None
    km_origem: Optional[float] = None        # distância da origem até este ponto
    km_acumulado: Optional[float] = None     # km acumulado no manifesto


class ManifestoSchema(BaseModel):
    """Um manifesto (rota) gerado pelo motor."""
    manifesto_id: str
    tipo: str                                # "fechado" | "composto"
    veiculo_id: str
    veiculo_perfil: str
    veiculo_placa: Optional[str] = None
    qtd_eixos: int
    qtd_entregas: int
    peso_total_kg: float
    volume_total_m3: float
    ocupacao_peso_perc: float
    ocupacao_volume_perc: float
    km_total_estimado: float
    tipo_carga_predominante: Optional[str] = None
    entregas: List[EntregaManifestoSchema]


class CargaNaoRoteirizadaSchema(BaseModel):
    """Carga que não pôde ser alocada em nenhum manifesto."""
    nro_doc: Optional[Any] = None
    romane: Optional[Any] = None
    destin: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    peso: Optional[float] = None
    vlr_merc: Optional[float] = None
    motivo: str                              # motivo claro da não alocação


class LogEtapaSchema(BaseModel):
    """Encadeamento: o que entrou e saiu de cada etapa do pipeline."""
    etapa: str
    descricao: str
    qtd_entrada: int
    qtd_saida: int
    qtd_descartada: int
    detalhes: Optional[str] = None


class ResumoSchema(BaseModel):
    total_carteira: int
    total_roteirizado: int
    total_nao_roteirizado: int
    total_manifestos: int
    total_manifestos_fechados: int
    total_manifestos_compostos: int
    ocupacao_media_peso_perc: float
    ocupacao_media_volume_perc: float
    km_total_estimado: float


class RoteirizacaoResponse(BaseModel):
    status: str                              # "sucesso" | "erro" | "parcial"
    mensagem: str
    rodada_id: str
    resumo: Optional[ResumoSchema] = None
    manifestos: List[ManifestoSchema] = []
    nao_roteirizados: List[CargaNaoRoteirizadaSchema] = []
    encadeamento: List[LogEtapaSchema] = []  # entrada/saída de cada etapa
