from __future__ import annotations

from typing import Any, List, Literal, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class CarteiraItem(BaseModel):
    """
    Schema bruto da carteira recebida do Sistema 1.

    Diretriz deste schema:
    - aceitar o contrato novo como padrão
    - aceitar aliases de compatibilidade com layout antigo
    - aceitar aliases do dataset real/truncado usado nos testes
    - não aplicar regra de negócio aqui
    - deixar o M1 responsável pela padronização interna do pipeline
    """
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    # ============================================================
    # IDENTIFICAÇÃO / DOCUMENTO
    # ============================================================
    Filial_R: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Filial R", "Filial"),
    )
    Romane: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Romane", "Romanei"),
    )
    Filial_D: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Filial D", "Filial (origem)", "Filial "),
    )
    Serie: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Série", "Serie", "Série D"),
    )
    Nro_Doc: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Nro Doc.", "Nro Do"),
    )

    # ============================================================
    # DATAS
    # ============================================================
    Data_Des: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Data Des", "Data", "Data D"),
    )
    Data_NF: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Data NF", "Data N"),
    )
    DLE: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("D.L.E.", "DLE"),
    )
    Agendam: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Agendam.", "Agendam"),
    )

    # ============================================================
    # CARGA / PESO / VALOR
    # ============================================================
    Palet: Optional[Any] = None
    Conf: Optional[Any] = None
    Peso: Optional[Any] = None
    Vlr_Merc: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Vlr.Merc."),
    )
    Qtd: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Qtd.", "Qtd"),
    )
    Peso_Cub: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Peso Cub.", "Peso Cub", "Peso C"),
    )
    Peso_Calculo: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices(
            "Peso Calculo",
            "Peso Calculado",
            "Peso Cálculo",
        ),
    )

    # ============================================================
    # CLASSIFICAÇÃO / CLIENTES
    # ============================================================
    Classif: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Classif", "Classifi", "Classifica"),
    )
    Tomad: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Tomad", "Tomador"),
    )
    Destin: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Destin", "Destinatário", "Destina"),
    )
    Bairro: Optional[Any] = None
    Cidad: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Cidad", "Cida"),
    )
    UF: Optional[Any] = None
    NF_Serie: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("NF / Serie"),
    )
    Tipo_Ca: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Tipo Ca", "Tipo Carg"),
    )
    Tipo_Carga: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Tipo Carga", "Tipo C"),
    )
    Qtd_NF: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Qtd.NF"),
    )

    # ============================================================
    # REGIONALIDADE / OBSERVAÇÕES
    # ============================================================
    Regiao: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Região", "Regiao"),
    )
    Mesoregiao: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Mesoregião", "Mesoregiao"),
    )
    Sub_Regiao: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Sub-Região", "Sub-Regiao"),
    )
    Ocorrencias_NF: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices(
            "Ocorrências NF",
            "Ocorrências NFs",
            "Ocorrências N",
        ),
    )
    Remetente: Optional[Any] = None
    Observacao: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Observação", "Observação R"),
    )
    Ref_Cliente: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Ref Cliente"),
    )
    Cidade_Dest: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Cidade Dest.", "Cidade Dest"),
    )
    Agenda: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Agenda"),
    )
    Ultima_Ocorrencia: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Última Ocorrência", "Última"),
    )
    Status_R: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Status R", "Status"),
    )

    # ============================================================
    # GEO
    # ============================================================
    Latitude: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Latitude", "Lat.", "Lat"),
    )
    Longitude: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Longitude", "Lon.", "Lon"),
    )

    # ============================================================
    # NOVOS CAMPOS OPERACIONAIS V2
    # ============================================================
    Prioridade: Optional[Any] = None
    Restricao_Veiculo: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices(
            "Restrição Veículo",
            "Restrição Veíc",
            "Restrição Veíc.",
        ),
    )
    Carro_Dedicado: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices(
            "Carro Dedicado",
            "Veiculo Exclusivo",
            "Veículo Exclusivo",
        ),
    )
    Inicio_Ent: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Inicio Ent.", "Início Ent."),
    )
    Fim_En: Optional[Any] = Field(
        default=None,
        validation_alias=AliasChoices("Fim En", "Fim Ent.", "Fim Ent"),
    )


class Veiculo(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    id: Optional[Any] = None
    placa: Optional[Any] = None
    perfil: str
    qtd_eixos: int
    capacidade_peso_kg: float
    capacidade_vol_m3: float
    max_entregas: int
    max_km_distancia: float
    ocupacao_minima_perc: float
    filial_id: Optional[Any] = None
    ativo: bool
    tipo_frota: Optional[Any] = None
    dedicado: Optional[Any] = None


class Regionalidade(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    cidade: str
    uf: str
    mesorregiao: str
    microrregiao: str


class FilialRodada(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    id: str
    nome: str
    cidade: str
    uf: str
    latitude: float
    longitude: float


class ConfiguracaoFrotaItem(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    perfil: str
    quantidade: int


class ParametrosRoteirizacao(BaseModel):
    """
    Contexto operacional da execução.

    Observação:
    - o contrato oficial do motor mantém configuracao_frota no TOPO do payload
    - aqui ficam apenas metadados/contexto da execução
    """
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    usuario_id: str
    usuario_nome: str
    filial_id: str
    filial_nome: str
    upload_id: str
    rodada_id: str
    data_execucao: str
    data_base_roteirizacao: str
    origem_sistema: str
    modelo_roteirizacao: str
    tipo_roteirizacao: Literal["carteira", "frota"]
    filtros_aplicados: dict = Field(default_factory=dict)


class RoteirizacaoRequest(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

    rodada_id: str
    upload_id: str
    usuario_id: str
    filial_id: str
    data_base_roteirizacao: str
    tipo_roteirizacao: Literal["carteira", "frota"]

    filial: FilialRodada

    carteira: List[CarteiraItem] = Field(default_factory=list)
    veiculos: List[Veiculo] = Field(default_factory=list)
    regionalidades: List[Regionalidade] = Field(default_factory=list)

    parametros: ParametrosRoteirizacao
    configuracao_frota: List[ConfiguracaoFrotaItem] = Field(default_factory=list)
