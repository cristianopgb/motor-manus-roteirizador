import pandas as pd
import pytest

from app.pipeline.m6_2_complemento_ocupacao import (
    COLS_REMANESCENTE_OBRIGATORIAS,
    _normalizar_remanescente,
)


def test_normalizar_remanescente_dataframe_vazio_sem_colunas_retorna_schema_obrigatorio():
    out = _normalizar_remanescente(pd.DataFrame())

    assert out.empty
    assert list(out.columns) == COLS_REMANESCENTE_OBRIGATORIAS


def test_normalizar_remanescente_none_retorna_schema_obrigatorio():
    out = _normalizar_remanescente(None)

    assert out.empty
    assert list(out.columns) == COLS_REMANESCENTE_OBRIGATORIAS


def test_normalizar_remanescente_com_linhas_e_sem_colunas_obrigatorias_continua_falhando():
    with pytest.raises(Exception, match="df_remanescente_m5_4 sem colunas obrigatórias"):
        _normalizar_remanescente(pd.DataFrame([{"id_linha_pipeline": "1"}]))
