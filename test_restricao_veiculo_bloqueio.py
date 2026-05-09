import pandas as pd

from app.pipeline.m4_manifestos_fechados import _combo_respeita_restricao_veiculo, _veiculo_compativel_com_restricao
from app.pipeline.m5_common import grupo_respeita_restricao_veiculo, veiculo_compativel_com_restricao


VEICULOS = ["VUC", "3/4", "TOCO", "TRUCK", "CARRETA"]


def test_sem_restricao_todos_permitidos():
    for restr in [None, "", "-", "null", "nan"]:
        for v in VEICULOS:
            assert _veiculo_compativel_com_restricao(v, restr)
            assert veiculo_compativel_com_restricao(v, restr)


def test_restricao_simples_carreta_bloqueada():
    assert not _veiculo_compativel_com_restricao("CARRETA", "CARRETA")
    assert not veiculo_compativel_com_restricao("CARRETA", "CARRETA")
    for v in ["TRUCK", "TOCO", "VUC", "3/4"]:
        assert _veiculo_compativel_com_restricao(v, "CARRETA")


def test_restricao_multipla():
    restr = "CARRETA, TRUCK"
    assert not veiculo_compativel_com_restricao("CARRETA", restr)
    assert not veiculo_compativel_com_restricao("TRUCK", restr)
    for v in ["TOCO", "VUC", "3/4"]:
        assert veiculo_compativel_com_restricao(v, restr)


def test_preserva_token_tres_quartos():
    restr = "3/4, CARRETA"
    assert not veiculo_compativel_com_restricao("3/4", restr)
    assert not veiculo_compativel_com_restricao("CARRETA", restr)
    for v in ["VUC", "TOCO", "TRUCK"]:
        assert veiculo_compativel_com_restricao(v, restr)


def test_combo_duas_linhas():
    df = pd.DataFrame({"restricao_veiculo": ["CARRETA", "TRUCK"]})
    carreta = pd.Series({"tipo": "CARRETA"})
    truck = pd.Series({"tipo": "TRUCK"})
    toco = pd.Series({"tipo": "TOCO"})
    assert not _combo_respeita_restricao_veiculo(df, carreta)
    assert not _combo_respeita_restricao_veiculo(df, truck)
    assert _combo_respeita_restricao_veiculo(df, toco)
    assert not grupo_respeita_restricao_veiculo(df, carreta)
    assert grupo_respeita_restricao_veiculo(df, toco)


def test_regra_antiga_removida_toco_bloqueia_toco():
    assert not veiculo_compativel_com_restricao("TOCO", "TOCO")
    for v in ["VUC", "3/4", "TRUCK", "CARRETA"]:
        assert veiculo_compativel_com_restricao(v, "TOCO")
