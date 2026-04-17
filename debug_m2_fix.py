"""Verificar o que o M2 retorna e se folga_dias está correto."""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import _payload_to_dataframes
from app.pipeline.m1_padronizacao import executar_m1_padronizacao
from app.pipeline.m2_enriquecimento import executar_m2_enriquecimento
import pandas as pd

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

df_c, df_g, df_p, df_v = _payload_to_dataframes(payload)
r1 = executar_m1_padronizacao(df_c, df_g, df_p, df_v)
r2 = executar_m2_enriquecimento(r1['df_carteira_tratada'], r1['df_geo_tratado'], r1['param_dict'])

print("=== RETORNO DO M2 (chaves) ===")
print(list(r2.keys()))
print(f"data_base: {r2.get('data_base')}")
print(f"fator_km: {r2.get('fator_km')}")

df = r2['df_carteira_enriquecida']
folga = pd.to_numeric(df['folga_dias'], errors='coerce')

print()
print("=== DISTRIBUIÇÃO DE folga_dias APÓS CORREÇÃO ===")
print(f"  Nulos: {folga.isna().sum()}")
print(f"  < 0 (agenda_vencida): {(folga < 0).sum()}")
print(f"  == 0: {(folga == 0).sum()}")
print(f"  == 1: {(folga == 1).sum()}")
print(f"  >= 2 (futuro): {(folga >= 2).sum()}")
print(f"  0 <= folga < 2 (roteirizavel): {((folga >= 0) & (folga < 2)).sum()}")

print()
print("=== AMOSTRA: data_limite_considerada e folga_dias ===")
cols = [c for c in ['nro_documento', 'destin', 'cidade', 'data_descarga', 'data_leadtime',
                    'data_agenda', 'agendada', 'data_limite_considerada', 'folga_dias'] if c in df.columns]
print(df[cols].head(10).to_string())
