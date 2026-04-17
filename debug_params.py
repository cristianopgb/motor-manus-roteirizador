"""Debug: inspecionar param_dict gerado pelo M1 a partir do payload real."""
import json, sys
sys.path.insert(0, '.')
from app.services.pipeline_service import _payload_to_dataframes
from app.pipeline.m1_padronizacao import executar_m1_padronizacao

with open('/home/ubuntu/upload/payload_swagger_corrigido_pretty.json') as f:
    payload = json.load(f)

# Mostrar a estrutura de parâmetros do payload
print("=== PARÂMETROS NO PAYLOAD ===")
params = payload.get('parametros', {})
print(f"Tipo: {type(params)}")
print(f"Valor: {params}")

print()
print("=== FILIAL NO PAYLOAD ===")
filial = payload.get('filial', {})
print(f"Filial: {filial}")

print()
print("=== DATA_BASE NO PAYLOAD ===")
# Verificar onde está a data_base
for campo in ['data_base', 'data_base_roteirizacao', 'data_referencia']:
    val = payload.get(campo)
    if val:
        print(f"  payload['{campo}'] = {val}")
    val_p = params.get(campo) if isinstance(params, dict) else None
    if val_p:
        print(f"  payload['parametros']['{campo}'] = {val_p}")

print()
print("=== DATAFRAME DE PARÂMETROS GERADO ===")
df_c, df_g, df_p, df_v = _payload_to_dataframes(payload)
print(df_p.to_string())

print()
print("=== PARAM_DICT APÓS M1 ===")
r1 = executar_m1_padronizacao(df_c, df_g, df_p, df_v)
param_dict = r1['param_dict']
print(f"Chaves: {list(param_dict.keys())}")
print(f"data_base: {param_dict.get('data_base')}")
print(f"data_base_roteirizacao: {param_dict.get('data_base_roteirizacao')}")
for k, v in param_dict.items():
    print(f"  {k}: {v}")
