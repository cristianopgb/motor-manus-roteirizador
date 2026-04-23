-- Auditoria FLAT M5.1: colunas físicas reais para snapshots de triagem de cidades

alter table public.auditoria_pipeline_modular add column if not exists peso_total_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists km_referencia_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_clientes_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists qtd_linhas_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists status_perfil_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_status_perfil_cidade text;
alter table public.auditoria_pipeline_modular add column if not exists regra_aplicada text;
alter table public.auditoria_pipeline_modular add column if not exists cidade_elegivel_m5_1 text;
alter table public.auditoria_pipeline_modular add column if not exists motivo_status_cidade_m5_1 text;
alter table public.auditoria_pipeline_modular add column if not exists ordem_cidade_m5_1 text;

create index if not exists idx_auditoria_pipeline_modular_snapshot_m5_1 on public.auditoria_pipeline_modular (snapshot_nome, modulo);
