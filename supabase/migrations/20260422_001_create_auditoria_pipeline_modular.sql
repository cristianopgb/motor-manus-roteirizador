create extension if not exists pgcrypto;

create table if not exists public.auditoria_pipeline_modular (
    id uuid primary key default gen_random_uuid(),
    teste_id uuid not null,
    rodada_id uuid null,
    upload_id uuid null,
    modulo text not null,
    ordem_modulo integer not null,
    tipo_roteirizacao text null,
    filial_id uuid null,
    usuario_id uuid null,
    data_base_roteirizacao timestamptz null,

    id_linha_pipeline text null,
    manifesto_id text null,
    status_linha text null,
    status_triagem text null,
    grupo_logico text null,
    etapa_origem text null,

    payload_linha_json jsonb not null,
    campos_base_json jsonb null,
    campos_calculados_json jsonb null,
    campos_auditoria_json jsonb null,

    created_at timestamptz not null default now()
);

create index if not exists idx_auditoria_pipeline_modular_teste_id
    on public.auditoria_pipeline_modular (teste_id);

create index if not exists idx_auditoria_pipeline_modular_rodada_id
    on public.auditoria_pipeline_modular (rodada_id);

create index if not exists idx_auditoria_pipeline_modular_modulo
    on public.auditoria_pipeline_modular (modulo);

create index if not exists idx_auditoria_pipeline_modular_id_linha_pipeline
    on public.auditoria_pipeline_modular (id_linha_pipeline);

create index if not exists idx_auditoria_pipeline_modular_manifesto_id
    on public.auditoria_pipeline_modular (manifesto_id);
