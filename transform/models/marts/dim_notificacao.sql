{{ config(
    materialized='table',
    unique_key='notificacao_key',
) }}

WITH notificacao_base AS (
    SELECT
        surrogate_key_caso AS caso_key,

        -- Informações da Notificação
        tipo_notificacao,
        semana_notificacao,
        semana_primeiros_sintomas,
        ano_notificacao,

        -- Datas
        data_notificacao,
        data_sintomas,
        data_investigacao,
        data_encerramento,
        data_internacao,
        data_obito,

        -- Classificação e Evolução
        classificacao_final,
        criterio_confirmacao,
        evolucao,
        hospitalizacao

    FROM {{ ref('stg_dengue') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['caso_key']) }} AS notificacao_key,
    caso_key,

    tipo_notificacao,
    CASE
        WHEN tipo_notificacao = 1 THEN 'Negativa'
        WHEN tipo_notificacao = 2 THEN 'Individual'
        WHEN tipo_notificacao = 3 THEN 'Surto'
        ELSE 'Não Informado'
    END AS tipo_notificacao_descricao,

    data_notificacao,
    data_sintomas,
    data_investigacao,
    data_encerramento,
    data_internacao,
    data_obito,

    semana_notificacao,
    semana_primeiros_sintomas,
    ano_notificacao,

    classificacao_final,
    CASE
        WHEN classificacao_final = 5 THEN 'Descartado'
        WHEN classificacao_final = 8 THEN 'Inconclusivo'
        WHEN classificacao_final = 10 THEN 'Dengue'
        WHEN classificacao_final = 11 THEN 'Dengue com sinais de alarme'
        WHEN classificacao_final = 12 THEN 'Dengue grave'
        ELSE 'Não Classificado'
    END AS classificacao_final_descricao,

    criterio_confirmacao,
    CASE
        WHEN criterio_confirmacao = 1 THEN 'Laboratório'
        WHEN criterio_confirmacao = 2 THEN 'Clínico-epidemiológico'
        WHEN criterio_confirmacao = 3 THEN 'Em investigação'
        ELSE 'Não Informado'
    END AS criterio_confirmacao_descricao,

    evolucao,
    CASE
        WHEN evolucao = 1 THEN 'Cura'
        WHEN evolucao = 2 THEN 'Óbito por dengue'
        WHEN evolucao = 3 THEN 'Óbito por outras causas'
        WHEN evolucao = 4 THEN 'Óbito em investigação'
        WHEN evolucao = 9 THEN 'Ignorado'
        ELSE 'Não Informado'
    END AS evolucao_descricao,

    -- Hospitalização
    hospitalizacao,
    CASE
        WHEN hospitalizacao = 1 THEN 'Sim'
        WHEN hospitalizacao = 2 THEN 'Não'
        WHEN hospitalizacao = 9 THEN 'Ignorado'
        ELSE 'Não Informado'
    END AS hospitalizacao_descricao,

    -- Métricas de Tempo
    CASE
        WHEN data_sintomas IS NOT NULL AND data_notificacao IS NOT NULL
        THEN data_notificacao - data_sintomas
        ELSE NULL
    END AS dias_sintomas_ate_notificacao,

    CASE
        WHEN data_investigacao IS NOT NULL AND data_notificacao IS NOT NULL
        THEN data_investigacao - data_notificacao
        ELSE NULL
    END AS dias_notificacao_ate_investigacao,

    CASE
        WHEN data_encerramento IS NOT NULL AND data_notificacao IS NOT NULL
        THEN data_encerramento - data_notificacao
        ELSE NULL
    END AS dias_notificacao_ate_encerramento,

    CASE
        WHEN data_internacao IS NOT NULL AND data_sintomas IS NOT NULL
        THEN data_internacao - data_sintomas
        ELSE NULL
    END AS dias_sintomas_ate_internacao,

    -- Indicadores
    CASE WHEN evolucao IN (2, 3, 4) THEN TRUE ELSE FALSE END AS evoluiu_obito,
    CASE WHEN classificacao_final IN (11, 12) THEN TRUE ELSE FALSE END AS caso_grave,
    CASE WHEN hospitalizacao = 1 THEN TRUE ELSE FALSE END AS foi_hospitalizado,
    CASE WHEN criterio_confirmacao = 1 THEN TRUE ELSE FALSE END AS confirmado_laboratorio

FROM notificacao_base
ORDER BY notificacao_key
