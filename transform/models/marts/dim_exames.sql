{{ config(
    materialized='table',
    unique_key='exame_key',
) }}

WITH exames_base AS (
    SELECT
        surrogate_key_caso AS caso_key,

        resultado_igm,
        resultado_ns1,
        resultado_pcr,
        resultado_viral,
        resultado_imunohisto,
        resultado_histopatologia,
        sorotipo,

        data_coleta_igm,
        data_coleta_ns1,
        data_coleta_pcr

    FROM {{ ref('stg_dengue') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['caso_key']) }} AS exame_key,
    caso_key,

    -- Resultado IgM (Sorologia)
    resultado_igm,
    CASE
        WHEN resultado_igm = 1 THEN 'Positivo'
        WHEN resultado_igm = 2 THEN 'Negativo'
        WHEN resultado_igm = 3 THEN 'Inconclusivo'
        WHEN resultado_igm = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_igm_descricao,
    data_coleta_igm,

    -- Resultado NS1 (Antígeno)
    resultado_ns1,
    CASE
        WHEN resultado_ns1 = 1 THEN 'Positivo'
        WHEN resultado_ns1 = 2 THEN 'Negativo'
        WHEN resultado_ns1 = 3 THEN 'Inconclusivo'
        WHEN resultado_ns1 = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_ns1_descricao,
    data_coleta_ns1,

    -- Resultado RT-PCR (Molecular)
    resultado_pcr,
    CASE
        WHEN resultado_pcr = 1 THEN 'Positivo'
        WHEN resultado_pcr = 2 THEN 'Negativo'
        WHEN resultado_pcr = 3 THEN 'Inconclusivo'
        WHEN resultado_pcr = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_pcr_descricao,
    data_coleta_pcr,

    -- Isolamento Viral
    resultado_viral,
    CASE
        WHEN resultado_viral = 1 THEN 'Positivo'
        WHEN resultado_viral = 2 THEN 'Negativo'
        WHEN resultado_viral = 3 THEN 'Inconclusivo'
        WHEN resultado_viral = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_viral_descricao,

    -- Imunohistoquímica
    resultado_imunohisto,
    CASE
        WHEN resultado_imunohisto = 1 THEN 'Positivo'
        WHEN resultado_imunohisto = 2 THEN 'Negativo'
        WHEN resultado_imunohisto = 3 THEN 'Inconclusivo'
        WHEN resultado_imunohisto = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_imunohisto_descricao,

    -- Histopatologia
    resultado_histopatologia,
    CASE
        WHEN resultado_histopatologia = 1 THEN 'Positivo'
        WHEN resultado_histopatologia = 2 THEN 'Negativo'
        WHEN resultado_histopatologia = 3 THEN 'Inconclusivo'
        WHEN resultado_histopatologia = 4 THEN 'Não Realizado'
        ELSE 'Não Informado'
    END AS resultado_histopatologia_descricao,

    -- Sorotipo (1, 2, 3, 4)
    sorotipo,
    CASE
        WHEN sorotipo = 1 THEN 'DENV-1'
        WHEN sorotipo = 2 THEN 'DENV-2'
        WHEN sorotipo = 3 THEN 'DENV-3'
        WHEN sorotipo = 4 THEN 'DENV-4'
        ELSE 'Não Identificado'
    END AS sorotipo_descricao,

    -- Indicadores de confirmação laboratorial
    CASE
        WHEN resultado_igm = 1 OR resultado_ns1 = 1 OR resultado_pcr = 1 OR resultado_viral = 1
        THEN TRUE
        ELSE FALSE
    END AS confirmado_laboratorialmente,

    CASE
        WHEN resultado_pcr = 1 OR resultado_viral = 1 THEN 'Molecular'
        WHEN resultado_ns1 = 1 THEN 'Antigênico'
        WHEN resultado_igm = 1 THEN 'Sorológico'
        WHEN resultado_imunohisto = 1 OR resultado_histopatologia = 1 THEN 'Histopatológico'
        ELSE 'Não Confirmado'
    END AS tipo_confirmacao_laboratorial,

    (CASE WHEN resultado_igm IS NOT NULL AND resultado_igm != 4 THEN 1 ELSE 0 END +
     CASE WHEN resultado_ns1 IS NOT NULL AND resultado_ns1 != 4 THEN 1 ELSE 0 END +
     CASE WHEN resultado_pcr IS NOT NULL AND resultado_pcr != 4 THEN 1 ELSE 0 END +
     CASE WHEN resultado_viral IS NOT NULL AND resultado_viral != 4 THEN 1 ELSE 0 END +
     CASE WHEN resultado_imunohisto IS NOT NULL AND resultado_imunohisto != 4 THEN 1 ELSE 0 END +
     CASE WHEN resultado_histopatologia IS NOT NULL AND resultado_histopatologia != 4 THEN 1 ELSE 0 END) AS total_exames_realizados

FROM exames_base
ORDER BY exame_key
