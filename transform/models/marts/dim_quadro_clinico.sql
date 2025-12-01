{{ config(
    materialized='table',
    unique_key='quadro_clinico_key',
) }}

WITH quadro_base AS (
    SELECT
        surrogate_key_caso AS caso_key,

        -- Sintomas Clássicos
        sintoma_febre,
        sintoma_cefaleia,
        sintoma_mialgia,
        sintoma_dor_costas,
        sintoma_dor_retroorbital,
        sintoma_artralgia,
        sintoma_artrite,
        sintoma_exantema,
        sintoma_conjuntivite,

        -- Sintomas Gastrointestinais
        sintoma_nausea,
        sintoma_vomito,

        -- Sinais de Alarme
        sintoma_petequia,
        sintoma_leucopenia,
        sintoma_laco,

        -- Doenças Pré-Existentes
        dpe_diabetes,
        dpe_hipertensao,
        dpe_autoimune,
        dpe_acido_peptica,
        dpe_hematologica,
        dpe_hepatopatia,
        dpe_renal

    FROM {{ ref('stg_dengue') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['caso_key']) }} AS quadro_clinico_key,
    caso_key,

    sintoma_febre,
    CASE WHEN sintoma_febre = 1 THEN TRUE WHEN sintoma_febre = 2 THEN FALSE ELSE NULL END AS tem_febre,

    sintoma_cefaleia,
    CASE WHEN sintoma_cefaleia = 1 THEN TRUE WHEN sintoma_cefaleia = 2 THEN FALSE ELSE NULL END AS tem_cefaleia,

    sintoma_mialgia,
    CASE WHEN sintoma_mialgia = 1 THEN TRUE WHEN sintoma_mialgia = 2 THEN FALSE ELSE NULL END AS tem_mialgia,

    sintoma_dor_costas,
    CASE WHEN sintoma_dor_costas = 1 THEN TRUE WHEN sintoma_dor_costas = 2 THEN FALSE ELSE NULL END AS tem_dor_costas,

    sintoma_dor_retroorbital,
    CASE WHEN sintoma_dor_retroorbital = 1 THEN TRUE WHEN sintoma_dor_retroorbital = 2 THEN FALSE ELSE NULL END AS tem_dor_retroorbital,

    sintoma_artralgia,
    CASE WHEN sintoma_artralgia = 1 THEN TRUE WHEN sintoma_artralgia = 2 THEN FALSE ELSE NULL END AS tem_artralgia,

    sintoma_artrite,
    CASE WHEN sintoma_artrite = 1 THEN TRUE WHEN sintoma_artrite = 2 THEN FALSE ELSE NULL END AS tem_artrite,

    sintoma_exantema,
    CASE WHEN sintoma_exantema = 1 THEN TRUE WHEN sintoma_exantema = 2 THEN FALSE ELSE NULL END AS tem_exantema,

    sintoma_conjuntivite,
    CASE WHEN sintoma_conjuntivite = 1 THEN TRUE WHEN sintoma_conjuntivite = 2 THEN FALSE ELSE NULL END AS tem_conjuntivite,

    -- Sintomas Gastrointestinais
    sintoma_nausea,
    CASE WHEN sintoma_nausea = 1 THEN TRUE WHEN sintoma_nausea = 2 THEN FALSE ELSE NULL END AS tem_nausea,

    sintoma_vomito,
    CASE WHEN sintoma_vomito = 1 THEN TRUE WHEN sintoma_vomito = 2 THEN FALSE ELSE NULL END AS tem_vomito,

    -- Sinais de Alarme
    sintoma_petequia,
    CASE WHEN sintoma_petequia = 1 THEN TRUE WHEN sintoma_petequia = 2 THEN FALSE ELSE NULL END AS tem_petequia,

    sintoma_leucopenia,
    CASE WHEN sintoma_leucopenia = 1 THEN TRUE WHEN sintoma_leucopenia = 2 THEN FALSE ELSE NULL END AS tem_leucopenia,

    sintoma_laco,
    CASE WHEN sintoma_laco = 1 THEN TRUE WHEN sintoma_laco = 2 THEN FALSE ELSE NULL END AS prova_laco_positiva,

    -- Doenças Pré-Existentes
    dpe_diabetes,
    CASE WHEN dpe_diabetes = 1 THEN TRUE WHEN dpe_diabetes = 2 THEN FALSE ELSE NULL END AS tem_diabetes,

    dpe_hipertensao,
    CASE WHEN dpe_hipertensao = 1 THEN TRUE WHEN dpe_hipertensao = 2 THEN FALSE ELSE NULL END AS tem_hipertensao,

    dpe_autoimune,
    CASE WHEN dpe_autoimune = 1 THEN TRUE WHEN dpe_autoimune = 2 THEN FALSE ELSE NULL END AS tem_doenca_autoimune,

    dpe_acido_peptica,
    CASE WHEN dpe_acido_peptica = 1 THEN TRUE WHEN dpe_acido_peptica = 2 THEN FALSE ELSE NULL END AS tem_doenca_acido_peptica,

    dpe_hematologica,
    CASE WHEN dpe_hematologica = 1 THEN TRUE WHEN dpe_hematologica = 2 THEN FALSE ELSE NULL END AS tem_doenca_hematologica,

    dpe_hepatopatia,
    CASE WHEN dpe_hepatopatia = 1 THEN TRUE WHEN dpe_hepatopatia = 2 THEN FALSE ELSE NULL END AS tem_hepatopatia,

    dpe_renal,
    CASE WHEN dpe_renal = 1 THEN TRUE WHEN dpe_renal = 2 THEN FALSE ELSE NULL END AS tem_doenca_renal,

    -- Contadores de sintomas
    (CASE WHEN sintoma_febre = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_cefaleia = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_mialgia = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_dor_costas = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_dor_retroorbital = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_artralgia = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_artrite = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_exantema = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_conjuntivite = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_nausea = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_vomito = 1 THEN 1 ELSE 0 END) AS total_sintomas,

    (CASE WHEN sintoma_petequia = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_leucopenia = 1 THEN 1 ELSE 0 END +
     CASE WHEN sintoma_laco = 1 THEN 1 ELSE 0 END) AS total_sinais_alarme,

    (CASE WHEN dpe_diabetes = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_hipertensao = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_autoimune = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_acido_peptica = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_hematologica = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_hepatopatia = 1 THEN 1 ELSE 0 END +
     CASE WHEN dpe_renal = 1 THEN 1 ELSE 0 END) AS total_comorbidades

FROM quadro_base
ORDER BY quadro_clinico_key
