SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    -- Datas
    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,
    CAST("DT_INVEST" AS DATE) AS data_investigacao,
    CAST("DT_INTERNA" AS DATE) AS data_internacao,
    CAST("DT_OBITO" AS DATE) AS data_obito,

    -- Localização
    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,
    CAST("ID_UNIDADE" AS VARCHAR) AS id_unidade_saude,
    CAST("ID_REGIONA" AS VARCHAR) AS id_regional_saude,
    CAST("ID_MN_RESI" AS VARCHAR) AS municipio_residencia,

    -- Paciente
    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,
    CAST("CS_ESCOL_N" AS INTEGER) AS escolaridade,

    -- Sintomas
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_cefaleia,
    CAST("MIALGIA" AS INTEGER) AS sintoma_mialgia,
    CAST("EXANTEMA" AS INTEGER) AS sintoma_exantema,
    CAST("VOMITO" AS INTEGER) AS sintoma_vomito,
    CAST("NAUSEA" AS INTEGER) AS sintoma_nausea,
    CAST("DOR_COSTAS" AS INTEGER) AS sintoma_dor_costas,
    CAST("CONJUNTVIT" AS INTEGER) AS sintoma_conjuntivite,
    CAST("ARTRITE" AS INTEGER) AS sintoma_artrite,
    CAST("ARTRALGIA" AS INTEGER) AS sintoma_artralgia,
    CAST("PETEQUIA_N" AS INTEGER) AS sintoma_petequia,
    CAST("LEUCOPENIA" AS INTEGER) AS sintoma_leucopenia,
    CAST("LACO" AS INTEGER) AS sintoma_laco,
    CAST("DOR_RETRO" AS INTEGER) AS sintoma_dor_retroorbital,

    -- Doenças Pré-Existentes
    CAST("DIABETES" AS INTEGER) AS dpe_diabetes,
    CAST("HIPERTENSA" AS INTEGER) AS dpe_hipertensao,
    CAST("AUTO_IMUNE" AS INTEGER) AS dpe_autoimune,
    CAST("ACIDO_PEPT" AS INTEGER) AS dpe_acido_peptica,
    CAST("HEMATOLOG" AS INTEGER) AS dpe_hematologica,
    CAST("HEPATOPAT" AS INTEGER) AS dpe_hepatopatia,
    CAST("RENAL" AS INTEGER) AS dpe_renal,

    -- Exames
    CAST("RESUL_SORO" AS INTEGER) AS resultado_igm,
    CAST("RESUL_NS1" AS INTEGER) AS resultado_ns1,
    CAST("RESUL_PCR_" AS INTEGER) AS resultado_pcr,
    CAST("RESUL_VI_N" AS INTEGER) AS resultado_viral,
    CAST("IMUNOH_N" AS INTEGER) AS resultado_imunohisto,
    CAST("HISTOPA_N" AS INTEGER) AS resultado_histopatologia,
    CAST("SOROTIPO" AS INTEGER) AS sorotipo,
    CAST("DT_SORO" AS DATE) AS data_coleta_igm,
    CAST("DT_NS1" AS DATE) AS data_coleta_ns1,
    CAST("DT_PCR" AS DATE) AS data_coleta_pcr,

    -- Notificação
    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("CRITERIO" AS INTEGER) AS criterio_confirmacao,
    CAST("EVOLUCAO" AS INTEGER) AS evolucao,
    CAST("HOSPITALIZ" AS INTEGER) AS hospitalizacao,
    CAST("TP_NOT" AS INTEGER) AS tipo_notificacao,
    CAST("SEM_NOT" AS INTEGER) AS semana_notificacao,
    CAST("SEM_PRI" AS INTEGER) AS semana_primeiros_sintomas,
    CAST("NU_ANO" AS INTEGER) AS ano_notificacao,

    '2020' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2020') }}

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    -- Datas
    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,
    CAST("DT_INVEST" AS DATE) AS data_investigacao,
    CAST("DT_INTERNA" AS DATE) AS data_internacao,
    CAST("DT_OBITO" AS DATE) AS data_obito,

    -- Localização
    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,
    CAST("ID_UNIDADE" AS VARCHAR) AS id_unidade_saude,
    CAST("ID_REGIONA" AS VARCHAR) AS id_regional_saude,
    CAST("ID_MN_RESI" AS VARCHAR) AS municipio_residencia,

    -- Paciente
    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,
    CAST("CS_ESCOL_N" AS INTEGER) AS escolaridade,

    -- Sintomas
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_cefaleia,
    CAST("MIALGIA" AS INTEGER) AS sintoma_mialgia,
    CAST("EXANTEMA" AS INTEGER) AS sintoma_exantema,
    CAST("VOMITO" AS INTEGER) AS sintoma_vomito,
    CAST("NAUSEA" AS INTEGER) AS sintoma_nausea,
    CAST("DOR_COSTAS" AS INTEGER) AS sintoma_dor_costas,
    CAST("CONJUNTVIT" AS INTEGER) AS sintoma_conjuntivite,
    CAST("ARTRITE" AS INTEGER) AS sintoma_artrite,
    CAST("ARTRALGIA" AS INTEGER) AS sintoma_artralgia,
    CAST("PETEQUIA_N" AS INTEGER) AS sintoma_petequia,
    CAST("LEUCOPENIA" AS INTEGER) AS sintoma_leucopenia,
    CAST("LACO" AS INTEGER) AS sintoma_laco,
    CAST("DOR_RETRO" AS INTEGER) AS sintoma_dor_retroorbital,

    -- Doenças Pré-Existentes
    CAST("DIABETES" AS INTEGER) AS dpe_diabetes,
    CAST("HIPERTENSA" AS INTEGER) AS dpe_hipertensao,
    CAST("AUTO_IMUNE" AS INTEGER) AS dpe_autoimune,
    CAST("ACIDO_PEPT" AS INTEGER) AS dpe_acido_peptica,
    CAST("HEMATOLOG" AS INTEGER) AS dpe_hematologica,
    CAST("HEPATOPAT" AS INTEGER) AS dpe_hepatopatia,
    CAST("RENAL" AS INTEGER) AS dpe_renal,

    -- Exames
    CAST("RESUL_SORO" AS INTEGER) AS resultado_igm,
    CAST("RESUL_NS1" AS INTEGER) AS resultado_ns1,
    CAST("RESUL_PCR_" AS INTEGER) AS resultado_pcr,
    CAST("RESUL_VI_N" AS INTEGER) AS resultado_viral,
    CAST("IMUNOH_N" AS INTEGER) AS resultado_imunohisto,
    CAST("HISTOPA_N" AS INTEGER) AS resultado_histopatologia,
    CAST("SOROTIPO" AS INTEGER) AS sorotipo,
    CAST("DT_SORO" AS DATE) AS data_coleta_igm,
    CAST("DT_NS1" AS DATE) AS data_coleta_ns1,
    CAST("DT_PCR" AS DATE) AS data_coleta_pcr,

    -- Notificação
    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("CRITERIO" AS INTEGER) AS criterio_confirmacao,
    CAST("EVOLUCAO" AS INTEGER) AS evolucao,
    CAST("HOSPITALIZ" AS INTEGER) AS hospitalizacao,
    CAST("TP_NOT" AS INTEGER) AS tipo_notificacao,
    CAST("SEM_NOT" AS INTEGER) AS semana_notificacao,
    CAST("SEM_PRI" AS INTEGER) AS semana_primeiros_sintomas,
    CAST("NU_ANO" AS INTEGER) AS ano_notificacao,

    '2021' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2021') }}

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    -- Datas
    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,
    CAST("DT_INVEST" AS DATE) AS data_investigacao,
    CAST("DT_INTERNA" AS DATE) AS data_internacao,
    CAST("DT_OBITO" AS DATE) AS data_obito,

    -- Localização
    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,
    CAST("ID_UNIDADE" AS VARCHAR) AS id_unidade_saude,
    CAST("ID_REGIONA" AS VARCHAR) AS id_regional_saude,
    CAST("ID_MN_RESI" AS VARCHAR) AS municipio_residencia,

    -- Paciente
    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,
    CAST("CS_ESCOL_N" AS INTEGER) AS escolaridade,

    -- Sintomas
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_cefaleia,
    CAST("MIALGIA" AS INTEGER) AS sintoma_mialgia,
    CAST("EXANTEMA" AS INTEGER) AS sintoma_exantema,
    CAST("VOMITO" AS INTEGER) AS sintoma_vomito,
    CAST("NAUSEA" AS INTEGER) AS sintoma_nausea,
    CAST("DOR_COSTAS" AS INTEGER) AS sintoma_dor_costas,
    CAST("CONJUNTVIT" AS INTEGER) AS sintoma_conjuntivite,
    CAST("ARTRITE" AS INTEGER) AS sintoma_artrite,
    CAST("ARTRALGIA" AS INTEGER) AS sintoma_artralgia,
    CAST("PETEQUIA_N" AS INTEGER) AS sintoma_petequia,
    CAST("LEUCOPENIA" AS INTEGER) AS sintoma_leucopenia,
    CAST("LACO" AS INTEGER) AS sintoma_laco,
    CAST("DOR_RETRO" AS INTEGER) AS sintoma_dor_retroorbital,

    -- Doenças Pré-Existentes
    CAST("DIABETES" AS INTEGER) AS dpe_diabetes,
    CAST("HIPERTENSA" AS INTEGER) AS dpe_hipertensao,
    CAST("AUTO_IMUNE" AS INTEGER) AS dpe_autoimune,
    CAST("ACIDO_PEPT" AS INTEGER) AS dpe_acido_peptica,
    CAST("HEMATOLOG" AS INTEGER) AS dpe_hematologica,
    CAST("HEPATOPAT" AS INTEGER) AS dpe_hepatopatia,
    CAST("RENAL" AS INTEGER) AS dpe_renal,

    -- Exames
    CAST("RESUL_SORO" AS INTEGER) AS resultado_igm,
    CAST("RESUL_NS1" AS INTEGER) AS resultado_ns1,
    CAST("RESUL_PCR_" AS INTEGER) AS resultado_pcr,
    CAST("RESUL_VI_N" AS INTEGER) AS resultado_viral,
    CAST("IMUNOH_N" AS INTEGER) AS resultado_imunohisto,
    CAST("HISTOPA_N" AS INTEGER) AS resultado_histopatologia,
    CAST("SOROTIPO" AS INTEGER) AS sorotipo,
    CAST("DT_SORO" AS DATE) AS data_coleta_igm,
    CAST("DT_NS1" AS DATE) AS data_coleta_ns1,
    CAST("DT_PCR" AS DATE) AS data_coleta_pcr,

    -- Notificação
    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("CRITERIO" AS INTEGER) AS criterio_confirmacao,
    CAST("EVOLUCAO" AS INTEGER) AS evolucao,
    CAST("HOSPITALIZ" AS INTEGER) AS hospitalizacao,
    CAST("TP_NOT" AS INTEGER) AS tipo_notificacao,
    CAST("SEM_NOT" AS INTEGER) AS semana_notificacao,
    CAST("SEM_PRI" AS INTEGER) AS semana_primeiros_sintomas,
    CAST("NU_ANO" AS INTEGER) AS ano_notificacao,

    '2022' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2022') }}
