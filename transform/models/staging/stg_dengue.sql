SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,

    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,

    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,

    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_dor_cabeca,
    
    '2020' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2020') }}

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,

    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,

    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,

    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_dor_cabeca,

    '2021' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2021') }}

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['"NU_IDADE_N"', '"DT_NOTIFIC"', '"SG_UF_NOT"']) }} as surrogate_key_caso,

    CAST("DT_NOTIFIC" AS DATE) AS data_notificacao,
    CAST("DT_SIN_PRI" AS DATE) AS data_sintomas,
    CAST("DT_ENCERRA" AS DATE) AS data_encerramento,

    CAST("ID_MUNICIP" AS VARCHAR) AS id_municipio,
    CAST("SG_UF_NOT" AS VARCHAR) AS uf_notificacao,

    CAST("NU_IDADE_N" AS INTEGER) AS idade,
    CAST("CS_SEXO" AS VARCHAR) AS sexo,
    CAST("CS_GESTANT" AS INTEGER) AS gestante,
    CAST("CS_RACA" AS INTEGER) AS raca,

    CAST("CLASSI_FIN" AS INTEGER) AS classificacao_final,
    CAST("FEBRE" AS INTEGER) AS sintoma_febre,
    CAST("CEFALEIA" AS INTEGER) AS sintoma_dor_cabeca,

    '2022' AS ano_fonte
FROM {{ source('dengue_raw', 'dengue_raw_2022') }}
