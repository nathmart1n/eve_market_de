SELECT 
    TYPEID,
    DATE,
    AVERAGE * VOLUME AS ISKVOLUME  -- Calculate the new column ISKVOLUME
FROM {{ref('trf_jita_history')}}