SELECT 
    *
FROM {{ source('market_data', 'eve_market_history') }}