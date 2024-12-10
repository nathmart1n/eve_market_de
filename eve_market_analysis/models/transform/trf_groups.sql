SELECT 
    it.typeid,
    ig.groupname,
    ic.categoryname
FROM 
    {{ source('market_data', 'invtypes') }} it
JOIN 
    {{ source('market_data', 'invgroups') }} ig ON it.groupid = ig.groupid
JOIN 
    {{ source('market_data', 'invcategories') }} ic ON ig.categoryid = ic.categoryid