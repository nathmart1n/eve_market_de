## Hi, this is the repo for the following Power BI report

[Report](https://app.powerbi.com/view?r=eyJrIjoiZTRmNTZjODMtYmNlOC00MDhkLTg0MTQtMGI5MmQwYTY0NzRhIiwidCI6IjM5NjU3M2NiLWYzNzgtNGI2OC05YmM4LTE1NzU1YzBjNTFmMyIsImMiOjZ9&pageName=98ff81757c46825deb00)

## Explainer

This report shows a small part of the data on Jita IV-4 (https://evemaps.dotlan.net/station/Jita_IV_-_Moon_4_-_Caldari_Navy_Assembly_Plant), the main trading hub for the online MMORPG EVE: Online.

EVE: Online is special compared to other MMORPS, in that every player (aside from a small group in China due to Chinese legal reasons) plays on the same server. So when markets fluctuate, it fluctuates for everyone. This also means people can make a living in the game by paying attention to the hundreds of trillions of ISK
that pass between hands in Jita to capitalize on price movements before they happen.

One of the ways people do this is by making reports like mine to see large-scale and long-term price movements. The in-game client does have a feature where you can view price movements, but it lacks a lot of the detail a trader would like. So I solved this by making my own, largely inspired by OZ_Eve's google sheet (https://www.youtube.com/ozeve).

There are tens of thousands of items each with thousands of days worth of trading data, meaning millions of rows of price data needs to be sifted through. I thought this would be a good application for my Data Engineering skills.

## Architecture

This project uses AWS Lambdas to pull data from CCP Games' ESI API (https://esi.evetech.net/ui/#/) and Steve Ronuken's Fuzzwork API (https://www.fuzzwork.co.uk/2021/07/17/understanding-the-eve-online-sde-1/) and stores the raw data in S3.

One lambdas job queries the /markets/{region_id}/history/ endpoint, cleans it up a bit and removes some items that I don't care about like Skins, and writes it to a json file in S3. The other job queries the SDE api Steve Ronuken has made, in order to make sure our metadata matches CCP's.

This data is then piped into Snowflake by a Snowpipe, and transformations and analysis are performed by dbt jobs. It's then pulled into Power BI for visualization.

## TODO

Include other markets. Amarr, Dodixie, Rens, and Hek are smaller hubs (MUCH smaller than Jita), but still are hubs and could be valuable to see

Automate dbt view creation with Airflow, Docker, and deploy on AWS. Right now I'm updating the report whenever I use it, would be nice to have that automated.
