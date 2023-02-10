## NHL API Extractor

A simple app for extracting data from NHL public API to Postgres.

API message example: https://statsapi.web.nhl.com/api/v1/teams/21/stats

The endpoint reponse's data is of two main types: statsSingleSeason and regularSeasonStatRankings. 

Thus, the data is stored in two tables: single_season_stats and regular_season_rankings.

***
### How to launch:

1) Copy the repository

2) Run services
          
          docker-compose up -d

3) Stop services

          docker-compose down
   
*** 

## Access details:

Airflow:      http://127.0.0.1:8080/

PGAdmin:      http://127.0.0.1:5050/

Postgres (Main DB):

              localhost, db:nhldb, user:pgadmin, password:pgadminpass, port:5432
              
Postgres (Airflow):

              localhost, db:airflow, user:airflow, password:airflow, port:5434
