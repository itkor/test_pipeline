import requests
import json
import psycopg2
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Setup connection to PostGress
hostname = 'localhost'
username = 'pgadmin'
password ='pgadminpass'
database = 'nhldb'

try:
    ## Create/Connect to database
    connection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
except Exception as e:
    logger.error(f"Error while connecting to Postgres at: {hostname}")

## Create cursor, used to execute commands
cur = connection.cursor()


def get_columns_names(cur, table_name):
    """
    :param cur: psycopg2 cursor
    :param table_name:  name of the PG table
    :return: string (all columns)
    """
    cur.execute(f"Select * FROM {table_name} LIMIT 0")
    colnames_lst = [desc[0] for desc in cur.description]
    colnames_str = ', '.join(str(x) for x in colnames_lst)
    return colnames_str

def create_tables(team_json, cur=cur):
    """
    :param team_json: dict with current team's data
    :param cur: psycopg2 cursor
    :return: list, list (columns of single_season_stats, regular_season_rankings)
    """
    # Create single_season_stats table if none exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.single_season_stats(
            type_displayName CHAR(30),
            gameType_id CHAR(5), 
            gameType_desc CHAR(30),
            gameType_postseason BOOLEAN,
            team_id SMALLINT,
            team_name CHAR(50),
            team_link CHAR(50),
            gamesPlayed SMALLINT,
            wins SMALLINT,
            losses SMALLINT,
            ot SMALLINT,
            pts SMALLINT,
            ptPctg  CHAR(8),
            goalsPerGame FLOAT(4),
            goalsAgainstPerGame FLOAT(4),
            evGGARatio FLOAT(4),
            powerPlayPercentage CHAR(8),
            powerPlayGoals FLOAT(4),
            powerPlayGoalsAgainst FLOAT(4),
            powerPlayOpportunities FLOAT(4),
            penaltyKillPercentage  CHAR(8),
            shotsPerGame FLOAT(4),
            shotsAllowed FLOAT(4),
            winScoreFirst FLOAT(4),
            winOppScoreFirst FLOAT(4),
            winLeadFirstPer FLOAT(4),
            winLeadSecondPer FLOAT(4),
            winOutshootOpp FLOAT(4),
            winOutshotByOpp FLOAT(4),
            faceOffsTaken FLOAT(4),
            faceOffsWon FLOAT(4),
            faceOffsLost FLOAT(4),
            faceOffWinPercentage  CHAR(8),
            shootingPctg FLOAT(4),
            savePctg  FLOAT(4),
            created_at timestamp NOT NULL,
            PRIMARY KEY(team_id, created_at),
            CONSTRAINT no_duplicate_record UNIQUE (team_id, created_at)
    )
    """)

    # Generate column names for the major part of attributes
    col_names_lst = []
    col_names_lst.extend(team_json.get('stats')[1].get('splits')[0].get('stat').keys())
    colnames_str = ' CHAR(7), '.join(str(x) for x in col_names_lst)

    # Create table regular_season_rankings if NONE exists
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS public.regular_season_rankings(
            type_displayName CHAR(30),
            type_gameType CHAR(5),
            team_id SMALLINT,
            team_name CHAR(50),
            team_link CHAR(50),
            {colnames_str} CHAR(7),
            created_at timestamp NOT NULL,
            PRIMARY KEY(team_id, created_at),
            CONSTRAINT no_duplicate_ranking UNIQUE (team_id, created_at)
        )
        """)


def import_single_season_data(cur, team_json, single_season_colnames_str):
    """
    :param cur:  psycopg2 cursor
    :param team_json:  dict with current team's data
    :param single_season_colnames_str: string of columns of single_season_stats
    :return: None
    """
    # Get values from the received JSON
    try:
        values_lst = []
        values_lst.append(team_json.get('stats')[0].get('type').get('displayName'))
        values_lst.extend(team_json.get('stats')[0].get('type').get('gameType').values())
        values_lst.extend(team_json.get('stats')[0].get('splits')[0].get('team').values())
        values_lst.extend(team_json.get('stats')[0].get('splits')[0].get('stat').values())
        values_lst.append(datetime.now(timezone.utc))

        # Generate placeholders for the INSERT query
        placeholders = ', '.join(['%s'] * len(values_lst))
        ## Define insert statement
        cur.execute(f""" insert into public.single_season_stats
                ({single_season_colnames_str}) values ({placeholders})""", (values_lst)
                    )
    except Exception as e:
        logger.error('Something wrong with data. Unable to parse the dictionary')

def import_season_rankings_data(cur, team_json, season_rankings_colnames_str):
    """
    :param cur:  psycopg2 cursor
    :param team_json:  dict with current team's data
    :param season_rankings_colnames_str: string of columns of regular_season_rankings
    :return: None
    """
    try:
        # Get values from the received JSON
        values_lst = []
        values_lst.extend(team_json.get('stats')[1].get('type').values())
        values_lst.extend(team_json.get('stats')[1].get('splits')[0].get('team').values())
        values_lst.extend(team_json.get('stats')[1].get('splits')[0].get('stat').values())
        values_lst.append(datetime.now(timezone.utc))

        # Generate placeholders for the INSERT query
        placeholders = ', '.join(['%s'] * len(values_lst))
        ## Define insert statement
        cur.execute(f""" insert into public.regular_season_rankings
                ({season_rankings_colnames_str}) values ({placeholders})""", (values_lst)
                    )
    except Exception as e:
        logger.error('Something wrong with data. Unable to parse the dictionary')



base_url = 'https://statsapi.web.nhl.com/api/v1/teams/'

try:
    teams_lst_response = requests.get(base_url,timeout=3)
    teams_lst_response.raise_for_status()
except requests.exceptions.HTTPError as errh:
    logger.error("Http Error:",errh)
except requests.exceptions.ConnectionError as errc:
    logger.error("Error Connecting:",errc)
except requests.exceptions.Timeout as errt:
    logger.error("Timeout Error:",errt)
except requests.exceptions.RequestException as err:
    logger.error("Another error",err)

teams_lst_data = teams_lst_response.text
teams_lst_json = json.loads(teams_lst_data)


team_num = teams_lst_json['teams'][-1].get('id')

# Iterate over teams for requesting data
for i in range(1,team_num+1):
    print(i)
    # Form a stats url for each team https://statsapi.web.nhl.com/api/v1/teams/30/stats
    team_url = str(base_url + str(i) + '/stats')
    team_json = json.loads(requests.get(team_url,timeout=3).text)

    # Creates tables on the first run if no exists
    if i == 1 and connection:
        create_tables(team_json, cur)
        single_season_colnames_str = get_columns_names(cur, 'single_season_stats')
        season_rankings_colnames_str = get_columns_names(cur, 'regular_season_rankings')

    # Check if there's data in the dict
    if team_json.get('stats')[0] and team_json.get('stats')[0].get('splits'):
        # Handling type of data: statsSingleSeason
        import_single_season_data(cur, team_json, single_season_colnames_str)
    else:
        logger.warning('No data in single_season_stats, skipping... ')

    # Check if there's data in the dict
    if team_json.get('stats')[1] and team_json.get('stats')[1].get('splits'):
        # Handling type of data: regularSeasonStatRankings
        import_season_rankings_data(cur, team_json, season_rankings_colnames_str)
    else:
        logger.warning('No data in season_rankings_stats, skipping... ')

connection.commit()
connection.close()
cur.close()