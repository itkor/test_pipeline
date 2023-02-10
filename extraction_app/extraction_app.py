import requests
import json
import psycopg2
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

class NHL_API_Extractor(object):

    def __init__(self):
        # Setup connection to PostGress
        self.hostname = os.environ.get('MAIN_PG_HOSTNAME')
        self.username = os.environ.get('MAIN_PG_USER')
        self.password = os.environ.get('MAIN_PG_PASSWORD')
        self.database = os.environ.get('MAIN_PG_DB')

        try:
            ## Create/Connect to database
            self.connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password, dbname=self.database)
        except Exception as e:
            logger.error(f"Error while connecting to Postgres at: {self.hostname}")

        ## Create cursor, used to execute commands
        self.cur = self.connection.cursor()

        self.base_url = 'https://statsapi.web.nhl.com/api/v1/teams/'

    def get_columns_names(self, table_name):
        """
        :param cur: psycopg2 cursor
        :param table_name:  name of the PG table
        :return: string (all columns)
        """
        self.cur.execute(f"Select * FROM {table_name} LIMIT 0")
        colnames_lst = [desc[0] for desc in self.cur.description]
        colnames_str = ', '.join(str(x) for x in colnames_lst)
        return colnames_str

    def create_tables(self, team_json):
        """
        :param team_json: dict with current team's data
        :param cur: psycopg2 cursor
        :return: list, list (columns of single_season_stats, regular_season_rankings)
        """
        # Create single_season_stats table if none exists
        self.cur.execute("""
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
        self.cur.execute(f"""
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

        self.connection.commit()

    def import_single_season_data(self,  team_json, single_season_colnames_str):
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
            self.cur.execute(f""" insert into public.single_season_stats
                    ({single_season_colnames_str}) values ({placeholders})""", (values_lst)
                        )
            self.connection.commit()
        except Exception as e:
            logger.error(f'Something wrong with data. Unable to parse the dictionary. \n {e}')

    def import_season_rankings_data(self,  team_json, season_rankings_colnames_str):
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
            self.cur.execute(f""" insert into public.regular_season_rankings
                    ({season_rankings_colnames_str}) values ({placeholders})""", (values_lst)
                        )
            self.connection.commit()
        except Exception as e:
            logger.error('Something wrong with data. Unable to parse the dictionary')

    def run(self):
        try:
            teams_lst_response = requests.get(self.base_url,timeout=3)
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
            # Form a stats url for each team https://statsapi.web.nhl.com/api/v1/teams/30/stats
            team_url = str(self.base_url + str(i) + '/stats')
            team_json = json.loads(requests.get(team_url,timeout=3).text)

            # Creates tables on the first run if no exists
            if i == 1 and self.connection:
                self.create_tables(team_json)
                single_season_colnames_str = self.get_columns_names('single_season_stats')
                season_rankings_colnames_str = self.get_columns_names('regular_season_rankings')

            # Check if there's data in the dict
            if team_json.get('stats')[0] and team_json.get('stats')[0].get('splits'):
                # Handling type of data: statsSingleSeason
                self.import_single_season_data(team_json, single_season_colnames_str)
            else:
                logger.warning('No data in single_season_stats, skipping... ')

            # Check if there's data in the dict
            if team_json.get('stats')[1] and team_json.get('stats')[1].get('splits'):
                # Handling type of data: regularSeasonStatRankings
                self.import_season_rankings_data(team_json, season_rankings_colnames_str)
            else:
                logger.warning('No data in season_rankings_stats, skipping... ')

        # Close the connection
        self.connection.close()
        self.cur.close()

if __name__ == '__main__':

    NHL_API_Extractor().run()


