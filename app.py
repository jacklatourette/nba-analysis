from datetime import date, datetime
import os
from typing import List, Optional

import duckdb
from fastparquet import write as pq_write
import pandas as pd
from requests_ratelimiter import LimiterSession
import requests

# Only needed if you want to recreate the parquet files
API_KEY = "xxxxxxxxxxxxxxxxxxx"
BASE_URL = "https://v1.basketball.api-sports.io"
NBA_ID = 12
HEADER = {"x-rapidapi-key": API_KEY}

# Needed to slow down for the API's rate limit
session = LimiterSession(per_second=10)


def pull_all_nba_seasons(since: int = 2014) -> List[str]:
    """Pull all NBA seasons from the API."""
    seasons = session.get(
        f"{BASE_URL}/seasons",
        headers=HEADER,
    ).json()["response"]
    return [
        season
        for season in seasons
        if isinstance(season, str) and int(season.split("-")[0]) >= since
    ]


def pull_games_by_season(season: str, allowed_teams: List[str] = None) -> List[dict]:
    """Pull all games played in given season"""
    if allowed_teams is None:
        allowed_teams = []
    games = session.get(
        f"{BASE_URL}/games?league={NBA_ID}&season={season}",
        headers=HEADER,
    ).json()["response"]
    formatted_games = []
    for game in games:
        if len(allowed_teams) > 1 and (
            game["teams"]["home"]["name"] not in allowed_teams
            or game["teams"]["away"]["name"] not in allowed_teams
        ):
            print(
                f"Team not allowed: {(game["teams"]["home"]["name"], game["teams"]["away"]["name"])}"
            )
            continue
        formatted_game = {
            "game_id": game["id"],
            "season": season,
            "date": datetime.fromisoformat(game["date"]),
            "home_team": game["teams"]["home"]["name"],
            "away_team": game["teams"]["away"]["name"],
            "home_score": game["scores"]["home"]["total"],
            "away_score": game["scores"]["away"]["total"],
        }
        formatted_games.append(formatted_game)
    return formatted_games


def get_conference_and_division(team_id: int) -> Optional[dict]:
    """Pull groups of each team that are its division and conference"""
    # this returns a list of a single list of responses so need to go one more deep. Seems like a bug.
    print(f"Getting data for team: {team_id}...")
    groups = session.get(
        f"{BASE_URL}/standings?league=12&season=2023-2024&stage=NBA - Regular Season&team={team_id}",
        headers=HEADER,
    ).json()["response"]
    conference, division = None, None
    # Only want teams that are associated to a division and a conference.
    if len(groups) < 1:
        print(f"Skipping team... {groups}")
        return None
    for group in groups[0]:
        if "conference" in group["group"]["name"].lower():
            conference = group["group"]["name"]
        else:
            division = group["group"]["name"]
    return {"conference": conference, "division": division}


def pull_teams() -> List[dict]:
    """Pull all team data"""
    teams_formatted = []
    teams = session.get(
        f"{BASE_URL}/teams?league={NBA_ID}&season=2023-2024",
        headers=HEADER,
    ).json()["response"]
    for team in teams:
        team_id = team["id"]
        group_data = get_conference_and_division(team_id)
        if group_data is None:
            # no division or conference data found for team so ignore
            continue
        team_data = {
            "team_id": team_id,
            "team_name": team["name"],
        }
        team_data.update(group_data)
        teams_formatted.append(team_data)
    return teams_formatted


def create_nba_games_parquet(allowed_teams: List[str] = None) -> List[dict]:
    if allowed_teams is None:
        allowed_teams = []
    seasons = pull_all_nba_seasons()
    print(f"Seasons found: {seasons}")
    games = []
    for season in seasons:
        games.extend(pull_games_by_season(season, allowed_teams=allowed_teams))
        print(f"Games so far: {len(games)}")
    games_df = pd.DataFrame(games)
    pq_write("games.parquet", games_df)
    return games


def create_nba_teams_parquet() -> List[dict]:
    teams = pull_teams()
    teams_df = pd.DataFrame(teams)
    pq_write("teams.parquet", teams_df)
    return teams


class Tasks:
    """Holds all the assigned tasks."""

    def __init__(self):
        if not os.path.isfile("teams.parquet"):
            teams = create_nba_teams_parquet()
        if not os.path.isfile("games.parquet"):
            create_nba_games_parquet(
                allowed_teams=[team["team_name"] for team in teams]
            )

    def run_tasks(self):
        for t in range(1, 7):
            task = getattr(self, f"task_{t}")
            print(f"Starting Task {t}")
            try:
                result = task()
                print(result)
            except Exception as e:
                print(f"Task {t} failed with exception: {str(e)}")

    def task_1(self):
        """Write a query to retrieve the top 1- hightest scoring games in the last decade."""
        return duckdb.sql(
            """
                SELECT game_id, (home_score + away_score) AS score
                FROM games.parquet
                WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
                ORDER BY score desc LIMIT 10;
            """
        ).show(max_rows=300, max_width=500)

    def task_2(self):
        """Write a query to calculate the win-loss record for each team over the last decade."""
        return duckdb.sql(
            """
                SELECT 
                    t.team_name,
                    SUM(
                        CASE
                            WHEN t.team_name = g.home_team AND g.home_score > g.away_score THEN 1
                            WHEN t.team_name = g.away_team AND g.away_score > g.home_score THEN 1
                            ELSE 0 END
                    ) as wins,
                    SUM(
                        CASE
                            WHEN t.team_name = g.home_team AND g.home_score < g.away_score THEN 1
                            WHEN t.team_name = g.away_team AND g.away_score < g.home_score THEN 1
                            ELSE 0 END
                    ) as loses,
                FROM teams.parquet t
                RIGHT JOIN games.parquet g on t.team_name = g.home_team or t.team_name = g.away_team
                GROUP BY t.team_name
            """
        ).show(max_rows=300, max_width=500)

    def task_3(self):
        """Write a query to calculate the average points scored by each team per season over the last decade"""
        return duckdb.sql(
            """
            SELECT 
                t.team_name team,
                g.season season,
                AVG(
                    CASE 
                        WHEN t.team_name = g.home_team THEN home_score
                        WHEN t.team_name = g.away_team THEN away_score
                        ELSE NULL END
                ) as average_score
            FROM teams.parquet t
            JOIN games.parquet g ON t.team_name = g.home_team or t.team_name = g.away_team
            WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
            GROUP BY t.team_name, g.season
            ORDER BY team, season
        """
        ).show(max_rows=300, max_width=500)

    def task_4(self):
        """Write a query to determine which conference has had the most wins in the last decade"""
        return duckdb.sql(
            """
                SELECT 
                    t.conference,
                    SUM(
                        CASE
                            WHEN t.team_name = g.home_team AND g.home_score > g.away_score THEN 1
                            WHEN t.team_name = g.away_team AND g.away_score > g.home_score THEN 1
                            ELSE 0 END
                    ) as wins
                FROM teams.parquet t
                RIGHT JOIN games.parquet g on t.team_name = g.home_team or t.team_name = g.away_team
                WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
                GROUP BY t.conference
            """
        ).show(max_rows=300, max_width=500)

    def task_5(self):
        """Write a query to find the team with the highest average margin of victory in the last decade"""
        return duckdb.sql(
            """
                SELECT
                    t.team_name as name,
                    ROUND(AVG(
                        CASE
                            WHEN t.team_name = g.home_team AND g.home_score > g.away_score THEN g.home_score - g.away_score
                            WHEN t.team_name = g.away_team AND g.away_score > g.home_score THEN g.away_score - g.home_score
                            ELSE NULL END
                    ), 2) as average_margin
                FROM teams.parquet t 
                JOIN games.parquet g ON t.team_name = g.home_team OR t.team_name = g.away_team
                WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
                GROUP BY t.team_name
                ORDER BY average_margin DESC
            """
        ).show(max_rows=300, max_width=500)

    def task_6(self):
        """I went over the time limit, wanted to do it anyways. Apologies, I got stuck with rate limit issues with the API."""
        return duckdb.sql(
            """
            WITH PointsAllowed AS (
                SELECT 
                    g.season,
                    t.team_name,
                    COUNT(t.team_name) AS games_played,
                    SUM(
                        CASE
                            WHEN t.team_name = g.home_team THEN g.away_score
                            WHEN t.team_name = g.away_team THEN g.home_score
                            ELSE NULL END
                    ) AS points_allowed
                FROM teams.parquet t
                JOIN games.parquet g ON t.team_name = g.home_team OR t.team_name = g.away_team
                WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
                GROUP BY g.season, t.team_name
                ORDER BY t.team_name, g.season
            ),
            PointsScored AS (
                SELECT 
                    g.season,
                    t.team_name,
                    COUNT(t.team_name) AS games_played,
                    SUM(
                        CASE
                            WHEN t.team_name = g.home_team THEN g.home_score
                            WHEN t.team_name = g.away_team THEN g.away_score
                            ELSE NULL END
                    ) AS points_scored
                FROM teams.parquet t
                JOIN games.parquet g ON t.team_name = g.home_team OR t.team_name = g.away_team
                WHERE CAST(LEFT(season, 4) as int) >= YEAR(current_date) - 10
                GROUP BY g.season, t.team_name
                ORDER BY t.team_name, g.season
            )
            SELECT
                ps.team_name as team_name,
                ps.season as season,
                pa.games_played,
                ROUND(ps.points_scored / ps.games_played, 2) as avg_points_scored,
                ROUND(pa.points_allowed / pa.games_played, 2) as avg_points_allowed
            FROM PointsScored ps
            JOIN PointsAllowed pa ON ps.team_name = pa.team_name AND ps.season = pa.season

            ORDER BY ps.team_name, pa.season
        """
        ).show(max_rows=300, max_width=500)


if __name__ ==  """__main__""":
    Tasks().run_tasks()
