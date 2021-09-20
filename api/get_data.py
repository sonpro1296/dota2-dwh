from datetime import datetime

import requests
import pandas as pd
import time

api = "https://api.opendota.com/api/"

match_api = api + "proMatches"
players_api = api + "proPlayers"
heroes_api = api + "heroes"
items_api = api + "explorer?sql=SELECT%20*%20FROM%20items"
teams_api = api + "teams"

hero_image_api_prefix = "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/heroes/"
item_image_api_prefix = "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/items/"

match_info_api_prefix = api + "matches/"
player_info_api_prefix = api + "players/"  # need account id
teams_info_api_prefix = api + "teams/"
heroes_info_api_prefix = api + "heroes/"

record_headers = ["match_id", "player_id", "team_id", "won", "kills", "deaths", "assists", "item_0", "item_1", "item_2",
                  "item_3", "item_4", "item_5", "hero_id", "date_id"]

teams_header = ["team_id", "team_name"]
items_header = ["item_id", "item_name", "localized_name"]
heroes_header = ["hero_id", "hero_name", "primary_attr", "attack_type", "localized_name"]
players_header = ["player_id", "player_name"]


def get_teams() -> pd.DataFrame:
    team_response = requests.get(teams_api).json()
    team_list = list()
    for team in team_response:
        team_info = list()
        team_info.append(team["team_id"])
        team_info.append(team["team_name"])
        team_list.append(team_info)

    return pd.DataFrame(team_list, columns=teams_header, index=None)


def get_items() -> pd.DataFrame:
    item_response = requests.get(items_api).json()
    item_list = list()
    for item in item_response["rows"]:
        item_info = list()
        item_info.append(item["id"])
        item_info.append(item["localized_name"])
        item_info.append(str(item["name"]).replace("item_", ""))
        item_list.append(item_info)
    return pd.DataFrame(item_list, columns=items_header, index=None)


def get_heroes() -> pd.DataFrame:
    hero_response = requests.get(heroes_api).json()
    hero_list = list()
    for hero in hero_response:
        hero_info = list()
        hero_info.append(hero["id"])
        hero_info.append(hero["localized_name"])
        hero_info.append(hero["primary_attr"])
        hero_info.append(hero["attack_type"])
        hero_info.append(str(hero["name"]).replace("npc_dota_hero_", ""))
        hero_list.append(hero_info)
    return pd.DataFrame(hero_list, columns=heroes_header, index=None)


def get_match_info(match_id) -> pd.DataFrame:
    match_info_api = match_info_api_prefix + str(match_id)
    match_info = requests.get(match_info_api).json()
    print(match_id, match_info)
    player_info = match_info["players"]
    factList = list()
    timestamp = match_info["start_time"]
    dt_object = datetime.fromtimestamp(timestamp)
    for player in player_info:
        tmpList = list()
        tmpList.append(match_id)
        tmpList.append(player["account_id"])
        if player["isRadiant"]:
            team = match_info["radiant_team_id"]
        else:
            team = match_info["dire_team_id"]
        if team is None:
            team = 0
        else:
            team = int(team)
        tmpList.append(team)
        tmpList.append(player["win"])
        tmpList.append(player["kills"])
        tmpList.append(player["deaths"])
        tmpList.append(player["assists"])
        tmpList.append(player["item_0"])
        tmpList.append(player["item_1"])
        tmpList.append(player["item_2"])
        tmpList.append(player["item_3"])
        tmpList.append(player["item_4"])
        tmpList.append(player["item_5"])
        tmpList.append(player["hero_id"])
        tmpList.append(dt_object)
        factList.append(tmpList)

    df = pd.DataFrame(factList, columns=record_headers, index=None)
    time.sleep(1)
    return df


def get_team_by_ids(team_ids: list) -> pd.DataFrame:
    team_list = list()
    pro_teams = requests.get(teams_api).json()
    for team in pro_teams:
        if team["team_id"] in team_ids:
            team_info = list()
            team_info.append(team["team_id"])
            team_info.append(team["name"])
            team_list.append(team_info)
            team_ids.remove(team["team_id"])

    for team_id in team_ids:
        print(teams_info_api_prefix + str(team_id))
        try:
            team_response = requests.get(teams_info_api_prefix + str(team_id)).json()
            if team_response is None:
                team_list.append([team_id, "Unknown Team"])
                continue
            team_info = list()
            team_info.append(team_response["team_id"])
            team_info.append(team_response["name"])
            team_list.append(team_info)
            time.sleep(1)
        except Exception:
            team_list.append([team_id, "Unknown Team"])
    return pd.DataFrame(team_list, columns=teams_header, index=None)


def get_players_by_ids(players_ids: list) -> pd.DataFrame:
    player_list = list()
    pro_players = requests.get(players_api).json()
    for player in pro_players:
        if player["account_id"] in players_ids:
            player_info = list()
            player_info.append(player["account_id"])
            player_info.append(player["name"])
            player_list.append(player_info)
            players_ids.remove(player["account_id"])
    for player_id in players_ids:
        try:
            player_response = requests.get(player_info_api_prefix + str(player_id)).json()
            if player_response is None:
                player_list.append([player_id, "Unknown Player"])
                continue
            player_info = list()
            player_profile = player_response["profile"]
            player_info.append(player_profile["account_id"])
            player_info.append(player_profile["name"])
            player_list.append(player_info)
            time.sleep(1)
        except Exception:
            player_list.append([player_id, "Unknown Player"])
    return pd.DataFrame(player_list, columns=players_header, index=None)


def get_matches(last_match: int) -> list:
    matches = list()
    matches_response = requests.get(match_api).json()
    for match in matches_response:
        if int(match["match_id"]) <= last_match:
            break
        matches.append(match["match_id"])
    return matches
