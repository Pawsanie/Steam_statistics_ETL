#!/bin/bash
# AllSteamAppsData args:
all_steam_apps_path=$HOME"/Steam_ETL/All_steam_apps"
#date_path_part=$(date +%F)
# GetSteamAppInfo args:
get_steam_app_info_path=$HOME"/Steam_ETL/Info_about_steam_apps"

# Start:
python3 -B -m steam_statistics_luigi_ETL GetSteamAppInfo.GetSteamAppInfo --local-scheduler \
--AllSteamAppsData.AllSteamAppsData-all-steam-apps-path $all_steam_apps_path \
--GetSteamAppInfo.GetSteamAppInfo-get-steam-app-info-path $get_steam_app_info_path
