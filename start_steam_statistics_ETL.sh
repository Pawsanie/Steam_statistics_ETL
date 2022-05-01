#!/bin/bash
# AllSteamAppsData args:
all_steam_apps_path=$HOME"/Steam_ETL/All_steam_apps"
# GetSteamAppInfo args:
get_steam_app_info_path=$HOME"/Steam_ETL/Info_about_steam_apps"
# AppInfoCSVJoiner args:
app_info_csv_joiner_path=$HOME"/Steam_ETL/App_info_csv_joiner_path"
#date_path_part=$(date +%F)

# Start:
python3 -B -m steam_statistics_luigi_ETL AppInfoCSVJoiner.AppInfoCSVJoiner --local-scheduler \
--AllSteamAppsData.AllSteamAppsData-all-steam-apps-path $all_steam_apps_path \
--GetSteamAppInfo.GetSteamAppInfo-get-steam-app-info-path $get_steam_app_info_path \
--AppInfoCSVJoiner.AppInfoCSVJoiner-app-info-csv-joiner-path $app_info_csv_joiner_path
