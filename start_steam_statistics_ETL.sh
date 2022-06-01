#!/bin/bash
# AllSteamAppsData args:
all_steam_apps_path=$HOME"/Steam_ETL/All_steam_apps"
# GetSteamAppInfo args:
get_steam_app_info_path=$HOME"/Steam_ETL/Info_about_steam_apps"
# AppInfoCSVJoiner args:
app_info_csv_joiner_path=$HOME"/Steam_ETL/App_info_csv_joiner_path"
# Date:
date_path_part=$(date +%F)  # Today
#date_path_part=$(date +%F --date "YYY-MM-DD")  # Exemple

# Start:
python3 -B -m steam_statistics_luigi_ETL AppInfoCSVJoiner.AppInfoCSVJoiner --local-scheduler \
--AllSteamAppsData.AllSteamAppsData-all-steam-apps-path $all_steam_apps_path \
--AllSteamAppsData.AllSteamAppsData-date-path-part $date_path_part \
--GetSteamAppInfo.GetSteamAppInfo-get-steam-app-info-path $get_steam_app_info_path \
--GetSteamAppInfo.GetSteamAppInfo-date-path-part $date_path_part \
--AppInfoCSVJoiner.AppInfoCSVJoiner-app-info-csv-joiner-path $app_info_csv_joiner_path \
--AppInfoCSVJoiner.AppInfoCSVJoiner-date-path-part $date_path_part
