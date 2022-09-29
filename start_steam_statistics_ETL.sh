#!/bin/bash

# AllSteamProductsData args:
all_steam_products_data_path=$HOME"/Steam_ETL/Data_Lake/All_steam_products_data"

# GetSteamProductsDataInfo args:
get_steam_products_data_info_path=$HOME"/Steam_ETL/Data_Lake/Info_about_steam_products"
get_steam_products_data_info_logfile_path=$HOME"/Steam_ETL/Logs/steam_products_data_info.log"
get_steam_products_data_info_loglevel=30
#log_level:
# CRITICAL - 50
# ERROR - 40
# WARNING - 30
# INFO - 20
# DEBUG - 10
# NOTSET - 0

# SteamAppInfoCSVJoiner args:
steam_apps_info_path=$HOME"/Steam_ETL/Data_Warehouse/Steam_apps_info"

# SteamDLCInfoCSVJoiner args:
steam_DLC_info_path=$HOME"/Steam_ETL/Data_Warehouse/Steam_DLC_info"

# Date:
date_path_part=$(date +%F)  # Today
#date_path_part=$(date +%F --date "YYYY-MM-DD")  # Exemple


# Start:
python3 -B -m steam_statistics_luigi_ETL SteamDLCInfoCSVJoiner.SteamDLCInfoCSVJoiner \
\
--AllSteamProductsData.AllSteamProductsData-all-steam-products-data-path $all_steam_products_data_path \
--AllSteamProductsData.AllSteamProductsData-date-path-part $date_path_part \
\
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-path $get_steam_products_data_info_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-date-path-part $date_path_part \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-logfile-path $get_steam_products_data_info_logfile_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-loglevel $get_steam_products_data_info_loglevel \
\
--SteamProductsInfo.SteamAppInfoCSVJoiner-steam-apps-info-path $steam_apps_info_path \
--SteamProductsInfo.SteamAppInfoCSVJoiner-date-path-part $date_path_part \
\
--SteamProductsInfo.SteamDLCInfoCSVJoiner-steam-apps-info-path $steam_DLC_info_path \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-date-path-part $date_path_part \
\
\
--local-scheduler
