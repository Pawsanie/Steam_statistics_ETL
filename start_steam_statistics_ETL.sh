#!/bin/bash

# AllSteamProductsData args:
all_steam_products_data_path=$HOME"/Steam_ETL/Data_Lake/All_steam_products_data"
all_steam_products_logfile_path=$HOME"/Steam_ETL/Logs/all_steam_products.log"
all_steam_products_data_file_mask='json'
all_steam_products_data_ancestor_file_mask='json'
all_steam_products_data_file_name='AllSteamProductsData'
all_steam_products_loglevel=30

# GetSteamProductsDataInfo args:
get_steam_products_data_info_path=$HOME"/Steam_ETL/Data_Lake/Info_about_steam_products"
get_steam_products_data_info_logfile_path=$HOME"/Steam_ETL/Logs/steam_products_data_info.log"
get_steam_products_data_info_file_mask='csv'
get_steam_products_data_info_ancestor_file_mask='json'
get_steam_products_data_info_info_loglevel=30
#log_level:
# CRITICAL - 50
# ERROR - 40
# WARNING - 30
# INFO - 20
# DEBUG - 10
# NOTSET - 0

# SteamAppInfoCSVJoiner args:
steam_apps_info_path=$HOME"/Steam_ETL/Data_Warehouse/Steam_apps_info"
steam_apps_info_logfile_path=$HOME"/Steam_ETL/Logs/steam_apps_info.log"
steam_apps_info_file_mask='csv'
steam_apps_info_ancestor_file_mask='csv'
steam_apps_info_data_file_name='SteamAppsInfo'
steam_apps_info_loglevel=30

# SteamDLCInfoCSVJoiner args:
steam_DLC_info_path=$HOME"/Steam_ETL/Data_Warehouse/Steam_DLC_info"
steam_DLC_info_logfile_path=$HOME"/Steam_ETL/Logs/steam_dlc_info.log"
steam_DLC_info_file_mask='csv'
steam_DLC_info_ancestor_file_mask='csv'
steam_DLC_info_data_file_name='SteamDLCInfo'
steam_DLC_info_loglevel=30

# CreateAppsDiagramSteamStatistics args:
create_diagrams_steam_statistics_path=$HOME"/Steam_ETL/Data_Warehouse/Diagram_Steam_Statistics"

# Date:
#date_path_part=$(date +%F)  # Today
#date_path_part=$(date +%F --date "YYYY-MM-DD")  # Exemple
date_path_part=$(date +%F --date "2022-12-10")  # Exemple


# Start:
python3 -B -m steam_statistics_luigi_ETL CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics \
\
\
--AllSteamProductsData.AllSteamProductsData-landing-path-part $all_steam_products_data_path \
--AllSteamProductsData.AllSteamProductsData-date-path-part $date_path_part \
--AllSteamProductsData.AllSteamProductsData-file-mask $all_steam_products_data_file_mask \
--AllSteamProductsData.AllSteamProductsData-ancestor-file-mask $all_steam_products_data_ancestor_file_mask \
--AllSteamProductsData.AllSteamProductsData-file-name $all_steam_products_data_file_name \
--AllSteamProductsData.AllSteamProductsData-logfile-path $all_steam_products_logfile_path \
--AllSteamProductsData.AllSteamProductsData-loglevel $all_steam_products_loglevel \
\
\
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-landing-path-part $get_steam_products_data_info_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-date-path-part $date_path_part \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-logfile-path $get_steam_products_data_info_logfile_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-loglevel $get_steam_products_data_info_info_loglevel \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-file-mask $get_steam_products_data_info_file_mask \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-ancestor-file-mask $get_steam_products_data_info_ancestor_file_mask \
\
\
--SteamProductsInfo.SteamAppInfoCSVJoiner-landing-path-part $steam_apps_info_path \
--SteamProductsInfo.SteamAppInfoCSVJoiner-date-path-part $date_path_part \
--SteamProductsInfo.SteamAppInfoCSVJoiner-logfile-path $steam_apps_info_logfile_path \
--SteamProductsInfo.SteamAppInfoCSVJoiner-loglevel $steam_apps_info_loglevel \
--SteamProductsInfo.SteamAppInfoCSVJoiner-file-mask  $steam_apps_info_file_mask \
--SteamProductsInfo.SteamAppInfoCSVJoiner-ancestor-file-mask $steam_apps_info_ancestor_file_mask \
--SteamProductsInfo.SteamAppInfoCSVJoiner-file-name $steam_apps_info_data_file_name \
\
\
--SteamProductsInfo.SteamDLCInfoCSVJoiner-landing-path-part $steam_DLC_info_path \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-date-path-part $date_path_part \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-logfile-path $steam_DLC_info_logfile_path \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-loglevel $steam_DLC_info_loglevel \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-file-mask  $steam_DLC_info_file_mask \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-ancestor-file-mask $steam_DLC_info_ancestor_file_mask \
--SteamProductsInfo.SteamDLCInfoCSVJoiner-file-name $steam_DLC_info_data_file_name \
\
\
--local-scheduler
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-landing-path-part $create_diagrams_steam_statistics_path \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-date-path-part $date_path_part \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-create-diagrams-steam-logfile-path $get_steam_products_data_info_logfile_path \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-create-diagrams-steam-loglevel $get_steam_products_data_info_loglevel \
#\
#\
#--local-scheduler
