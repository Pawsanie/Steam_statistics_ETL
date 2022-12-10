#!/bin/bash

# AllSteamProductsData args:
all_steam_products_data_path=$HOME"/Steam_ETL/Data_Lake/All_steam_products_data"
all_steam_products_data_file_mask='json'
all_steam_products_data_ancestor_file_mask=all_steam_products_data_file_mask
all_steam_products_data_file_name='AllSteamProductsData'

# GetSteamProductsDataInfo args:
get_steam_products_data_info_path=$HOME"/Steam_ETL/Data_Lake/Info_about_steam_products"
get_steam_products_data_info_logfile_path=$HOME"/Steam_ETL/Logs/steam_products_data_info.log"
get_steam_products_data_info_file_mask='csv'
get_steam_products_data_info_ancestor_file_mask='json'
get_steam_products_data_info_file_name='AllSteamProductsData'
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

# SteamDLCInfoCSVJoiner args:
steam_DLC_info_path=$HOME"/Steam_ETL/Data_Warehouse/Steam_DLC_info"

# CreateAppsDiagramSteamStatistics args:
create_diagrams_steam_statistics_path=$HOME"/Steam_ETL/Data_Warehouse/Diagram_Steam_Statistics"

# Date:
date_path_part=$(date +%F)  # Today
#date_path_part=$(date +%F --date "YYYY-MM-DD")  # Exemple


# Start:
python3 -B -m steam_statistics_luigi_ETL AllSteamProductsData.AllSteamProductsDataTask \
\
--AllSteamProductsData.AllSteamProductsDataTask-landing-path-part $all_steam_products_data_path \
--AllSteamProductsData.AllSteamProductsDataTask-date-path-part $date_path_part \
--AllSteamProductsData.AllSteamProductsDataTask-file-mask $all_steam_products_data_file_mask \
--AllSteamProductsData.AllSteamProductsDataTask-ancestor-file-mask $all_steam_products_data_ancestor_file_mask \
--AllSteamProductsData.AllSteamProductsDataTask-file-name $all_steam_products_data_file_name \
\
\
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-get-steam-products-data-info-path $get_steam_products_data_info_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-date-path-part $date_path_part \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-get-steam-products-data-info-logfile-path $get_steam_products_data_info_logfile_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-get-steam-products-data-info-loglevel $get_steam_products_data_info_loglevel \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-file-mask $get_steam_products_data_info_file_mask \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-ancestor-file-mask $get_steam_products_data_info_ancestor_file_mask \
--GetSteamProductsDataInfo.GetSteamProductsDataInfoTask-file-name $get_steam_products_data_info_file_name \
--local-scheduler
#\
#--SteamProductsInfo.SteamAppInfoCSVJoiner-steam-apps-info-path $steam_apps_info_path \
#--SteamProductsInfo.SteamAppInfoCSVJoiner-date-path-part $date_path_part \
#\
#--SteamProductsInfo.SteamDLCInfoCSVJoiner-steam-dlc-info-path $steam_DLC_info_path \
#--SteamProductsInfo.SteamDLCInfoCSVJoiner-date-path-part $date_path_part \
#\
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-create-diagrams-steam-statistics-path $create_diagrams_steam_statistics_path \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-date-path-part $date_path_part \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-create-diagrams-steam-logfile-path $get_steam_products_data_info_logfile_path \
#--CreateDiagramsSteamStatistics.CreateDiagramsSteamStatistics-create-diagrams-steam-loglevel $get_steam_products_data_info_loglevel \
#\
#\
#--local-scheduler
