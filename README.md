# Steam statistics ETL

## Disclaimer:
**Using** some or all of the elements of this code, **You** assume **responsibility for any consequences!**<br/>

## Description of the pipeline:
This pipeline is used to collect statistical information about all games, <br/>
distributed through the Steam platform, including:
* Price
* Tags
* Publisher
* Developer
* Steam release date<br/>

Unfortunately, the [Steam Web API](https://developer.valvesoftware.com/wiki/Steam_Web_API) does not provide such information when requested directly through its [methods](https://wiki.teamfortress.com/wiki/WebAPI) at the moment.<br/>
To solve this problem, this pipeline is being developed.<br/>
The pipeline also receives directly from the Steam Web API:
* Application names
* And their id on Steam.

Additionally, the pipeline remembers the scan date.
## Installing the Required Packages:
```bash
pip install luigi
pip install pandas
pip install fake_useragent
pip install requests
pip install beautifulsoup4
pip install numpy
pip install pyarrow
```
## Launch:
If Your OS has a bash shell the ETL pipeline can be started using the bash script:
```bash
./start_steam_statistics_ETL.sh
```
The script contains an example of all the necessary arguments to run.<br/>
To launch the pipeline through this script, do not forget to make it executable.
```bash
chmod +x ./start_steam_statistics_ETL.sh
```
The script can also be run directly with python.
```bash
python3 -B -m steam_statistics_luigi_ETL SteamAppsInfo.SteamAppsInfo --local-scheduler \
--AllSteamProductsData.AllSteamProductsData-all-steam-products-data-path $all_steam_products_data_path \
--AllSteamProductsData.AllSteamProductsData-date-path-part $date_path_part \
\
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-path $get_steam_products_data_info_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-date-path-part $date_path_part \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-logfile-path $get_steam_products_data_info_logfile_path \
--GetSteamProductsDataInfo.GetSteamProductsDataInfo-get-steam-products-data-info-loglevel $get_steam_products_data_info_loglevel \
\
--SteamAppsInfo.SteamAppsInfo-steam-apps-info-path $steam_apps_info_path \
--SteamAppsInfo.SteamAppsInfo-date-path-part $date_path_part
```
The example above shows the launch of all tasks.

## Description of tasks:
AllSteamProductsData
* Retrieves a list of applications from steam Web-API.
* If the launch is not the first time, saves the difference with the previous launch as a result.
****
GetSteamProductsDataInfo
* Requests application pages received from the last task.
* Masquerades as a new user every request and waits for a random value of seconds between 3 and 6 before a new request.
* Scraping data on these pages.
* Filters out everything that is not applications and additions to them.
* Sifts DLC and saves them to a separate file from applications.
* Separately saves applications and add-ons that are not available to the request in this region.
* Saves each request to a local cache, in case the pipeline crashes.
* Reads the local cache of applications and DLC every unsuccessful instances.
* Deletes the local cache of applications and add-ons after successful instances.

Result features:
* Some apps require registration to scrape. Information about them cannot be collected.<br/>
All columns in the row about this application will be empty, except for the 'id' and 'name'.
* The fields of the 'rating_30d_percent' and 'rating_30d_count' columns can be empty if no one has left a review in the last 30 days.
* 'not_available_in_steam_now' is set in the 'price' column value if the app is no longer available in the Steam store.<br/>
Usually this situation is adjacent to the previous point.
* Some apps and DLCs may be missing tags. Most often this applies to various OSTs, 
in which case the cell will be empty.
* If the release of the application, or DLC has not yet occurred, then the cells of the steam_release_date column will be marked "in the pipelane".

****

SteamAppsInfo
* Collects the results of all successful instances of the past task and merges them into a new file containing statistics about applications.
* Fills in the empty cells 'nan'.

## Known Bugs:
* Applications that do not have a price receive as a value not 0, but literally emptiness.
* Sometimes it is not possible to scrape information about the publisher and developer of the application.
The value in the cell will be empty.

Thank you for showing interest in my work.