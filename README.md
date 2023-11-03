# Steam statistics ETL

## Disclaimer:
:warning:**Using** some or all of the elements of this code, **You** assume **responsibility for any consequences!**<br/>

:warning:The **licenses** for the technologies on which the code **depends** are subject to **change by their authors**.

## Description of the pipeline:
This pipeline is used to collect statistical information about all games, <br/>
distributed through the Steam platform, including:
* :moneybag:Price
* :label:Tags
* :globe_with_meridians:Publisher
* :hammer_and_wrench:Developer
* :date:Steam release date

Unfortunately, the [Steam Web API](https://developer.valvesoftware.com/wiki/Steam_Web_API) does not provide such information when requested directly through its [methods](https://wiki.teamfortress.com/wiki/WebAPI) at the moment.<br/>
To solve this problem, this pipeline is being developed.<br/>
The pipeline also receives directly from the Steam Web API:
* :abc:Application names
* :id:And their id on Steam.

:spiral_calendar:Additionally, the pipeline remembers the scan date.

****

## Required:
The application code is written in python and obviously depends on it.<br>
**Python** version 3.6 [Python Software Foundation License / (with) Zero-Clause BSD license (after 3.8.6 version Python)]:
* :octocat:[Python GitHub](https://github.com/python)
* :bookmark_tabs:[Python internet page](https://www.python.org/)

## Required Packages:
Used to Luigi tasks conveyor.<br>
**Luigi** [Apache License 2.0]:
* :octocat:[Luigi GitHub](https://github.com/spotify/luigi)

Used to work with tabular data.<br>
**Pandas** [BSD-3-Clause license]:
* :octocat:[Pandas GitHub](https://github.com/pandas-dev/pandas/)
* :bookmark_tabs:[Pandas internet page](https://pandas.pydata.org/)

Used to create a random user agent.<br>
**fake-useragent** [Apache-2.0 license]:
* :octocat:[fake-useragent GitHub](https://github.com/fake-useragent/fake-useragent)

Used to send requests and receive responses.<br>
**Requests** [Apache-2.0 license]:
* :octocat:[Requests GitHub](https://github.com/psf/requests)
* :bookmark_tabs:[Requests internet page](https://requests.readthedocs.io/en/latest/)

Used for scraping.<br>
**BeautifulSoup4** [MIT]:
* :octocat:[BeautifulSoup4 GitHub](https://github.com/getanewsletter/BeautifulSoup4)
* :bookmark_tabs:[BeautifulSoup4 internet page](https://www.crummy.com/software/BeautifulSoup/)

Used to bring the table cells to the desired value.<br>
**NumPy** [BSD-3-Clause license]:
* :octocat:[NumPy GitHub](https://github.com/numpy/numpy)
* :bookmark_tabs:[NumPy internet page](https://numpy.org/)

Used to save data in parquet format.<br>
**PyArrow** [Apache-2.0 license]:
* :octocat:[PyArrow GitHub](https://github.com/apache/arrow)
* :bookmark_tabs:[PyArrow internet page](https://arrow.apache.org/)

Used to monitor the progress of certain tasks from the terminal while they are running.<br>
**tqdm** [MIT/ (with) Mozilla v. 2.0]:
* :octocat:[tqdm GitHub](https://github.com/tqdm/tqdm)
* :bookmark_tabs:[tqdm internet page](https://tqdm.github.io/)

## Installing the Required Packages:
```bash
pip install luigi
pip install pandas
pip install fake_useragent
pip install requests
pip install beautifulsoup4
pip install numpy
pip install pyarrow
pip install tqdm
```
## Launch:
If Your OS has a bash shell the ETL pipeline can be started using the bash script:
```bash
./start_steam_statistics_ETL.sh
```
At the beginning of this script, the values of variables are described, <br/>
by changing the values of which you can change this pipeline.<br/>
**File location:**<br>
./:open_file_folder:Steam_statistics_ETL<br>
   └── :page_facing_up:start_steam_statistics_ETL.sh<br>
The script contains an example of all the necessary arguments to run.<br/>
To launch the pipeline through this script, do not forget to make it executable.
```bash
chmod +x ./start_steam_statistics_ETL.sh
```
The script can also be run directly with 'python' command.<br/>
**Example of one task with 'python' command:**
```bash
python3 -B -m steam_statistics_luigi_ETL AllSteamProductsData.AllSteamProductsData \
\
--AllSteamProductsData.AllSteamProductsData-landing-path-part $all_steam_products_data_path \
--AllSteamProductsData.AllSteamProductsData-date-path-part $date_path_part \
--AllSteamProductsData.AllSteamProductsData-file-mask $all_steam_products_data_file_mask \
--AllSteamProductsData.AllSteamProductsData-ancestor-file-mask $all_steam_products_data_ancestor_file_mask \
--AllSteamProductsData.AllSteamProductsData-file-name $all_steam_products_data_file_name \
--AllSteamProductsData.AllSteamProductsData-logfile-path $all_steam_products_logfile_path \
--AllSteamProductsData.AllSteamProductsData-loglevel $all_steam_products_loglevel \

```
The example above shows the launch of one task.

Also note that the task pipeline itself is described in the 'steam_statistics_luigi_ETL.py' script.<br/>
**File location:**<br>
./:open_file_folder:Steam_statistics_ETL<br>
   └── :page_facing_up:steam_statistics_luigi_ETL.py<br>

## Launch in Docker:
To run a docker container with etl, you can use a ready-made dockerfile.<br>
To do this, run the build command with administrator rights in Windows, or with sudo privileges in Unix-like systems.<br>
**Example docker build command:**
```commandline
docker build -t luigi-steam -f С:\Git\Steam_statistics_ETL\Docker\dockerfile.df С:\Git\Steam_statistics_ETL\Docker\
```
Wait for the image to build.

Start building the docker image with a shell command.<br>
**Example docker run command:**
```commandline
docker run -d -t --name Steam_Statistics luigi-steam
```

## Description of tasks:
**AllSteamProductsData**
* Retrieves a list of applications from steam Web-API.
* If the launch is not the first time, saves the difference with the previous launch as a result.
****
**GetSteamProductsDataInfo**
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

**[SteamAppInfoCSVJoiner, SteamDLCInfoCSVJoiner]**
* Collects the results of all successful instances of the past task and merges them into a new file containing statistics about applications.
* Fills in the empty cells 'nan'.

## Known Problems:
### FakeUserAgentError:
When a task tries to send a request to a page, a number of **fake-useragent errors** appear:
```text
FakeUserAgentError:
Error occurred during loading data. Trying to use cache server https://fake-useragent.herokuapp.com/browsers/0.1.11
...
urllib.error.HTTPError: HTTP Error 503: Service Unavailable
...
fake_useragent.errors.FakeUserAgentError: Maximum amount of retries reached
...
```
This is due to the outdated version of the fake-useragent.<br>
**Solution:**<br>
Update the fake-useragent with the terminal, or command line.
```bash
pip install fake-useragent --upgrade
```
### bs4.FeatureNotFound:
```text
raise FeatureNotFound:
bs4.FeatureNotFound: Couldn't find a tree builder with the features you requested: lxml. Do you need to install a parser library?
```
It looks like you don't have the lxml library installed.<br>
**Solution:**<br>
Install the lxml library with terminal, or command line.
```bash
pip install lxml
```

## Known Bugs:
* Applications that do not have a price receive as a value not 0, but literally emptiness.
* Sometimes it is not possible to scrape information about the publisher and developer of the application.
The value in the cell will be empty.

***

**Thank you** for your interest in my work.<br><br>
