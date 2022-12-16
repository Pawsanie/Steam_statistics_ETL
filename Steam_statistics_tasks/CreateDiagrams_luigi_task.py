import logging
# from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from matplotlib import pyplot
from pandas import DataFrame, concat
from luigi import Parameter, DateParameter

from .Logging_Config import logging_config
from .Universal_luigi_task import UniversalLuigiTask
from .SteamProductsInfoCSVJoiner_luigi_task import SteamProductsInfoInfoCSVJoinerTask
# from .GetSteamProductsDataInfo_steam_statistics_luigi_task import make_flag


def get_steam_products_tags_list_and_slices_count(cors_number: int, interest_df: DataFrame
                                                  ) -> list[list[str], list[int]]:
    """
    Make slices for diagram.
    Tags and counts.
    """
    tags_column = interest_df['tags'].values()
    tags_list, count_list, result_dict = [], [], {}
    result = [tags_list, count_list]

    def executor_job():
        for tags in tags_column:
            for tag in tags:
                if tag not in result_dict:
                    result_dict.update({str(tag): 1})
                else:
                    count = result_dict.get(str(tag))
                    result_dict.update({str(tag): count + 1})
                logging.info("'" + tag + "' count: " + result_dict.get(str(tag)) + '...')

    with ThreadPoolExecutor(max_workers=cors_number) as executor:
        executor.map(executor_job)

    for key in result_dict:
        tags_list.append(key)
        count_list.append(result_dict.get(key))

    return result


def make_diagram(path_to_save_diagram: str, steam_products_tags_list: list[str], slices_from_tags_count: list[int]):
    """
    Make diagram png.
    """
    colors = ['r', 'y', 'g', 'b']

    pyplot.pie(slices_from_tags_count,
               labels=steam_products_tags_list,
               colors=colors,
               startangle=90,
               shadow=True,
               explode=(0, 0, 0.1, 0),
               radius=1.2,
               autopct='%1.1f%%')

    pyplot.savefig(fname=path_to_save_diagram,
                   # dpi='figure',
                   dpi='auto',
                   format=None,
                   metadata=None,
                   bbox_inches=None,
                   pad_inches=0.1,
                   facecolor='auto',
                   edgecolor='auto',
                   backend=None)


class CreateDiagramsSteamStatisticsTask(UniversalLuigiTask):
    """
    Create diagrams for the report.
    """
    # Luigi parameters:
    landing_path_part: str = Parameter(
        significant=True,
        description='Root path for landing task result.')
    file_mask: str = Parameter(
        significant=True,
        description='File format for landing.')
    ancestor_file_mask: str = Parameter(
        significant=True,
        description='File format for extract.')
    date_path_part: date = DateParameter(
        significant=True,
        default=date.today(),
        description='Date for root path')
    # Luigi loging parameters:
    logfile_path: str = Parameter(
        default="steam_products_info.log",
        description='Path to ".log" file')
    loglevel: int = Parameter(
        default=30,
        description='Log Level')
    # Task settings:
    task_namespace = 'CreateDiagramsSteamStatistics'
    priority = 200

    columns = [
        'app_id', 'app_name', 'developer', 'rating_all_time_percent',
        'rating_all_time_count', 'rating_30d_percent', 'rating_30d_count',
        'publisher', 'price', 'steam_release_date', 'tags', 'scan_date'
    ]

    def __int__(self):
        self.interested_aps: dict = {}
        self.interested_dlc: dict = {}

    def requires(self):
        """
        Please note that information is expected about independent tasks, about applications and addons!!!
        """
        return {'SteamAppInfoCSVJoiner': SteamProductsInfoInfoCSVJoinerTask(),  # Apps
                'SteamDLCInfoCSVJoiner': SteamProductsInfoInfoCSVJoinerTask()}  # DLC

    def run(self):
        # Logging settings:
        logging_config(self.logfile_path, int(self.loglevel))
        # Result Successor:
        interested_aps: str = self.input()['SteamAppInfoCSVJoiner']
        self.interested_aps: dict[str, DataFrame] = self.get_extract_data(interested_aps)
        interested_dlc: str = self.input()['SteamDLCInfoCSVJoiner']
        self.interested_dlc: dict[str, DataFrame] = self.get_extract_data(interested_dlc)
        # Run:
