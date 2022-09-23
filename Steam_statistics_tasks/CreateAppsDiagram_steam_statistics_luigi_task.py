import logging
# from multiprocessing.pool import ThreadPool
from concurrent.futures import ThreadPoolExecutor

from matplotlib import pyplot
from pandas import DataFrame, concat

from .Logging_Config import logging_config
from .Universal_steam_statistics_luigi_task import my_beautiful_task_data_landing, \
    my_beautiful_task_data_frame_merge, my_beautiful_task_universal_parser_part
from .GetSteamProductsDataInfo_steam_statistics_luigi_task import make_flag


def get_steam_products_tags_list_and_slices_count(cors_number: int, interest_df: DataFrame) -> list[list[str], list[int]]:
    columns, result_dict = ['app_id', 'app_name', 'developer', 'rating_all_time_percent',
                            'rating_all_time_count', 'rating_30d_percent', 'rating_30d_count',
                            'publisher', 'price', 'steam_release_date', 'tags', 'scan_date'], {}

    tags = interest_df['tags'].values()
    tags_list, count_list = [], []
    result = [tags_list, count_list]

    def executor_job():
        for tag in tags:
            if tag not in tags:
                result_dict.update({str(tag): 1})
            else:
                count = result_dict.get(str(tag))
                result_dict.update({str(tag): count + 1})

    with ThreadPoolExecutor(max_workers=cors_number) as executor:
        executor.map(executor_job)

    interest_df = interest_df[columns]

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


def create_apps_diagram_steam_statistics_luigi_task_run(self, day_for_landing: str):
    """
    Function for Luigi.Task.run()
    """
    logging_config(self.get_steam_products_data_info_logfile_path, int(self.get_steam_products_data_info_loglevel))
    make_flag(f"{self.get_steam_products_data_info_path}/{day_for_landing}")
