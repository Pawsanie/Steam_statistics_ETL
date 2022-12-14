from random import uniform, randint

from luigi import run, Task

from Steam_statistics_tasks.AllSteamProductsData_luigi_task import AllSteamProductsDataTask
from Steam_statistics_tasks.GetSteamProductsDataInfo_luigi_task import GetSteamProductsDataInfoTask
from Steam_statistics_tasks.SteamProductsInfoCSVJoiner_luigi_task import \
    SteamProductsInfoInfoCSVJoinerTask
# from Steam_statistics_tasks.CreateDiagrams_sluigi_task import \
#     create_diagrams_steam_statistics_luigi_task_run
"""
Steam statistics Luigi ETL.
"""


class AllSteamProductsData(AllSteamProductsDataTask):
    """
    Gets a list of products from the SteamAPI.
    """
    # Task settings:
    task_namespace: str = 'AllSteamProductsData'
    priority = 300


class GetSteamProductsDataInfo(GetSteamProductsDataInfoTask):
    """
    Parses and scrapes the list of products available on Steam.
    """
    # Task settings:
    task_namespace: str = 'GetSteamProductsDataInfo'
    priority: int = 5000
    retry_count: int = 30
    # Wait settings:
    time_wait: int = randint(1, 3)
    # time_wait: float = uniform(0.1, 0.3)

    def requires(self):
        return {'AllSteamProductsData': AllSteamProductsData()}


class SteamAppsInfoCSVJoiner(SteamProductsInfoInfoCSVJoinerTask):
    """
    Merges all raw CSV tables into one MasterData for Steam Apps.
    """
    # Task settings:
    directory_for_csv_join = 'Apps_info'
    task_namespace: str = 'SteamProductsInfo'
    priority: int = 100

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}


class SteamDLCInfoCSVJoiner(SteamProductsInfoInfoCSVJoinerTask):
    """
    Merges all raw CSV tables into one MasterData for Steam DLC.
    """
    # Task settings:
    directory_for_csv_join = 'DLC_info'
    task_namespace: str = 'SteamProductsInfo'
    priority: int = 100

    def requires(self):
        return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}


class CreateDiagramsSteamStatistics(Task):
    """
    Create diagrams for the report.
    """
    # Task settings:
    task_namespace = 'CreateDiagramsSteamStatistics'
    priority = 200
#     create_diagrams_steam_statistics_path = \
#         Parameter(significant=True,
#                   description='Path to join all CreateAppsDiagramSteamStatistics .csv')
#     date_path_part = DateParameter(default=date.today(), description='Date for root path')
#     create_diagrams_steam_logfile_path = Parameter(default="create_diagrams_steam_statistics.log",
#                                                    description='Path for ".log" file')
#     create_diagrams_steam_loglevel = Parameter(default=30, description='Log Level')

    def requires(self):
        return {'SteamAppInfoCSVJoiner': SteamAppsInfoCSVJoiner(),
                'SteamDLCInfoCSVJoiner': SteamDLCInfoCSVJoiner()}

    def run(self):
        pass
#         create_diagrams_steam_statistics_luigi_task_run(self)


if __name__ == "__main__":
    # luigi.build([task], local_scheduler=True)
    run()
