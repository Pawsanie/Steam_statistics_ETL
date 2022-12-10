from luigi import run, Task, LocalTarget, DateParameter, Parameter

from Steam_statistics_tasks.AllSteamProductsData_steam_statistics_luigi_task import AllSteamProductsDataTask
from Steam_statistics_tasks.GetSteamProductsDataInfo_steam_statistics_luigi_task import GetSteamProductsDataInfoTask
# from Steam_statistics_tasks.SteamProductsInfoCSVJoiner_universal_steam_statistics_luigi_task import \
#     steam_products_info_run
# from Steam_statistics_tasks.CreateDiagrams_steam_statistics_luigi_task import \
#     create_diagrams_steam_statistics_luigi_task_run
"""
Steam statistics Luigi ETL.
"""


class AllSteamProductsData(AllSteamProductsDataTask):
    """
    Gets a list of products from the SteamAPI.
    """


class GetSteamProductsDataInfo(GetSteamProductsDataInfoTask):
    """
    Parses and scrapes the list of products available on Steam.
    """
    def requires(self):
        return {'AllSteamProductsData': AllSteamProductsData()}

#
#
# class SteamAppInfoCSVJoiner(Task):
#     """
#     Merges all raw CSV tables into one MasterData for Steam Apps.
#     """
#     task_namespace = 'SteamProductsInfo'
#     priority = 100
#     steam_apps_info_path = Parameter(significant=True, description='Path to join all GetSteamProductsDataInfo .csv')
#     date_path_part = DateParameter(default=date.today(), description='Date for root path')
#
#     directory_for_csv_join = 'Apps_info'
#     csv_file_for_result = "SteamAppsInfo.csv"
#
#     def requires(self):
#         return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}
#
#     def output(self):
#         return LocalTarget(
#             path.join(f"{self.steam_apps_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))
#
#     def run(self):
#         steam_products_info_run(self, self.steam_apps_info_path)
#
#
# class SteamDLCInfoCSVJoiner(Task):
#     """
#     Merges all raw CSV tables into one MasterData for Steam DLC.
#     """
#     task_namespace = 'SteamProductsInfo'
#     priority = 100
#     steam_dlc_info_path = Parameter(significant=True, description='Path to join all GetSteamProductsDataInfo .csv')
#     date_path_part = DateParameter(default=date.today(), description='Date for root path')
#
#     directory_for_csv_join = 'DLC_info'
#     csv_file_for_result = "SteamDLCInfo.csv"
#
#     def requires(self):
#         return {'GetSteamProductsDataInfo': GetSteamProductsDataInfo()}
#
#     def output(self):
#         return LocalTarget(
#             path.join(f"{self.steam_dlc_info_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"))
#
#     def run(self):
#         steam_products_info_run(self, self.steam_dlc_info_path)
#
#
# class CreateDiagramsSteamStatistics(Task):
#     """
#     Create diagrams for the report.
#     """
#     task_namespace = 'CreateDiagramsSteamStatistics'
#     priority = 200
#     create_diagrams_steam_statistics_path = \
#         Parameter(significant=True,
#                   description='Path to join all CreateAppsDiagramSteamStatistics .csv')
#     date_path_part = DateParameter(default=date.today(), description='Date for root path')
#     create_diagrams_steam_logfile_path = Parameter(default="create_diagrams_steam_statistics.log",
#                                                    description='Path for ".log" file')
#     create_diagrams_steam_loglevel = Parameter(default=30, description='Log Level')
#
#     def requires(self):
#         return {'SteamAppInfoCSVJoiner': SteamAppInfoCSVJoiner(),
#                 'SteamDLCInfoCSVJoiner': SteamDLCInfoCSVJoiner()}
#
#     def output(self):
#         return LocalTarget(
#             path.join(
#                 f"{self.create_diagrams_steam_logfile_path}/{self.date_path_part:%Y/%m/%d}/{'_Validate_Success'}"
#             ))
#
#     def run(self):
#         create_diagrams_steam_statistics_luigi_task_run(self)


if __name__ == "__main__":
    # luigi.build([task], local_scheduler=True)
    run()
