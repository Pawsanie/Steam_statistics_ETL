"""
Contains code for luigi task 'AllSteamAppsData'.
'''
Содержит код для Луиджи такски 'AllSteamAppsData'.
"""


def steam_aps_from_web_api_parser(interested_data):
    """
    Parses the result received from the Steam Web-API.
    '''
    Парсит результат получаемый от Steam Web-API.
    """
    all_aps_data = interested_data
    all_aps_data = all_aps_data.get('applist')
    all_aps_data = all_aps_data.get('apps')
    return all_aps_data
