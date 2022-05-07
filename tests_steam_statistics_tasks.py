from unittest import *
from requests import get

server_satus = get('http://api.steampowered.com/ISteamWebAPIUtil/GetServerInfo/v0001/')
