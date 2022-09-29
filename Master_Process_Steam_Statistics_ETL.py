import subprocess
from time import sleep
from random import randint


def unix_process_script_start():
    return subprocess.check_output(['./start_steam_statistics_ETL.sh'])


def process_run():
    try:
        subprocess.check_output(['./start_steam_statistics_ETL.sh'])
    except subprocess.CalledProcessError as error:
        sleep(randint(30, 60))
        subprocess.check_output(['./start_steam_statistics_ETL.sh'])
        print(error, '\n================================_New_try:_================================')


if __name__ == "__main__":
    process_run()
