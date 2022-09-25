import subprocess


def process_run():
    try:
        subprocess.check_output(['./start_steam_statistics_ETL.sh'])
    except subprocess.CalledProcessError as error:
        subprocess.check_output(['./start_steam_statistics_ETL.sh'])
        print(error, '\n================================_New_try:_================================')


if __name__ == "__main__":
    process_run()
