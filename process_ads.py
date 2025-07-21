import subprocess
from datetime import timedelta, datetime
import sys

def main():
    DURATION_DAYS = 90
    SCRAPE_DAYS_PER_EXECUTION = 30

    DURATION_DAYS += SCRAPE_DAYS_PER_EXECUTION
    iter_ = SCRAPE_DAYS_PER_EXECUTION
    program = 'app_ads.py'
    python_executable = sys.executable

    try:
        for i in range(SCRAPE_DAYS_PER_EXECUTION, DURATION_DAYS , iter_):
            print("Starting new date")
            date = datetime.today() - timedelta(days=i)
            command = [python_executable, program, date.strftime('%Y-%m-%d'),'1']
            subprocess.run(command, check=True)
            print("Script executed successfully.")
        
    except subprocess.CalledProcessError as e:
        print("Error:", e)

    try:
        for i in range(SCRAPE_DAYS_PER_EXECUTION, DURATION_DAYS , iter_):
            print("Starting new date")
            date = datetime.today() - timedelta(days=i)
            command = [python_executable, program, date.strftime('%Y-%m-%d'),'2']
            subprocess.run(command, check=True)
            print("Script executed successfully.")
        
    except subprocess.CalledProcessError as e:
        print("Error:", e)
    
if __name__ == '__main__':
    main()