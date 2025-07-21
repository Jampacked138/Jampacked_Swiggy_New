from modules import *
import os
import pandas as pd
import requests
import warnings
import io
import logging
import shutil
import json
import traceback
from datetime import datetime

current_date = datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(filename = f"logs/session_create_{current_date}.log", 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

def extend_cookie_expiry(cookie, days):
    if 'expiry' in cookie:
        expiry_date = datetime.fromtimestamp(cookie['expiry'])
        new_expiry_date = expiry_date + timedelta(days=days)
        cookie['expiry'] = int(new_expiry_date.timestamp())
    return cookie

def printLog(msg):
    """
    Prints log messages.

    Args:
        message (str): Message to be logged.
    """
    logging.info(msg)
    print(msg)

def printError(e,print_=False):
 
    error_type = type(e).__name__
    line_number = sys.exc_info()[-1].tb_lineno
    if e.args:
        error_name = e.args[0]
    else:
        error_name = "No additional information available"
    error_msg = f"Error Type: {error_type}\nError Name: {error_name}\nLine where error occurred: {line_number}"
 
    logging.error(error_msg)

    if print_:
        print(error_msg)

def create_session(driver,phone_,pass_):
    try:
        phone_placeholder = wait_visible(driver, '//input[@id="Enter Restaurant ID / Mobile number"]')
        phone_placeholder.send_keys(str(phone_))

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="submit-phone-number"]')

        time.sleep(3)
        wait_click(driver,'//*[text()="Login with Password"]')

        password_placeholder = wait_visible(driver,'//input[@type="password"]')
        password_placeholder.send_keys(pass_)

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="Login-Button"]')

        # checking again
        time.sleep(3)
        driver.refresh()

    except Exception as e:
        printError(e,True)

    return driver

def main():
    printLog("-------- Creating Sessions of Accounts --------")
    df = pd.read_excel('accounts.xlsx')

    for i in range(len(df)):
        
        account_ = df.iloc[i]

        phone_ = account_.Phone
        pass_ = account_.Password
        printLog(f"-----Account {phone_}")

        if os.path.exists(f'cookies/{phone_}.pkl'):
            printLog("Cookies exists. Checking validation")
            cookies = get_cookies(phone_)
            driver = init_driver(cookies,False)
            
            driver.refresh()
            driver.maximize_window()

            time.sleep(4)

            if driver.current_url == 'https://partner.swiggy.com/login/':
                printLog("--Cookies expired. Creating new session.")
                driver.delete_all_cookies()
                driver = create_session(driver,phone_,pass_)

                if driver.current_url == 'https://partner.swiggy.com/login/':
                    printLog("Still having issue in creating cookies.")
                    
                    screenshot_file_path = os.path.join("screenshots", f'cookies_{phone_}_{current_date}.png')
                    driver.save_screenshot(screenshot_file_path)

                    os.remove(f"cookies/{phone_}.pkl")
                else:
                    printLog("Cookies created successfully.")
                    cookies = driver.get_cookies()

                    extended_cookies = [extend_cookie_expiry(cookie, 30) for cookie in cookies]

                    driver.delete_all_cookies()
                    for cookie in extended_cookies:
                        driver.add_cookie(cookie)

                    driver.get('https://partner.swiggy.com/growth')
                    time.sleep(2)

                    create_cookies(driver,phone_)
            else:
                printLog("Extending expiry time.")
                cookies = driver.get_cookies()

                extended_cookies = [extend_cookie_expiry(cookie, 30) for cookie in cookies]

                driver.delete_all_cookies()
                for cookie in extended_cookies:
                    driver.add_cookie(cookie)
                
                driver.get('https://partner.swiggy.com/growth')
                time.sleep(2)
                create_cookies(driver,phone_)

        else:
            printLog(f"No cookies found for account {phone_}.\nCreating cookies")
            driver = init_driver(None,False)
            driver = create_session(driver,phone_,pass_)

            if driver.current_url == 'https://partner.swiggy.com/login/':
                    screenshot_file_path = os.path.join("screenshots", f'cookies_{phone_}_{current_date}.png')
                    driver.save_screenshot(screenshot_file_path)
                    
                    printLog("Still having issue in creating cookies.")
            else:
                cookies = driver.get_cookies()

                extended_cookies = [extend_cookie_expiry(cookie, 30) for cookie in cookies]

                driver.delete_all_cookies()
                for cookie in extended_cookies:
                    driver.add_cookie(cookie)

                driver.get('https://partner.swiggy.com/growth')
                
                create_cookies(driver,phone_)
        
        driver.quit()

if __name__ == "__main__":
    main()

