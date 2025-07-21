from modules import *
import os
import pandas as pd
import requests
import warnings
warnings.filterwarnings('ignore')
import logging
import shutil
import json
import traceback
from datetime import datetime
from app import process_operations_metrics, process_restaurant,process_restaurant_reviews

cur,connection = get_cursor()

### ************ MODULES ************ ###

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

def execute_query(query,values):
    try:
        cur.execute(query, values)
    except Exception as e:
        printError(e,True)
        connection.commit()


### ************ MAIN CODE HANDLING ALL STUFF ************ ###
def process_account(phone_,pass_):
    
    cookies = get_cookies(phone_)
    
    driver = init_driver(cookies,False)
    
    driver.refresh()
    driver.maximize_window()

    time.sleep(4)

    print("\n#------ Scraping account: {} ------#".format(phone_))

    if driver.current_url == 'https://partner.swiggy.com/login/':
        print("--Cookies expired. Creating new session.")
        
        phone_placeholer = wait_visible(driver,'//input[@aria-label="Enter Restaurant ID / Mobile number"]')
        phone_placeholer.send_keys(phone_)

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="submit-phone-number"]')

        time.sleep(3)
        wait_click(driver,'//*[text()="Login with Password"]')

        password_placeholder = wait_visible(driver,'//input[@type="password"]')
        password_placeholder.send_keys(pass_)

        time.sleep(1)
        wait_click(driver,'//div[@data-testid="Login-Button"]')

        create_cookies(driver,phone_)

    driver.get('https://partner.swiggy.com/orders')
    
    time.sleep(2)
    soup = BeautifulSoup(driver.page_source,features='lxml')

    main_divs = soup.find_all('div',class_='outlet-item-inner')

    restaurant_ids = [div.find('div',string=re.compile(r'\d+')).text for div in main_divs]

    clean_text = lambda x : re.sub(r'\s+',' ',x).strip()
    restaurants_names = [clean_text(div.find('div',class_='rest-details').text) for div in main_divs]

    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    if len(restaurant_ids) < 1:
        print("--Something went wrong. Please check code.")
        driver.quit()
        return
    
    for restaurant_id in restaurant_ids:
        
        log_msg = "\n<<****** Scraping data for restaurant: {} ******>>".format(restaurant_id)
        print(log_msg)
        logging.info(log_msg)
        
        # ### ************ Processing orders ************ ###
        # print("\n# ****** Scraping Restaurant Orders ****** #\n")
        # process_orders(driver,restaurant_id)

        # ### ************ Processing Daily metrics ************ ###
        # print("\n# ****** Scraping Restaurant Daily Metrics ****** #\n")
        #process_operations_metrics(driver,restaurant_id)

        # ### ************ Processing Restaurant Reviews ************ ###
        # print("\n# ****** Scraping Restaurant Reviews ****** #\n")
        
        #process_restaurant_reviews(driver,restaurant_id)

        # ### ************ Processing Restaurant Timings ************ ###
        # print("\n# ****** Scraping Restaurant Timings ****** #\n")
        # process_restaurant_timings(access_token,restaurant_id)

    
    driver.quit()

def clearFolders():
    if os.path.exists('downloads'):
        shutil.rmtree('downloads')
    os.makedirs('downloads', exist_ok=True)

    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data', exist_ok=True)

def main():
    df = pd.read_excel("accounts.xlsx")

    for i in range(len(df)):
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password

        clearFolders()
        
        process_account(phone_,pass_)
    
    print("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()
