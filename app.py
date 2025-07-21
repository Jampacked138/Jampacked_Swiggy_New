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
from selenium.common.exceptions import ElementNotInteractableException, TimeoutException, NoSuchElementException

warnings.filterwarnings('ignore')
current_date = datetime.now().strftime("%Y-%m-%d")

logging.basicConfig(filename = f"logs/swiggy_logs_{current_date}.log", 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                    )

cur,connection = get_cursor()
ADS_DATA_NOT_AVAILABLE = False

### ************ MODULES ************ ###

def printLog(msg,statement=None,end=None):
    """
    Prints log messages.

    Args:
        message (str): Message to be logged.
    """
    logging.info(msg)

    if end is not None:
        print(msg, end = end)
        return
    if statement is None:
        print(msg)
    else:
        print(msg,statement)

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
    """
    Executes a given SQL query with provided values.

    Args:
        query (str): SQL query to be executed.
        values (tuple): Values to be inserted into the query.
    """
    try:
        cur.execute(query, values)
    except Exception as e:
        printError(e,True)
        connection.commit()

### ************ ADS PERFORMANCE ************ ###
def refresh(driver,frame=0):
    driver.switch_to.default_content()

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
    driver.switch_to.frame(iframe)
    
    if frame == 1:
        frames = driver.find_elements(By.TAG_NAME,'iframe')
        driver.switch_to.frame(frames[0])
        
    elif frame == 2:
        frames = driver.find_elements(By.TAG_NAME,'iframe')
        driver.switch_to.frame(frames[1])
    
    return
    
def select_date(driver,date_to_select):

    def func():
        nonlocal elements
        nonlocal driver

        for input_element in elements:
            try:
                input_element.click()
                time.sleep(1.5)
                element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')

                while True:    
                    if element is None:
                        input_element.click()
                        time.sleep(1.5)
                        element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')
                        
                    selected_date_text = element.text
                    selected_date = datetime.strptime(selected_date_text, '%B %Y')
                    selected_month = selected_date.month
                    selected_year = selected_date.year

                    if provided_month == selected_month and provided_year == selected_year:
                        xpath = f"//*[contains(@class, 'DayPicker-Day') and text()='{day}']"
                        wait_click(driver,xpath)
                        time.sleep(1.5)
                        break

                    if provided_year < selected_year or (provided_year == selected_year and provided_month < selected_month):
                        wait_visible(driver, '//span[@aria-label="Previous Month"]').click()
                    else:
                        wait_visible(driver, '//span[@aria-label="Next Month"]').click()
                
                time.sleep(2)

            except Exception as e:
                return False
        
        return True

    driver.switch_to.default_content()

    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    time.sleep(3)

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]',15)
    driver.switch_to.frame(iframe)
    
    provided_date = datetime.strptime(date_to_select, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    input_element = wait_visible(driver,"//input[@placeholder='To']")

    from_input_element = wait_visible(driver,"//input[@placeholder='From']")

    elements = [input_element,from_input_element]

    check = func()

    start = input_element.get_attribute('value')
    end = from_input_element.get_attribute('value')

    if check:
        if start == end:
            printLog(f"Date selected: {date_to_select},", end= " ")
            return True
        else:
            printLog(f"Date selection error: {date_to_select}")
            return False
    else:
        printLog(f"Date selection error: {date_to_select}")
        return False
  
def get_data(driver,date_):

    def wait(frame=0):
        global ADS_DATA_NOT_AVAILABLE
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.XPATH,'//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            error_in_frame = "Couldn't load the data for this visual"
            if error_in_frame in soup.text:
                printLog(error_in_frame)
                ADS_DATA_NOT_AVAILABLE = True
                return False
            cells = soup.find_all('div', class_='pivotTableCellWrap')

            if len(cells) > 0:
                break
            elif time_lapsed >= 20:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
                    
    driver.switch_to.default_content()

    iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
    driver.switch_to.frame(iframe)

    try:
        wait_click(driver,'//*[text()="New Users"]',5)
    except:
        return False
    data_available = wait(0)

    if not data_available:
        printLog("Data not fetched from 1st pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    data = {}

    for i in range(0, len(cells), 7):  # Assuming each record has 7 cells
        try:
            res_id = re.search(r'\d+',cells[i].text).group()
            record = {
                'ad_date' : date_,
                'restaurant_id': res_id,
                'created_at' : get_current_date_formated(),
                'updated_at' : get_current_date_formated(),
                'ad_spend': cells[i+2].text.strip(),
                'impressions': cells[i+3].text.strip(),
                'menu_visits': cells[i+4].text.strip(),
                'orders': cells[i+5].text.strip(),
                'sales': cells[i+6].text.strip(),
            }
        except:
            continue
        data[res_id] = record

    if len(data) == 0:
        printLog("No data available.")
        driver.switch_to.default_content()
        try:
            wait_click(driver,'/html/body/div[2]/div/div/button')
        except:
            pass
        return True

    data_available = wait(1)

    if not data_available:
        printLog("Data not fetched from 2nd pane.")
        return False
    
    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
    length = len(headers)
    
    try:
        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'nu_orders':dct.get('NU Orders','0.0'),
                    }
            
            data[res_id].update(record)
        
        for res,record in data.items():
            if 'nu_orders' in data[res].keys():
                pass
            else:
                data[res]['nu_orders'] = '0.0'
            
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['nu_orders'] = '0.0'
    

    driver.switch_to.default_content()
    driver.switch_to.frame(iframe)

    wait_click(driver,'//*[text()="Slot Types"]')

    data_available = wait(1)

    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')
    scraped_res = []
    try:
        length = int(cells[-1]['aria-colindex'])
        headers = [cells[i].text for i in range(length)]

        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')

            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'breakfast_orders':dct.get('Breakfast','0.0'),
                    'dinner_orders':dct.get('Dinner','0.0'),
                    'ln1_orders':   dct.get('LN 1 (12-5 am)','0.0'),
                    'ln2_orders':   dct.get('LN 2 (11-12 pm)','0.0'),
                    'lunch_orders': dct.get('Lunch','0.0'),
                    'snacks_orders':dct.get('Snacks','0.0'),
                    }
            
            data[res_id].update(record)
            scraped_res.append(res_id)

        for res in data.keys():
            if res not in scraped_res:
                data[res]['breakfast_orders'] = '0.0'
                data[res]['dinner_orders'] = '0.0'
                data[res]['ln1_orders'] = '0.0'
                data[res]['ln2_orders'] = '0.0'
                data[res]['lunch_orders'] = '0.0'
                data[res]['snacks_orders'] = '0.0'
            
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['breakfast_orders'] = '0.0'
            data[res]['dinner_orders'] = '0.0'
            data[res]['ln1_orders'] = '0.0'
            data[res]['ln2_orders'] = '0.0'
            data[res]['lunch_orders'] = '0.0'
            data[res]['snacks_orders'] = '0.0'
            
    for res,record in data.items():
        for key, value in record.items():
            if key not in ['restaurant_id', 'location', 'res_id']:
                data[res][key] = value.replace('₹','').replace(',', '').replace('%', '')
                
            if value == '':
                data[res][key] = '0.0'
      
    for res,record in data.items():

        query = """ 
        INSERT INTO swiggy_ad_metrics (ad_date, restaurant_id, created_at, updated_at, ad_spend, impressions, menu_visits, orders, sales, nu_orders, breakfast_orders,dinner_orders, ln1_orders, ln2_orders, lunch_orders, snacks_orders)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s);
        """
        
        execute_query(query,tuple(record.values()))
    
    connection.commit()
    printLog("--data scraped.")

    driver.switch_to.default_content()
    driver.refresh()

    return True
   
def process_ad_performance(driver,restaurant_ids):
    """
    Scrapes ad performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape ad performance for.
    """
    global ADS_DATA_NOT_AVAILABLE
    ADS_DATA_NOT_AVAILABLE = False

    driver.get(f'https://partner.swiggy.com/business-metrics/ads-performance/restaurant/{restaurant_ids[0]}')
    
    time.sleep(3)
    
    try:
        wait_click(driver,'/html/body/div[2]/div/div/button')
    except:
        pass
    
    query = """
        SELECT DISTINCT date
        FROM swiggy_ads_month_track
        WHERE restaurant_id IN %(restaurant_list)s;
        """
    
    cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})

    record = cur.fetchone()

    if record is None:
        printLog("--Record not available. Run Process Ads!!!")
        
        for restaurant_id in restaurant_ids:
            query = """
                INSERT INTO swiggy_ads_month_track(
                restaurant_id,
                date
                ) 
                VALUES(
                    %s, %s
                    );
                """
            values = (restaurant_id,datetime.today().strftime('%Y-%m-%d'))
            execute_query(query,values)
        connection.commit()

        for i in range(31,1,-1):
            
            delta = datetime.today() - timedelta(days=i)
            date = delta.strftime('%Y-%m-%d')

            check = select_date(driver,date)
            if not check:
                driver.switch_to.default_content()
                continue

            try:
                refresh(driver)
                wait_click(driver,'//*[text()="Outlet level Performance"]',2)
            except Exception as e:
                printLog("Path is changed or Webpage structure is changed. Please check the code.")
                printError(e)
                pass
            
            get_data(driver,date)
            
            for restaurant_id in restaurant_ids:
                query = """
                        UPDATE swiggy_ads_month_track
                        SET 
                            date = %s
                        WHERE 
                            restaurant_id = %s
                        """
                values = (date, restaurant_id)
                execute_query(query,values)
            connection.commit()

            time.sleep(1)
    
    else:
        printLog("--Record available. Downloading new data...")
        previous_date = record[0]
        current_date = datetime.today().date()
        delta_days = (current_date - previous_date).days

        if delta_days > 30:
            printLog("--days to scraped are more than 30. Please run process ads file.")
            return
        
        ids_str = ','.join(map(str, restaurant_ids))
        cur.execute(f"select distinct ad_date from swiggy_ad_metrics where restaurant_id in ({ids_str})")
        records = cur.fetchall()
        scraped_dates = [str(record[0]) for record in records]

        for i in range(delta_days, 0, -1):
            delta = current_date - timedelta(days=i)
            date = delta.strftime('%Y-%m-%d')

            if date in scraped_dates:
                printLog("Date is already scraped. Skipping date.")
                continue
            
            check = select_date(driver,date)
            if not check:
                printLog("Date selection error. !!!SKIPPING DATE!!!")
                with open('ad_dates_with_error.txt','+a') as file:
                    file.write(date)
                    file.write('\n')
                continue

            try:
                refresh(driver)
                wait_click(driver, '//*[text()="Outlet level Performance"]', 2)
            except Exception as e:
                printLog("Path is changed or Webpage structure is changed. Please check the code.")
                printError(e)
                continue

            check = get_data(driver,date)
            if check:
                pass
            elif ADS_DATA_NOT_AVAILABLE:
                printLog("Unable to fetch data. Not available on Swiggy.")

                for restaurant_id in restaurant_ids:
                    query = """
                            UPDATE swiggy_ads_month_track
                            SET 
                                date = %s
                            WHERE 
                                restaurant_id = %s
                            """
                    values = (date, restaurant_id)
                    execute_query(query,values)
                connection.commit()
                return
            
            else:
                driver.refresh()
                select_date(driver,date)
                get_data(driver,date)

            for restaurant_id in restaurant_ids:
                query = """
                    UPDATE swiggy_ads_month_track
                    SET 
                        date = %s
                    WHERE 
                        restaurant_id = %s
                    """
                values = (date, restaurant_id)
                execute_query(query, values)
            connection.commit()

            time.sleep(1)
        
        return

### ************ DISCOUNT PERFORMANCE ************ ###
def get_rev_single_res(driver,restaurant_id,date):
    
    def wait(frame=0):
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.TAG_NAME,'iframe')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            card = soup.find('div', class_='card')

            if card is not None:
                break
            elif time_lapsed >= 40:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
      
    data = {'business_date': date,
            'created_at' : get_current_date_formated(),
            'updated_at' : get_current_date_formated(),
            'restaurant_id': restaurant_id,}

    data_available = wait(0)
    if not data_available:
        return
    
    soup = BeautifulSoup(driver.page_source)

    card = soup.find('div', class_='card')

    if card:
        elements = card.find_all('div', class_='cardItemContainer')
        for element in elements:
            title = element.find('div', class_='details').get_text(strip=True)
            value = element.find('div', class_='caption').get_text(strip=True)
            if 'Blank' in value:
                value = 0
            if title == "Orders":
                data['orders'] = value
            elif title == "Revenue":
                data['revenue'] = value
            elif title == "Spends":
                data['spends'] = value
            elif title == "AOV":
                data['aov'] = value

    refresh(driver,frame=2)
    
    soup = BeautifulSoup(driver.page_source)

    card = soup.find('div', class_='card')

    if card:
        elements = card.find_all('div', class_='cardItemContainer')
        for element in elements:
            title = element.find('div', class_='details').get_text(strip=True)
            value = element.find('div', class_='caption').get_text(strip=True)
            if 'Blank' in value:
                value = 0
            if title == "New User Orders":
                data['new_user_orders'] = value
            elif title == "Repeat User Orders":
                data['repeat_user_orders'] = value
            elif title == "Dormant User Orders":
                data['dormant_user_orders'] = value
    else:
        data['new_user_orders'] = 0
        data['repeat_user_orders'] = 0
        data['dormant_user_orders'] = 0
    
    for key, value in data.items():
        if key not in ['restaurant_id','business_date','created_at','updated_at']:
            try:
                data[key] = value.replace('₹','').replace(',', '').replace('%', '')
            except:
                continue

    if len(data.keys()) > 5:
        
        query = """ 
        INSERT INTO swiggy_discount_metrics (business_date, created_at, updated_at, restaurant_id, orders, revenue,
        spends, aov, new_user_orders, repeat_user_orders, dormant_user_orders)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        execute_query(query,tuple(data.values()))

        connection.commit()

        printLog("--data scraped.")
        driver.switch_to.default_content()

        return True
    
    else:
        printLog("--No data available")
        return True

def are_dates_same(date_str1, date_str2):
    # Define possible date formats
    date_formats = [
        "%Y-%m-%d",     # Example: 2024-06-10
        "%d-%m-%Y",     # Example: 10-06-2024
        "%d/%m/%Y",     # Example: 06/10/2024
        "%Y/%m/%d",     # Example: 2024/06/10
        "%B %d, %Y",    # Example: June 10, 2024
        "%d %B %Y",     # Example: 10 June 2024
        "%b %d, %Y",    # Example: Jun 10, 2024
        "%d %b %Y",     # Example: 10 Jun 2024
        "%Y-%m-%dT%H:%M:%S", # Example: 2024-06-10T15:30:00
        "%Y-%m-%d %H:%M:%S"  # Example: 2024-06-10 15:30:00
        # Add more formats as needed
    ]
    
    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                continue
        raise ValueError(f"Date format for '{date_str}' is not recognized")
    
    # Parse the dates
    date1 = parse_date(date_str1)
    date2 = parse_date(date_str2)
    
    # Compare the dates
    return date1.date() == date2.date()

def select_date(driver, date_to_select):
    
    def func():
        nonlocal elements
        nonlocal driver

        for input_element in elements:
            try:
                input_element.click()
                time.sleep(1.5)
                element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')

                while True:    
                    if element is None:
                        input_element.click()
                        time.sleep(1.5)
                        element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')
                        
                    selected_date_text = element.text
                    selected_date = datetime.strptime(selected_date_text, '%B %Y')
                    selected_month = selected_date.month
                    selected_year = selected_date.year

                    if provided_month == selected_month and provided_year == selected_year:
                        xpath = f"//*[contains(@class, 'DayPicker-Day') and text()='{day}']"
                        wait_click(driver, xpath)
                        time.sleep(1.5)
                        break

                    if provided_year < selected_year or (provided_year == selected_year and provided_month < selected_month):
                        wait_visible(driver, '//span[@aria-label="Previous Month"]').click()
                    else:
                        wait_visible(driver, '//span[@aria-label="Next Month"]').click()
                
                time.sleep(2)

            except Exception as e:
                return False
        
        return True

    driver.switch_to.default_content()

    try:
        wait_click(driver, '/html/body/div[2]/div/div/button')
    except:
        pass
    
    time.sleep(3)

    iframe = wait_visible(driver, '//iframe[@id="metrics-powerbi-frame"]', 15)
    driver.switch_to.frame(iframe)
    
    provided_date = datetime.strptime(date_to_select, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    input_element = wait_visible(driver, "//input[@placeholder='To']")
    from_input_element = wait_visible(driver, "//input[@placeholder='From']")
    
    elements = [input_element, from_input_element]

    retry_count = 0
    while retry_count < 2:
        check = func()

        start = input_element.get_attribute('value')
        end = from_input_element.get_attribute('value')

        if check:
            if are_dates_same(start, date_to_select) and are_dates_same(end, date_to_select):
                printLog(f"Date selected correctly: {date_to_select}", end=" ")
                return True
            else:
                printLog(f"Date selection mismatch: Expected {date_to_select}, Got From: {start}, To: {end}")
                retry_count += 1
                printLog("Retrying...")
                time.sleep(2)  # Optionally wait before retrying
        else:
            printLog(f"Date selection error: {date_to_select}")
            return False

    printLog(f"Date selection failed after retrying: {date_to_select}")
    return False
 
def get_data_rev(driver,date):
        
    def wait(frame=0):
        time_lapsed = 0

        while True:
            driver.switch_to.default_content()

            iframe = wait_visible(driver,'//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)

            frames = driver.find_elements(By.XPATH,'//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])

            soup = BeautifulSoup(driver.page_source)
            cells = soup.find_all('div', class_='pivotTableCellWrap')

            if len(cells) > 0:
                break
            elif time_lapsed >= 40:
                return False
                
            time_lapsed += 1
            time.sleep(1)
        
        return True
      
    data = {}
 
    data_available = wait(0)
    if not data_available:
        printLog("Data not fetched from 1st pane.")
        return False
    
    refresh(driver,frame=1)
    
    soup = BeautifulSoup(driver.page_source)
    
    cells = soup.find_all('div', class_='pivotTableCellWrap')

    try:
        headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
        length = len(headers)

        data = {}
        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'business_date' : date,
                    'created_at' : get_current_date_formated(),
                    'updated_at' : get_current_date_formated(),
                    'restaurant_id': res_id,
                    'orders': dct.get('Orders','0'),
                    'revenue': dct.get('Revenue','0.0'),
                    'spends': dct.get('Spends','0.0'),
                    'aov' : dct.get('AOV','0.0')
                    }
            
            data[res_id] = record

    except Exception as e:
        printError(e)
        return False
    
    if len(data) == 0:
        printLog("No data available.")
        driver.switch_to.default_content()
        try:
            wait_click(driver,'/html/body/div[2]/div/div/button')
        except:
            pass
        return True
    
    data_available = wait(1)

    refresh(driver,frame=2)

    soup = BeautifulSoup(driver.page_source)

    cells = soup.find_all('div', class_='pivotTableCellWrap')

    if len(cells) == 0:
        refresh(driver,frame=2)

    try:
        headers = [cell.text.replace('\xa0','') for cell in soup.find_all('div',role='columnheader')[1:]]
        length = len(headers)

        for i in range(length,len(cells),length):
            dct = {}
            for header in headers:
                index = headers.index(header)
                dct[header] = cells[i+index].text.replace('\xa0','')
            res_id = re.search(r'\d+',cells[i].text).group()

            record = {
                    'new_user_orders': dct.get('New user orders','0'),
                    'repeat_user_orders': dct.get('Repeat user orders','0'),
                    'dormant_user_orders': dct.get('Dormant  user orders','0'),
                    }
            
            data[res_id].update(record)
        
    except Exception as e:
        printError(e)
        for res,record in data.items():
            data[res]['new_user_orders'] = '0'
            data[res]['repeat_user_orders'] = '0'
            data[res]['dormant_user_orders'] = '0'

    for res,record in data.items():
        for key, value in record.items():
            if key not in ['restaurant_id','res_id']:
                data[res][key] = value.replace('₹','').replace(',', '').replace('%', '')
                
            if value == '':
                data[res][key] = '0.0'

    if len(data.keys()) > 0:
        for res,record in data.items():
            query = """ 
            INSERT INTO swiggy_discount_metrics (business_date, created_at, updated_at, restaurant_id, orders, revenue,
            spends, aov, new_user_orders, repeat_user_orders, dormant_user_orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            execute_query(query,tuple(record.values()))

        connection.commit()

        printLog("--data scraped.")
        driver.switch_to.default_content()

        return True
    
    else:
        printLog("--No data available")
        return True

def handle_container_popups(driver):
    """
    Handles potential popups in the #mCSB_1_container before proceeding.
    """
    try:
        # Wait for the container to load
        container_xpath = '//*[@id="mCSB_1_container"]'
        if not container_xpath:
            print (f"no container xpath")
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, container_xpath))
        )

        if not container:
            print (f"no container")        

        # Look for popup close buttons inside the container
        popup_close_button_xpath = '//a[contains(@class, "dismiss") or contains(@class, "icon-close")]'  # Adjust as needed
        popups = driver.find_elements(By.XPATH, popup_close_button_xpath)
        
        if popups:
            for popup in popups:
                try:
                    wait_click(driver,popup_close_button_xpath)
                    time.sleep(1)  # Allow time for the popup to dismiss
                    print("Popup closed successfully.")
                except Exception as e:
                    print(f"Could not close popup: {e}")
        else:
            print("No popups found in the container.")
    except Exception as e:
        print(f"Error handling popups: {e}")

def process_discount_performance(driver,restaurant_ids,restaurants_names,phone_):
    """
    Scrapes discount performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
        phone_ (str): Phone number associated with the account.
    """
    
    def retry():
        url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
        driver.get(url)
        
        refresh(driver)

    cities_ = list({res.split(',')[-1].strip() for res in restaurants_names})

    url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
    

    query = """
        SELECT DISTINCT date
        FROM swiggy_discount_month_track
        WHERE restaurant_id IN %(restaurant_list)s;
        """
    cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})

    record = cur.fetchone()                   

    if record is None:
        printLog("--Record not available. Processing 90 days data.")

        for i in range(len(cities_)):
            printLog(f"--Scraping for city: {cities_[i]}")
            driver.get(url)
            handle_container_popups(driver)
            popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
            try:
            # Check if the popup close button exists and is interactable
                close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
                if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                    wait_click(driver, popup_close_xpath)  # Click if interactable
                else:
                    print("Popup exists but is not interactable or does not need closing")
            except:
                print(f"error handling popup 1")

            refresh(driver)

            compare_button_xpath = '//button[text()="Compare Performance Of Outlets"]'
            compare_button = wait_visible(driver,compare_button_xpath,10)
            if compare_button is None:
                retry()
            
            try:
                wait_click(driver,compare_button_xpath)
                city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div')
                if city_pane is None:
                    city_pane = driver.find_element(By.CLASS_NAME, 'Stack__StackBody-b66d5h-0 dQWwyk SelectFilterList__StyledStack-sc-105m70e-1 jFgQhN')
                cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                
                # Locate and use the search bar to search for the city
                search_bar = city_pane.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')  # Adjust placeholder if needed
                search_bar.clear()  # Clear any pre-filled text if necessary
                search_bar.send_keys(cities_[i])  # Type the city name
                
                # Wait for the search results to load and select the first city
                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')  # Ensure results are visible
                cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                
                for city in cities:
                    if city.text == cities_[i]:
                        city.click()
                        break

                # Now try clicking on the Continue button
                try:
                    wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')
                    printLog("Continue button clicked")
                except Exception as e:
                    printLog("Continue button click failed, retrying city click...")
                    
                    wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')  # Ensure results are visible
                    cities = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                    
                    for city in cities:
                        if city.text == cities_[i]:
                            city.click()
                            break
                    
                    # Try clicking the Continue button again
                    try:
                        wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')
                        printLog("Continue button clicked after retry")
                    except Exception as e:
                        printLog("Continue button click failed even after retry")
                
                # Wait for the outlet list to be visible
                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                # Click all visible outlets
                for outlet in outlets:
                    try:
                        outlet.click()
                    except Exception as e:
                        printLog(f"Error clicking outlet: {type(e).__name__} - {e}")

                # Try clicking on the confirm button
                confirm_button_xpath = '//div[contains(@class, "SelectFilterList__StyledFooter-sc-105m70e-9")]//button[text()="Confirm" and not(@disabled)]'
                try:
                    wait_click(driver, confirm_button_xpath)
                    printLog("Confirm button clicked")
                except Exception as e:
                    printLog("Confirm button click failed, retrying outlet click...")

                    # Refresh outlets in case of stale references
                    wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                    outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                    # Retry clicking the first visible outlet
                    try:
                        if outlets:
                            outlets[0].click()
                            printLog("Clicked first visible outlet")
                        else:
                            printLog("No outlets found on retry")
                    except Exception as e:
                        printLog(f"Retry outlet click failed: {type(e).__name__} - {e}")

                    # Retry confirm button
                    try:
                        wait_click(driver, confirm_button_xpath)
                        printLog("Confirm button clicked after retry")
                    except Exception as e:
                        printLog("Confirm button click failed even after retry")

                single_account = False
                
            except Exception as e:
                if len(restaurant_ids) > 1:
                    printLog(f"Unable to scrape for city: {cities_[i]}")
                    continue
                elif len(restaurant_ids) == 1:
                    printLog("ALERT!!! Account is having single restaurant. Scraping data.")
                    single_account = True

            driver.switch_to.default_content()
    
            for i in range(91,1,-1):
                date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
                check = select_date(driver,date)
                if not check:
                    printLog("Date selection failed, moving on to the next date")
                    driver.switch_to.default_content()
                    continue
                
                if single_account:
                    check = get_rev_single_res(driver,restaurant_ids[0],date)

                    if not check:
                        get_rev_single_res(driver,restaurant_ids[0],date)

                else:
                    check = get_data_rev(driver,date)

                    if not check:
                        get_data_rev(driver,date)
                    
                driver.switch_to.default_content()

                for restaurant_id in restaurant_ids:
                    query = """
                        INSERT INTO swiggy_discount_month_track (restaurant_id, date)
                        VALUES (%s, %s)
                        ON CONFLICT (restaurant_id)
                        DO UPDATE SET date = EXCLUDED.date
                        """
                    values = (restaurant_id,date)
                    execute_query(query, values)
                    connection.commit()
   
            refresh(driver)
     
    else:
        printLog("--Record available. Downloading new data...")
        ids_str = ','.join(map(str, restaurant_ids))
        cur.execute(f"select distinct business_date from swiggy_discount_metrics where restaurant_id in ({ids_str}) and campaign = 'Overall' order by business_date desc")
        records = cur.fetchall()
        scraped_dates = [str(record[0]) for record in records]
        previous_date = record[0]
        printLog(f"previous_date:({previous_date})")
        current_date = datetime.today().date()
        delta_days = (current_date - previous_date).days

        for i in range(len(cities_)):

            driver.get(url)
            handle_container_popups(driver)
            popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
            try:
            # Check if the popup close button exists and is interactable
                close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
                if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                    wait_click(driver, popup_close_xpath)  # Click if interactable
                else:
                    print("Popup exists but is not interactable or does not need closing")
            except:
                print(f"error handling popup 1")


            refresh(driver)

            printLog(f"--Scraping for city: {cities_[i]}")

            compare_button_xpath = '//button[text()="Compare Performance Of Outlets"]'
            compare_button = wait_visible(driver,compare_button_xpath,10)
            if compare_button is None:
                retry()
            
            try:
                wait_click(driver,compare_button_xpath)
                city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div')
                if city_pane is None:
                    city_pane = driver.find_element(By.CLASS_NAME, 'Stack__StackBody-b66d5h-0 dQWwyk SelectFilterList__StyledStack-sc-105m70e-1 jFgQhN')
                cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                
                # Locate and use the search bar to search for the city
                search_bar = city_pane.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')  # Adjust placeholder if needed
                search_bar.clear()  # Clear any pre-filled text if necessary
                search_bar.send_keys(cities_[i])  # Type the city name
                
                # Wait for the search results to load and select the first city
                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')  # Ensure results are visible
                cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                
                for city in cities:
                    if city.text == cities_[i]:
                        city.click()
                        break

                # Now try clicking on the Continue button
                try:
                    wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')
                    printLog("Continue button clicked")
                except Exception as e:
                    printLog("Continue button click failed, retrying city click...")
                    
                    wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')  # Ensure results are visible
                    cities = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                    
                    for city in cities:
                        if city.text == cities_[i]:
                            city.click()
                            break
                    
                    # Try clicking the Continue button again
                    try:
                        wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')
                        printLog("Continue button clicked after retry")
                    except Exception as e:
                        printLog("Continue button click failed even after retry")
                
               # Wait for the outlet list to be visible
                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                # Click all visible outlets
                for outlet in outlets:
                    try:
                        outlet.click()
                    except Exception as e:
                        printLog(f"Error clicking outlet: {type(e).__name__} - {e}")

                # Try clicking on the confirm button
                confirm_button_xpath = '//div[contains(@class, "SelectFilterList__StyledFooter-sc-105m70e-9")]//button[text()="Confirm" and not(@disabled)]'
                try:
                    wait_click(driver, confirm_button_xpath)
                    printLog("Confirm button clicked")
                except Exception as e:
                    printLog("Confirm button click failed, retrying outlet click...")

                    # Refresh outlets in case of stale references
                    wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                    outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                    # Retry clicking the first visible outlet
                    try:
                        if outlets:
                            outlets[0].click()
                            printLog("Clicked first visible outlet")
                        else:
                            printLog("No outlets found on retry")
                    except Exception as e:
                        printLog(f"Retry outlet click failed: {type(e).__name__} - {e}")

                    # Retry confirm button
                    try:
                        wait_click(driver, confirm_button_xpath)
                        printLog("Confirm button clicked after retry")
                    except Exception as e:
                        printLog("Confirm button click failed even after retry")


                single_account = False

            except Exception as e:

                if len(restaurant_ids) > 1:
                    printLog(f"Unable to scrape for city: {cities_[i]}")
                    continue
                elif len(restaurant_ids) == 1:
                    printLog("ALERT!!! Account is having single restaurant. Scraping data.")
                    single_account = True

                    
            driver.switch_to.default_content()

            for i in range(delta_days, 0, -1):
                delta = current_date - timedelta(days=i)
                date = delta.strftime('%Y-%m-%d')

                if date in scraped_dates:
                    printLog("Date is already scraped. Skipping date.")
                    continue

                check = select_date(driver,date)
                
                if not check:
                    printLog("Date selection failed, moving on to the next date")
                    driver.switch_to.default_content()
                    continue
                    
                if single_account:
                    check = get_rev_single_res(driver,restaurant_ids[0],date)

                    if not check:
                        get_rev_single_res(driver,restaurant_ids[0],date)

                else:
                    check = get_data_rev(driver,date)

                    if not check:
                        get_data_rev(driver,date)
                    
                driver.switch_to.default_content()

                for restaurant_id in restaurant_ids:
                    query = """
                        UPDATE swiggy_discount_month_track
                        SET 
                            date = %s
                        WHERE 
                            restaurant_id = %s
                        """
                    values = (date, restaurant_id)
                    execute_query(query, values)
                    connection.commit()

            refresh(driver)

            if not single_account:
                try:
                    wait_click(driver,'//*[text()="Go To Discount Dashboard Home"]')
                except:
                    driver.refresh()

### ************ DISCOUNT CAMPAIGN PERFORMANCE ************ ###

def are_dates_same(date_str1, date_str2):
    # Define possible date formats
    date_formats = [
        "%Y-%m-%d",     # Example: 2024-06-10
        "%d-%m-%Y",     # Example: 10-06-2024
        "%d/%m/%Y",     # Example: 06/10/2024
        "%Y/%m/%d",     # Example: 2024/06/10
        "%B %d, %Y",    # Example: June 10, 2024
        "%d %B %Y",     # Example: 10 June 2024
        "%b %d, %Y",    # Example: Jun 10, 2024
        "%d %b %Y",     # Example: 10 Jun 2024
        "%Y-%m-%dT%H:%M:%S", # Example: 2024-06-10T15:30:00
        "%Y-%m-%d %H:%M:%S"  # Example: 2024-06-10 15:30:00
        # Add more formats as needed
    ]
    
    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                continue
        raise ValueError(f"Date format for '{date_str}' is not recognized")
    
    # Parse the dates
    date1 = parse_date(date_str1)
    date2 = parse_date(date_str2)
    
    # Compare the dates
    return date1.date() == date2.date()

def select_date(driver, date_to_select):
    
    def func():
        nonlocal elements
        nonlocal driver

        for input_element in elements:
            try:
                input_element.click()
                time.sleep(1.5)
                element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')

                while True:    
                    if element is None:
                        input_element.click()
                        time.sleep(1.5)
                        element = wait_visible(driver, '//*[@class="DayPicker-Caption"]/div')
                        
                    selected_date_text = element.text
                    selected_date = datetime.strptime(selected_date_text, '%B %Y')
                    selected_month = selected_date.month
                    selected_year = selected_date.year

                    if provided_month == selected_month and provided_year == selected_year:
                        xpath = f"//*[contains(@class, 'DayPicker-Day') and text()='{day}']"
                        wait_click(driver, xpath)
                        time.sleep(1.5)
                        break

                    if provided_year < selected_year or (provided_year == selected_year and provided_month < selected_month):
                        wait_visible(driver, '//span[@aria-label="Previous Month"]').click()
                    else:
                        wait_visible(driver, '//span[@aria-label="Next Month"]').click()
                
                time.sleep(2)

            except Exception as e:
                return False
        
        return True

    driver.switch_to.default_content()

    try:
        wait_click(driver, '/html/body/div[2]/div/div/button')
    except:
        pass
    
    time.sleep(3)

    iframe = wait_visible(driver, '//iframe[@id="metrics-powerbi-frame"]', 15)
    driver.switch_to.frame(iframe)
    
    provided_date = datetime.strptime(date_to_select, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    input_element = wait_visible(driver, "//input[@placeholder='To']")
    from_input_element = wait_visible(driver, "//input[@placeholder='From']")
    
    elements = [input_element, from_input_element]

    retry_count = 0
    while retry_count < 2:
        check = func()

        start = input_element.get_attribute('value')
        end = from_input_element.get_attribute('value')

        if check:
            if are_dates_same(start, date_to_select) and are_dates_same(end, date_to_select):
                printLog(f"Date selected correctly: {date_to_select}", end=" ")
                return True
            else:
                printLog(f"Date selection mismatch: Expected {date_to_select}, Got From: {start}, To: {end}")
                retry_count += 1
                printLog("Retrying...")
                time.sleep(2)  # Optionally wait before retrying
        else:
            printLog(f"Date selection error: {date_to_select}")
            return False

    printLog(f"Date selection failed after retrying: {date_to_select}")
    return False

def handle_container_popups(driver):
    """
    Handles potential popups in the #mCSB_1_container before proceeding.
    """
    try:
        # Wait for the container to load
        container_xpath = '//*[@id="mCSB_1_container"]'
        if not container_xpath:
            print (f"no container xpath")
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, container_xpath))
        )

        if not container:
            print (f"no container")        

        # Look for popup close buttons inside the container
        popup_close_button_xpath = '//a[contains(@class, "dismiss") or contains(@class, "icon-close")]'  # Adjust as needed
        popups = driver.find_elements(By.XPATH, popup_close_button_xpath)
        
        if popups:
            for popup in popups:
                try:
                    wait_click(driver,popup_close_button_xpath)
                    time.sleep(1)  # Allow time for the popup to dismiss
                    print("Popup closed successfully.")
                except Exception as e:
                    print(f"Could not close popup: {e}")
        else:
            print("No popups found in the container.")
    except Exception as e:
        print(f"Error handling popups: {e}")

def scroll_down(driver):
    """Scroll down using the scrollbar."""
    try:
        scrollbar = driver.find_element(By.XPATH, '//*[@id="pvExplorationHost"]/div/div/exploration/div/explore-canvas/div/div[2]/div/div[2]/div[2]/visual-container-repeat/visual-container/transform/div/div[3]/div/div/visual-modern/div/div/div[2]/div[4]/div')
        driver.execute_script("arguments[0].scrollTop += 500;", scrollbar)
        time.sleep(1)  # Give some time to load new rows
        print("Scrolled down.")
    except Exception as e:
        print(f"Scroll failed: {e}")

def get_discounts_detail(driver, date, restaurant_id):
    def wait(frame=0):
        time_lapsed = 0
        while True:
            driver.switch_to.default_content()
            iframe = wait_visible(driver, '//iframe[@id="metrics-powerbi-frame"]')
            driver.switch_to.frame(iframe)
            frames = driver.find_elements(By.XPATH, '//iframe[@allowfullscreen="true"]')
            driver.switch_to.frame(frames[frame])
            soup = BeautifulSoup(driver.page_source, "html.parser")
            rows = soup.find_all("div", attrs={"aria-rowindex": re.compile("\\d+")})
            #print(f"Waiting for data in frame {frame}: Found {len(rows)} rows")
            if len(rows) > 0:
                break
            elif time_lapsed >= 40:
                print("Timeout: Data not found in frame")
                return False
            time_lapsed += 1
            time.sleep(1)
        return True

    data = {}
    #print("Fetching data from the first pane...")
    data_available = wait(0)
    if not data_available:
        print("Data not fetched from 1st pane.")
        return False

    refresh(driver, frame=1)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.find_all("div", attrs={"aria-rowindex": re.compile("\\d+")})
    #print(f"Found {len(rows)} rows in first pane.")

    try:
        row_counter = 0
        while row_counter < len(rows):
            row = rows[row_counter]
            row_index = row.get("aria-rowindex")
            
            if row_index and int(row_index) > 1:
                campaign_div = row.find("div", {"aria-colindex": "1"})
                campaign_text = campaign_div.text.strip() if campaign_div else ""

                orders = row.find("div", {"aria-colindex": "2"}).text.strip() if row.find("div", {"aria-colindex": "2"}) else ""
                revenue = row.find("div", {"aria-colindex": "3"}).text.strip() if row.find("div", {"aria-colindex": "3"}) else ""
                spends = row.find("div", {"aria-colindex": "4"}).text.strip() if row.find("div", {"aria-colindex": "4"}) else ""
                aov = row.find("div", {"aria-colindex": "5"}).text.strip() if row.find("div", {"aria-colindex": "5"}) else ""

                revenue = re.sub(r"[^0-9.]", "", revenue)
                spends = re.sub(r"[^0-9.]", "", spends)
                aov = re.sub(r"[^0-9.]", "", aov)

                # Attempt to find and click the expandable button
                expanded = False
                campaign_text_detail = ""
                campaign = ""

                try:
                    row_xpath = f'//div[@aria-rowindex="{row_index}"]'
                    expand_button_xpath = f'{row_xpath}//div[@aria-colindex="1"]//div[contains(@class, "expandCollapseButton clickable")]/i[@role="button"]'

                    expand_button = driver.find_element(By.XPATH, expand_button_xpath)
                    driver.execute_script("arguments[0].scrollIntoView(true);", expand_button)
                    expand_button.click()
                    #print(f"Clicked expand button for row {row_index}")
                    time.sleep(1)

                    # Capture the expanded content
                    expanded_content_xpath = f'//div[@aria-rowindex="{int(row_index) + 1}"]//div[contains(@class, "expandableContent pivotTableCellWrap tablixAlignLeft")]'
                    
                    # Wait for the expanded content to appear
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, expanded_content_xpath))
                    )
                    
                    campaign_text_detail = driver.find_element(By.XPATH, expanded_content_xpath).text.strip()
                    #print(f"Fetched campaign_text_detail: {campaign_text_detail}")
                    
                    expanded = True

                    # Collapse the row back
                    expand_button = driver.find_element(By.XPATH, expand_button_xpath)
                    expand_button.click()
                    #print(f"Collapsed row {row_index}")
                    time.sleep(1)

                except Exception as e:
                    print(f"No expandable button found for row {row_index}, attempting to scroll. Error: {e}")
                    scroll_down(driver)
                    row_counter -= 1  # Retry the same row after scrolling

                # Add data to the dictionary
                data[campaign_text] = {
                    'business_date': date,
                    'created_at': get_current_date_formated(),
                    'updated_at': get_current_date_formated(),
                    'restaurant_id': restaurant_id,
                    'campaign': campaign_text_detail,
                    'orders': orders,
                    'revenue': revenue,
                    'spends': spends,
                    'aov': aov
                }
                #printLog(f"Processed data for campaign {campaign_text}: {data[campaign_text]}")
            
            row_counter += 1

    except Exception as e:
        printLog(f"Error: {e}")
        return False

    #print("Fetching data from the second pane...")
    data_available = wait(1)
    if not data_available:
        print("Data not fetched from 2nd pane.")
        return False

    refresh(driver, frame=2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.find_all("div", attrs={"aria-rowindex": re.compile("\\d+")})
    #print(f"Found {len(rows)} rows in second pane.")

    try:
        for row in rows:
            row_index = row.get("aria-rowindex")
            if row_index and int(row_index) > 1:
                campaign_div = row.find("div", {"aria-colindex": "1"})
                campaign_text = campaign_div.text.strip() if campaign_div else ""

                new_user_orders = row.find("div", {"aria-colindex": "2"}).text.strip() if row.find("div", {"aria-colindex": "2"}) else ""
                repeat_user_orders = row.find("div", {"aria-colindex": "3"}).text.strip() if row.find("div", {"aria-colindex": "3"}) else ""
                dormant_user_orders = row.find("div", {"aria-colindex": "4"}).text.strip() if row.find("div", {"aria-colindex": "4"}) else ""

                if campaign_text in data:
                    data[campaign_text].update({
                        'new_user_orders': new_user_orders,
                        'repeat_user_orders': repeat_user_orders,
                        'dormant_user_orders': dormant_user_orders
                    })
                    #print(f"Updated data for campaign {campaign_text}: {data[campaign_text]}")
    except Exception as e:
        printLog(f"Error: {e}")
        return False

    if len(data.keys()) > 0:
        for campaign, record in data.items():
            query = """
            INSERT INTO swiggy_discount_metrics (
                business_date, created_at, updated_at, restaurant_id, campaign, orders, revenue,
                spends, aov, new_user_orders, repeat_user_orders, dormant_user_orders)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            execute_query(query, tuple(record.values()))

        connection.commit()
    return True

def process_discount_campaign_performance(driver, restaurant_ids, restaurants_names):

    """
    Scrapes discount campaign performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
        phone_ (str): Phone number associated with the account.
    """
    
    def retry():
        url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'
        driver.get(url)
        
        refresh(driver)

    cities_ = list({res.split(',')[-1].strip() for res in restaurants_names})

    url = f'https://partner.swiggy.com/business-metrics/discount-performance/restaurant/{restaurant_ids[0]}'

    for i in range(len(cities_)):
        printLog(f"--Scraping for city: {cities_[i]}")

        driver.get(url)
        handle_container_popups(driver)
        popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
        try:
        # Check if the popup close button exists and is interactable
            close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
            if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                wait_click(driver, popup_close_xpath)  # Click if interactable
            else:
                print("Popup exists but is not interactable or does not need closing")
        except:
            print(f"error handling popup 1")

        refresh(driver)
        time.sleep(2)
        individual_button_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[3]/button[2]'
        individual_button = wait_visible(driver,individual_button_xpath,10)
        if individual_button is None:
            retry()
        
        try:
            wait_click(driver, individual_button_xpath)
            city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div')
            if city_pane is None:
                city_pane = driver.find_element(By.CLASS_NAME, 'Stack__StackBody-b66d5h-0 dQWwyk SelectFilterList__StyledStack-sc-105m70e-1 jFgQhN')
            cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12 ')
            
            # Locate and use the search bar to search for the city
            search_bar = city_pane.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')  # Adjust placeholder if needed
            search_bar.clear()  # Clear any pre-filled text if necessary
            search_bar.send_keys(cities_[i])  # Type the city name
            
            # Wait for the city list to load and select the desired city
            wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
            cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

            for city in cities:
                if city.text == cities_[i]:
                    city.click()
                    break

            # Try clicking the Continue button
            continue_btn_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button'
            try:
                wait_click(driver, continue_btn_xpath)
                printLog("Continue button clicked")
            except Exception as e:
                printLog("Continue button click failed, retrying city click...")

                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                cities = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                for city in cities:
                    if city.text == cities_[i]:
                        city.click()
                        break

                try:
                    wait_click(driver, continue_btn_xpath)
                    printLog("Continue button clicked after retry")
                except Exception as e:
                    printLog("Continue button click failed even after retry")

            
            # Wait until the wrapper is visible
            wrapper_xpath = '//div[contains(@class, "SelectFilterList__ListWrapper-sc-105m70e-11")]'
            wrapper = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, wrapper_xpath))
            )

            # Initialize a set to store unique RIDs
            rest_ids = set()
            prev_count = -1

            while True:
                # Find all elements with RID text
                elements = wrapper.find_elements(By.CSS_SELECTOR, ".OutletFilterList__Description-nloq1s-1")
                
                # Add newly found RIDs
                for element in elements:
                    text = element.text.strip()
                    if "RID:" in text:
                        rest_ids.add(text.split(":")[-1].strip())

                # Break if no new elements found after scrolling
                if len(rest_ids) == prev_count:
                    break
                prev_count = len(rest_ids)

                # Scroll down inside the wrapper
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", wrapper)
                time.sleep(0.5)

            res_ids = list(rest_ids)

            printLog("Extracted RIDs:", res_ids)

            # Search for the first RID
            search_bar_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input'
            search_bar = driver.find_element(By.XPATH, search_bar_xpath)
            search_bar.clear()
            search_bar.send_keys(res_ids[0])

            # Select outlets
            wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
            outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
            for outlet in outlets:
                try:
                    outlet.click()
                except Exception as e:
                    printLog(f"Error clicking outlet: {type(e).__name__} - {e}")

            # Try clicking Confirm
            confirm_button_xpath = '//div[contains(@class, "SelectFilterList__StyledFooter-sc-105m70e-9")]//button[text()="Confirm" and not(@disabled)]'
            try:
                wait_click(driver, confirm_button_xpath)
                printLog("Confirm button clicked")
            except Exception as e:
                printLog("Confirm button click failed, retrying outlet click...")

                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                try:
                    if outlets:
                        outlets[0].click()
                        printLog("Clicked first visible outlet")
                    else:
                        printLog("No outlets found on retry")
                except Exception as e:
                    printLog(f"Retry outlet click failed: {type(e).__name__} - {e}")

                try:
                    wait_click(driver, confirm_button_xpath)
                    printLog("Confirm button clicked after retry")
                except Exception as e:
                    printLog("Confirm button click failed even after retry")


            for res_id in res_ids:

                printLog(f"scraping discount performance for restaurant id: {res_id}")

                Outlet_Filter_button_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[1]/div[2]/div[2]/button[2]'
                wait_click(driver, Outlet_Filter_button_xpath)

                search_bar = driver.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')
                search_bar.clear()
                search_bar.send_keys(res_id) 

                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                for outlet in outlets:
                    outlet.click()

                wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')                        

                coupon_performance_button_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[2]'
                coupon_performance_button = wait_visible(driver, coupon_performance_button_xpath, 10)
                if coupon_performance_button is None:
                    retry()

                wait_click(driver, coupon_performance_button_xpath)

                query = """
                    SELECT DISTINCT date
                    FROM swiggy_discount_performance_month_track
                    WHERE restaurant_id = %s;
                    """
                cur.execute(query, (int(res_id),))

                record = cur.fetchone()                   

                if record is None:
                    printLog("--Record not available. Processing 90 days data.")

                    for i in range(90, 1, -1):
                        date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')

                        check = select_date(driver, date)

                        if not check:
                            driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                            continue

                        check = get_discounts_detail(driver, date, res_id)
                        if not check:
                            get_discounts_detail(driver, date, res_id)

                        driver.switch_to.default_content()

                        query = """
                            INSERT INTO swiggy_discount_performance_month_track (restaurant_id, date)
                            VALUES (%s, %s)
                            ON CONFLICT (restaurant_id)
                            DO UPDATE SET date = EXCLUDED.date
                            """
                        values = (res_id,date)
                        execute_query(query, values)
                        connection.commit()

                    printLog(f"data scraped for {res_id}")
                    printLog(f"Swiggy_months_track updated for {res_id}")

                else:
                    printLog("--Record available. Downloading new data...")

                    query = """
                        SELECT DISTINCT business_date
                        FROM swiggy_discount_metrics
                        WHERE restaurant_id = %s
                        AND campaign != 'Overall'
                        ORDER BY business_date DESC;
                    """
                    cur.execute(query, (res_id,))
                    records = cur.fetchall()
                    scraped_dates = [str(record[0]) for record in records]
                    previous_date = record[0]
                    printLog(f"previous_date:({previous_date})")
                    current_date = datetime.today().date()
                    delta_days = (current_date - previous_date).days

                    for i in range(delta_days, 0, -1):
                        delta = current_date - timedelta(days=i)
                        date = delta.strftime('%Y-%m-%d')

                        if date in scraped_dates:
                            printLog("Date is already scraped. Skipping date.")
                            continue

                        check = select_date(driver, date)
                        if not check:
                            driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                            continue

                        check = get_discounts_detail(driver, date, res_id)
                        if not check:
                            get_discounts_detail(driver, date, res_id)

                        query = """
                            UPDATE swiggy_discount_performance_month_track
                            SET 
                                date = %s
                            WHERE 
                                restaurant_id = %s
                            """
                        values = (date, res_id)
                        execute_query(query, values)
                        connection.commit()                   


                    printLog(f"data scraped for {res_id}")
                    printLog(f"Swiggy_months_track updated for {res_id}")
                
                driver.switch_to.default_content() 
                driver.switch_to.frame("metrics-powerbi-frame")
                  
        except Exception as e:

            
            if len(restaurant_ids) > 1:
                printLog(f"Unable to scrape for city: {cities_[i]}")
                continue
            elif len(restaurant_ids) == 1:
                printLog("ALERT!!! Account is having single restaurant. Scraping data.")
                single_account = True

            coupon_performance_button_2_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[2]'
            coupon_performance_button_2 = wait_visible(driver, coupon_performance_button_2_xpath)
            if coupon_performance_button_2 is None:
                retry()

            wait_click(driver, coupon_performance_button_2_xpath)

                
            query = """
                SELECT DISTINCT date
                FROM swiggy_discount_performance_month_track
                WHERE restaurant_id IN %(restaurant_list)s;
                """
            cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})
            record = cur.fetchone()             

            if record is None:
                printLog("--Record not available. Processing 90 days data.")
                
                for i in range(90,1,-1):

                    driver.switch_to.default_content()

                    date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
                    check = select_date(driver,date)
                    if not check:
                        driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                        continue
                        
                    check = get_discounts_detail(driver,date,restaurant_id)
                    if not check:
                        get_discounts_detail(driver,date,restaurant_id)

                    driver.switch_to.default_content()

                    query = """
                        INSERT INTO swiggy_discount_performance_month_track (restaurant_id, date)
                        VALUES (%s, %s)
                        ON CONFLICT (restaurant_id)
                        DO UPDATE SET date = EXCLUDED.date
                        """
                    values = (restaurant_id,date)
                    execute_query(query, values)
                    connection.commit()

            else:
                ids_str = ','.join(map(str, restaurant_ids))
                for restaurant_id in restaurant_ids:
                    cur.execute(f"select distinct business_date from swiggy_discount_metrics where restaurant_id in ({ids_str}) AND campaign != 'Overall' order by business_date desc")
                    records = cur.fetchall()
                    scraped_dates = [str(record[0]) for record in records]
                    previous_date = record[0]
                    printLog(f"previous_date:({previous_date})")
                    current_date = datetime.today().date()
                    delta_days = (current_date - previous_date).days

                    for i in range(delta_days, 0, -1):
                        delta = current_date - timedelta(days=i)
                        date = delta.strftime('%Y-%m-%d')

                        driver.switch_to.default_content()

                        if date in scraped_dates:
                            printLog("Date is already scraped. Skipping date.")
                            continue
                        
                        check = select_date(driver,date)
                        if not check:
                            driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                            continue
                        
                        check = get_discounts_detail(driver,date, restaurant_ids[0])
                        if not check:
                            get_discounts_detail(driver,date, restaurant_ids[0])
    
                        driver.switch_to.default_content()
                    
                        query = """
                        UPDATE swiggy_discount_performance_month_track
                        SET 
                            date = %s
                        WHERE 
                            restaurant_id = %s
                        """
                        values = (date, restaurant_id)
                        execute_query(query, values)

                refresh(driver)
        
                driver.switch_to.default_content()

### ************ DISCOUNT campaigns ************ ###
def handle_container_popups(driver):
    """
    Handles potential popups in the #mCSB_1_container before proceeding.
    """
    try:
        # Wait for the container to load
        container_xpath = '//div[@id="mCSB_1_container"]'
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, container_xpath))
        )

        # Look for popup close buttons inside the container
        popup_close_button_xpath = '//a[contains(@class, "dismiss") or contains(@class, "icon-close")]'  # Adjust as needed
        popups = driver.find_elements(By.XPATH, popup_close_button_xpath)
        
        if popups:
            for popup in popups:
                try:
                    wait_click(driver,popup_close_button_xpath)
                    time.sleep(1)  # Allow time for the popup to dismiss
                    print("Popup closed successfully.")
                except Exception as e:
                    print(f"Could not close popup: {e}")
        else:
            print("No popups found in the container.")
    except Exception as e:
        print(f"Error handling popups: {e}")

def clean_date(date_str):
    # Remove ordinal suffixes (st, nd, rd, th)
    return re.sub(r'(?<=\d)(st|nd|rd|th)', '', date_str)

def scroll_to_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)

def process_campaigns_details(driver):
    try:
        # Locate the Campaigns pane
        Campaigns_pane = wait_visible(driver, '//*[@id="mfe-root"]/div[2]/div[1]')
        if Campaigns_pane is None:
            Campaigns_pane = driver.find_element(By.CLASS_NAME, 'styled__DiscountsList-ssd-app__sc-5gi7jb-10 eyRptP')
        
        # Check if the Campaigns pane is scrollable
        is_scrollable = driver.execute_script(
            "return arguments[0].scrollHeight > arguments[0].clientHeight;", 
            Campaigns_pane
        )
        if not is_scrollable:
            print("The element is not scrollable.")
        
        # Find all 'View Details' buttons within the Campaigns pane
        Campaign_buttons = Campaigns_pane.find_elements(
            By.CLASS_NAME, 
            'button__ButtonLink-ssd-app__sc-1cruoaz-1.styled__ViewDetailsButton-ssd-app__sc-5gi7jb-16.gjdpsj.lpeBti'
        )
        
        # Iterate over all Campaign buttons
        for index, Campaign_button in enumerate(Campaign_buttons):
            try:
                # Scroll to the button
                driver.execute_script("arguments[0].scrollIntoView({ behavior: 'smooth', block: 'center' });", Campaign_button)
                time.sleep(0.5)  # Allow time for scrolling
                
                # Click the button
                Campaign_button.click()
                print(f"Clicked on campaign button {index + 1}")

                time.sleep(5)  # Allow time for action to complete
                
                #Extract Campaign date
                discount_timeline_element = driver.find_element(By.CLASS_NAME, 'styled__DiscountCardContainer-ssd-app__sc-5gi7jb-11')
                # Extract the text content from the element
                timeline_text = discount_timeline_element.text
                
                # Use regex to extract the dates
                dates = re.findall(r'\d{1,2}[a-zA-Z]{2} \w+ \d{4}', timeline_text)
                if len(dates) == 2:
                    campaign_start_date_str = clean_date(dates[0])
                    campaign_start_date_object = datetime.strptime(campaign_start_date_str, '%d %B %Y')
                    campaign_start_date = campaign_start_date_object.strftime('%Y-%m-%d')

                    campaign_end_date_str = clean_date(dates[1])
                    campaign_end_date_object = datetime.strptime(campaign_end_date_str, '%d %B %Y')
                    campaign_end_date = campaign_end_date_object.strftime('%Y-%m-%d')

                if len(dates) == 1:
                    campaign_start_date_str = clean_date(dates[0])
                    campaign_start_date_object = datetime.strptime(campaign_start_date_str, '%d %B %Y')
                    campaign_start_date = campaign_start_date_object.strftime('%Y-%m-%d')

                    campaign_end_date = datetime.today().date().strftime('%Y-%m-%d')

                time.sleep(0.5) 

                # Locate the element using XPath
                discount_text_element = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/p')                
                # Extract the text content from the element
                discount_text = discount_text_element.text
                # Initialize default values
                discount = None
                capping = 0
                mov = None
                target = ""

                # Extract discount
                discount_match = re.search(r'(\d+%)|Flat Rs\.?(\d+)', discount_text, re.IGNORECASE)
                if discount_match:
                    if discount_match.group(1):  # Percentage discount
                        discount = discount_match.group(1)
                    elif discount_match.group(2):  # Flat Rs discount
                        discount = f" {discount_match.group(2)}"
                else:
                    print(f"discount not scraped")

                # Extract capping
                capping_match = re.search(r'(?:up\s*to|upto)\s*Rs\.?(\d+)', discount_text, re.IGNORECASE)
                if capping_match:
                    capping = int(capping_match.group(1))   
                else:
                    print(f"capping not scraped")

                # Extract MOV
                mov_match = re.search(r'orders above Rs\.?\s*(\d+)', discount_text, re.IGNORECASE)
                if mov_match:
                    mov = int(mov_match.group(1))                   
                else:
                    print(f"mov not scraped")

                # Extract target
                target_match = re.search(r'\b(new|dormant|returning|all)\b', discount_text, re.IGNORECASE)
                if target_match:
                    target_text = f"{target_match.group(1)} users"  
                    target = [e.strip() for e in target_text.split(",")]                 
                else:
                    print(f"target not scraped")

                time.sleep(0.5) 

                # Extract target
                target_element = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[1]/div[2]')
                # Extract the text
                target_1_text = target_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.gqVKKa.TSSQe').text
                target_1 = [e.strip() for e in target_1_text.split(",")]
                time.sleep(0.5) 

                if target_1 == ['Custom'] :  
                    target_value = target
                else:
                    target_value = target_1

                #Extract offer share
                Cost_sharing_element = driver.find_element(By.XPATH,'//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[2]/div[2]')
                Offer_Share_text = Cost_sharing_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.gqVKKa.TSSQe').text
                Offer_Share_find= re.search(r'\d+%', Offer_Share_text)
                if Offer_Share_find:
                    Offer_Share = Offer_Share_find.group(0)  # Get the matched percentage
                else:
                    print("No percentage found in the text.")
                
                time.sleep(0.5) 

                # Extract Menu
                Menu_inclusion_element = driver.find_element(By.XPATH,'//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[3]/div[2]')
                Menu = Menu_inclusion_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.gqVKKa.TSSQe').text
                time.sleep(0.5) 
                
                # Extract discount schedule
                discount_schedule_xpath = '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[4]/div[2]'
                # Locate the element
                discount_element = driver.find_element(By.XPATH, discount_schedule_xpath)

                time_slot_XPath = '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[4]/div[2]/p[2]/span[1]'
                time_slot_element = driver.find_element(By.XPATH,time_slot_XPath)

                # Default values
                discount_days = "All days of the week"
                extracted_time_slot = "All day"

                # Process the discount schedule element
                if discount_element:
                    try:
                        # Extract time_slot
                        time_slot_element_text = discount_element.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[4]/div[2]/p[2]/span[1]/span[1]')
                        extracted_time_slot = time_slot_element_text.text.strip()  # Extract text and remove leading/trailing spaces
                    except Exception as e:
                        extracted_time_slot = "All day"
                    try:
                        # Extract discount_days
                        day_elements = discount_element.find_elements(By.XPATH, ".//span[@type='highlight']")
                        if not day_elements:  # Check if day_elements is empty
                            raise Exception("day_elements is empty")
                        discount_days = ", ".join([day.text.strip() for day in day_elements])  # Join days with comma
                    except Exception as e:
                        discount_days = discount_element.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[4]/div[2]/p[2]').text
                else:
                    print("Discount schedule element not found")

                # Define the mapping of time slots to their labels
                time_slot_mapping = {
                    "07:00am - 11:00am": "Breakfast",
                    "11:00am - 03:00pm": "Lunch",
                    "03:00pm - 07:00pm": "Snacks",
                    "07:00pm - 11:00pm": "Dinner",
                }
                
                if extracted_time_slot in time_slot_mapping:
                    time_slot = time_slot_mapping[extracted_time_slot]
                else:
                    time_slot = extracted_time_slot

                time.sleep(0.5) 

                #Extract Restaurant id
                RID_Element = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[2]/div[2]')
                RID_Element_text = RID_Element.find_element(By.CSS_SELECTOR, '.text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.gqVKKa.eNDuN').text
                # Extract Restaurant_Id using regular expression

                #match = re.search(r'RID(?:\s*\(\d+\))?\s*:\s*([\d\s|]+)', RID_Element_text)
                match = re.search(r'RID\s*:\s*([\d| ]+)', RID_Element_text)

                if match:
                    restaurant_id_text = match.group(1)
                    restaurant_ids = [rid.strip() for rid in restaurant_id_text.split('|') if rid.strip()]
                else:
                    print("Restaurant_Id not found.")

                print("Campaign Start Date:", campaign_start_date)
                print("Campaign End Date:", campaign_end_date)
                print(f"dicount:{discount}")
                print(f"capping:{capping}")
                print(f"mov:{mov}")
                print(f"target:{target}")
                print(f"target_1: {target_1}")
                print(f"Offer_Share:{Offer_Share}")
                print(f"Menu: {Menu}")
                print(f"Discount Days: {discount_days}")
                print(f"Time Slot: {time_slot}")
                print(f"Restaurant_Ids: {restaurant_ids}")
                print(f"target_value: {target_value}")

                # Trim whitespace and iterate over each restaurant_id
                for restaurant_id in restaurant_ids:
                    
                    data = {
                        'restaurant_id': restaurant_id,
                        'created_at' : get_current_date_formated(),
                        'updated_at' : get_current_date_formated(),
                        'campaign_start_date': campaign_start_date,
                        'campaign_end_date': campaign_end_date,
                        'capping': capping,
                        'mov': mov,
                        'discount_slot': time_slot,
                        'target': target_value,  # Ensure target is a string; adjust formatting if needed
                        'merchant_share': Offer_Share,
                        'discount_days': discount_days,
                        'discount': discount,
                        'menu': Menu,
                    }

                    try:
                        # SQL query with placeholders
                        query = """
                        INSERT INTO swiggy_discount_campaigns (
                            restaurant_id, 
                            created_date, 
                            updated_date, 
                            campaign_start_date, 
                            campaign_end_date, 
                            capping, 
                            mov, 
                            discount_slot, 
                            target, 
                            merchant_share, 
                            discount_days, 
                            discount, 
                            menu
                        )
                        VALUES (
                            %(restaurant_id)s, 
                            %(created_at)s, 
                            %(updated_at)s, 
                            %(campaign_start_date)s, 
                            %(campaign_end_date)s, 
                            %(capping)s, 
                            %(mov)s, 
                            %(discount_slot)s, 
                            %(target)s, 
                            %(merchant_share)s, 
                            %(discount_days)s, 
                            %(discount)s, 
                            %(menu)s
                        );
                        """
                        
                        # Execute the query with data
                        cur.execute(query, data)
                        
                        # Commit the transaction
                        connection.commit()
                        printLog(f"Data for Restaurant ID {data['restaurant_id']} inserted successfully.")

                    except Exception as e:
                        printLog(f"Error inserting data: {e}")
                        connection.rollback()
                    
                time.sleep(5) 
            except Exception as e:
                print(f"Error clicking campaign button {index + 1}, {e}")
        
    except Exception as e:
        printLog(f"No discount campaigns to scrape.")

def process_discount_campaigns(driver, restaurant_ids, restaurants_names):
    """
    Scrapes discount performance data for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
    """
    cities_ = list({res.split(',')[-1].strip() for res in restaurants_names})
    
    url = 'https://partner.swiggy.com/growth/discounts'

    for i in range(len(cities_)):
        try:
            printLog(f"--Scraping for city: {cities_[i]}")

            driver.get(url)

            handle_container_popups(driver)

            time.sleep(2)

            popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
            try:
                # Check if the popup close button exists and is interactable
                close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
                if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                    wait_click(driver, popup_close_xpath)  # Click if interactable
                else:
                    print("Popup exists but is not interactable or does not need closing")
            except NoSuchElementException:
                print("Popup close button not found")
            except ElementNotInteractableException:
                print("Popup close button is not interactable")
            except TimeoutException:
                print("Timed out waiting for popup close button")
            except Exception as e:
                print(f"Unexpected error: {e}")

            # Wait for the iframe to load
            iframe = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="mfe-frame"]'))
            )
            if not iframe:
                print(f"iframe not found")

            # Switch to iframe
            Switch_to_iframe = WebDriverWait(driver, 10).until(EC.frame_to_be_available_and_switch_to_it(iframe))
            if not Switch_to_iframe:
                print(f"couldnot switch to iframe")

            # Find the scrollable container inside the iframe
            scrollable_container = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="mfe-root"]/div/div/div'))
            )
            if not scrollable_container:
                print(f"scrollable container not found")

            # Scroll the container till the View Active Discounts button is visible
            track_discounts_button_xpath = '//*[@id="mfe-root"]/div/div/div/div/div[2]/div[1]/div/div/div[2]/div[8]/div/div/div[1]/div[2]'
            last_scroll_height = 0
            
            while True:
                # Check if the button is visible
                track_discounts_button_found = wait_visible(driver, track_discounts_button_xpath, time=2)
                if not track_discounts_button_found:
                    print("track_discounts_button not found")
                    break

                # Scroll down by the container height
                driver.execute_script(
                    "arguments[0].scrollTop += arguments[0].clientHeight;", scrollable_container
                )

                # Wait briefly for new content to load
                time.sleep(1)

                # Check if the scroll height remains the same (reached bottom)
                current_scroll_height = driver.execute_script(
                    "return arguments[0].scrollTop;", scrollable_container
                )
                if current_scroll_height == last_scroll_height:
                    break

                last_scroll_height = current_scroll_height

            # Click the View Active Discounts button
            wait_click(driver, track_discounts_button_xpath)

            city_pane = wait_visible(driver,'//*[@id="mfe-root"]/div[3]/div/div[2]/div')
            if city_pane is None:
                city_pane = driver.find_element(By.CLASS_NAME, 'container__Container-ssd-app__sc-1qiheoo-0 styled__ListWrapper-ssd-app__sc-p639ph-0 hnhWbv hWOvAi')
            
            is_scrollable = driver.execute_script(
                "return arguments[0].scrollHeight > arguments[0].clientHeight;", 
            city_pane
            )
            if not is_scrollable:
                print("The element is not scrollable.")
                
            # Find all city elements within the scrollable list
            cities = city_pane.find_elements(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__Title-ssd-app__sc-1ahblbp-1')

            # Iterate over the desired cities
        
            for city in cities:
                scroll_to_element(driver, city)  # Scroll to bring the city into view
                if city.text == cities_[i]:
                    city.click()
                    city_found = True
                    break
            # Ensure proper error handling if a city isn't found
            else:
                print(f"City '{cities_[i]}' not found in the list.")

            wait_click(driver,'//*[@id="mfe-root"]/div[3]/div/button') #confirmbutton
            try:
                #wait for outlet pane to load
                Outlet_pane = wait_visible(driver,'//*[@id="mfe-root"]/div[3]/div')
                confirm_button_xpath = '//*[@id="mfe-root"]/div[3]/div/button' #confirm
                wait_click(driver,confirm_button_xpath)      
            except:
                print(f"{cities_[i]} has a single outlet ")      

            time.sleep(2)
            
            process_campaigns_details(driver)

            time.sleep(2)

        except Exception as e:
            print(f"Account has single outlet.")
            time.sleep(2) 

            process_campaigns_details(driver)

            time.sleep(2)

### ************ CUSTOMER FUNNEL ************ ###

def handle_container_popups(driver):
    """
    Handles potential popups in the #mCSB_1_container before proceeding.
    """
    try:
        # Wait for the container to load
        container_xpath = '//div[@id="mCSB_1_container"]'
        container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, container_xpath))
        )

        # Look for popup close buttons inside the container
        popup_close_button_xpath = '//a[contains(@class, "dismiss") or contains(@class, "icon-close")]'  # Adjust as needed
        popups = driver.find_elements(By.XPATH, popup_close_button_xpath)
        
        if popups:
            for popup in popups:
                try:
                    wait_click(driver,popup_close_button_xpath)
                    time.sleep(1)  # Allow time for the popup to dismiss
                    print("Popup closed successfully.")
                except Exception as e:
                    print(f"Could not close popup: {e}")
        else:
            print("No popups found in the container.")
    except Exception as e:
        print(f"Error handling popups: {e}")

def are_dates_same(date_str1, date_str2):
    # Define possible date formats
    date_formats = [
        "%Y-%m-%d",     # Example: 2024-06-10
        "%d-%m-%Y",     # Example: 10-06-2024
        "%d/%m/%Y",     # Example: 06/10/2024
        "%Y/%m/%d",     # Example: 2024/06/10
        "%B %d, %Y",    # Example: June 10, 2024
        "%d %B %Y",     # Example: 10 June 2024
        "%b %d, %Y",    # Example: Jun 10, 2024
        "%d %b %Y",     # Example: 10 Jun 2024
        "%Y-%m-%dT%H:%M:%S", # Example: 2024-06-10T15:30:00
        "%Y-%m-%d %H:%M:%S"  # Example: 2024-06-10 15:30:00
        # Add more formats as needed
    ]
    
    def parse_date(date_str):
        for date_format in date_formats:
            try:
                return datetime.strptime(date_str, date_format)
            except ValueError:
                continue
        raise ValueError(f"Date format for '{date_str}' is not recognized")
    
    # Parse the dates
    date1 = parse_date(date_str1)
    date2 = parse_date(date_str2)
    
    # Compare the dates
    return date1.date() == date2.date()

def select_date_funnel(driver, date):
    
    driver.switch_to.default_content()

    try:
        wait_click(driver, '/html/body/div[2]/div/div/button')  # Popup close button if it appears
    except:
        pass

    time.sleep(3)

    # 🔁 Switch to correct iframe again
    driver.switch_to.frame("mfe-frame")

    #click on filter button
    Outlet_filter_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/button[2]/span'
    wait_click(driver,Outlet_filter_button_xpath)    
    
    # ✅ Click "Custom" button
    custom_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[2]/div[1]/div[5]/div[2]/div'    
    try:
        wait_click(driver, custom_button_xpath)
        printLog("Clicked Custom Date button")
        time.sleep(2)
    except:
        '''
        driver.switch_to.default_content()
        popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[4]/ul/div/div/div[1]/a'
        try:
        # Check if the popup close button exists and is interactable
            close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
            if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                wait_click(driver, popup_close_xpath)  # Click if interactable
            else:
                print("Popup exists but is not interactable or does not need closing")
        except:
            print(f"error handling popup 1")

        driver.switch_to.frame("mfe-frame")
        wait_click(driver, custom_button_xpath)
        time.sleep(2)
        '''
        printLog("Custom Date button click failed.")
        return False        

    # 📅 Setup date
    provided_date = datetime.strptime(date, '%Y-%m-%d')
    provided_month = provided_date.month
    provided_year = provided_date.year
    day = provided_date.day

    try:        
        from_input_element = wait_visible(driver, '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[2]/div[1]/div[1]')
        to_input_element = wait_visible(driver, '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[2]/div[2]/div[1]')
        printLog("Located input fields")
    except Exception as e:
        printLog(f"Error locating input fields: {e}")
        return False

    try:
        # Locate the current visible calendar month text (add xpath)
        calendar_month_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[3]/div/div[1]/button[3]/span'
        month_element = wait_visible(driver, calendar_month_xpath)
        if not month_element:
            printLog("Could not find calendar month element")
            return False

        current_month_text = month_element.text  # Example: 'March 2025'
        current_date = datetime.strptime(current_month_text, '%B %Y')

        while current_date.month != provided_month or current_date.year != provided_year:
            if provided_date < current_date:
                printLog("Navigating to previous month")
                prev_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[3]/div/div[1]/button[2]'
                wait_click(driver, prev_button_xpath)
            else:
                printLog("Navigating to next month")
                next_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[3]/div/div[1]/button[4]'
                wait_click(driver, next_button_xpath)
            
            time.sleep(1.5)
            month_element = wait_visible(driver, calendar_month_xpath)
            current_month_text = month_element.text
            current_date = datetime.strptime(current_month_text, '%B %Y')

        printLog(f"Searching for date button with value: {day}")
        date_buttons = driver.find_elements(By.CLASS_NAME, "react-calendar__tile")
        found = False
        
        expected_full_date = provided_date.strftime('%B %#d, %Y')  # Use '%#d' for Windows

        for button in date_buttons:
            try:
                abbr = button.find_element(By.TAG_NAME, "abbr")
                if abbr:
                    full_date_text = abbr.get_attribute('aria-label') or abbr.get_attribute('title')
                    if full_date_text == expected_full_date:
                        printLog(f"Clicking date button for {expected_full_date}")
                        button.click()
                        time.sleep(0.3)
                        button.click()
                        found = True
                        break
            except Exception as e:
                printLog(f"Error while checking date button: {e}")
                continue

        if not found:
            printLog("Could not find the date in the calendar")
            return False

        # Check if from and to values match
        start = from_input_element.text
        print("Selected start date ", start)
        end = to_input_element.text
        print("Selected end date ", end)

        if are_dates_same(start, end):
            printLog(f"Date selected: {date}")
            
            #click Date Confirm button
            confirm_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[4]/div'
            wait_click(driver, confirm_button_xpath)

            #Click Filter Apply button
            Apply_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[3]/div/button'
            wait_click(driver,Apply_button_xpath)

            return True
        else:
            printLog(f"Date selection mismatch - From: {start}, To: {end}")
            
            #click Date Confirm button
            confirm_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[4]/div'
            wait_click(driver, confirm_button_xpath)

            #Click Filter Apply button
            Apply_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[3]/div/button'
            wait_click(driver,Apply_button_xpath)

            return False

    except Exception as e:
        printLog(f"Exception during date selection: {e}")

        #click Date Confirm button
        confirm_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[3]/div/div/div[4]/div'
        wait_click(driver, confirm_button_xpath)

        return False

# This function is called from scrape_daily_metrics to extract values from daily metrics using xpath
def extract_number_from_text(driver,text_xpath):

    try:
        text = driver.find_element(By.XPATH, text_xpath).text
    except Exception as e:
        print(f"[ERROR] Failed to scrape daily metrics: {e}")
        return None
    
    # Remove commas, currency, %, min, etc.
    clean_text = re.sub(r'[^\d.,]', '', text).replace(',', '')
    
    # Find first number: integer or decimal
    match = re.search(r'\d+(?:\.\d+)?', clean_text)
    if match:
        num_str = match.group()
        return float(num_str) if '.' in num_str else int(num_str)
    
    print(f"--Failed to find number in the scraped text: {text}")
    return None

def scrape_daily_metrics(driver, restaurant_id, date):
    """
    Scrapes customer funnel data (Menu, Cart, Orders) for a given restaurant and date,
    and stores it into the Swiggy_daily_metrics table.
    """
    try:
        printLog(f"[INFO] Starting funnel scrape for Restaurant ID: {restaurant_id}, Date: {date}")

        # Step 1: Switch to correct iframe
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame("mfe-frame")
            print("[INFO] Switched to 'mfe-frame'")
        except Exception as e:
            print(f"[ERROR] Failed to switch to iframe: {e}")
            return

        time.sleep(0.5)

        # Step 2: Define XPaths
        sales_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/div[1]/div/div[2]/div'
        delivered_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/div[2]/div/div[2]/div'
        aov_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/div[3]/div/div[2]/div'
        cancelled_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/div[4]/div/div[2]/div'
        cancelled_orders_loss_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[1]/div/div/div[2]/div/div[5]/div/div[2]/div'
        rated_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div'
        poor_rated_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[2]/div/div/div[2]/div/div[2]/div/div[2]/div'
        complaints_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[3]/div/div/div[2]/div[1]/div[2]/div/div[2]/div'
        complaints_unresolved_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[3]/div/div/div[2]/div[1]/div[3]/div/div[2]/div'
        complaints_wrong_missing_items_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[3]/div/div/div[2]/div[2]/div[3]/div[1]/div[2]'
        complaints_quality_quantity_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[3]/div/div/div[2]/div[2]/div[3]/div[2]/div[2]'
        complaints_packaging_spillage_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[1]/div[3]/div/div/div[2]/div[2]/div[3]/div[3]/div[2]'
        bolt_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[1]/div/div/div[2]/div/div[1]/div/div[2]/div'
        bolt_aov_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[1]/div/div/div[2]/div/div[2]/div/div[2]/div'
        bolt_kpt_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[1]/div/div/div[2]/div/div[3]/div/div[2]/div'
        bolt_orders_kpt_under_6_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[1]/div/div/div[2]/div/div[5]/div/div[2]/div'
        funnel_impressions_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div/div/div[1]/div/div[2]'
        funnel_menu_opens_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div/div/div[3]/div/div[2]'
        funnel_cart_builds_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div/div/div[5]/div/div[2]'
        funnel_orders_placed_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[2]/div/div/div[2]/div/div[1]/div/div[2]/div/div/div[7]/div/div[2]'
        customers_new_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[3]/div/div/div[2]/div/div[1]/div/div[2]/div'
        customers_repeat_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[3]/div/div/div[2]/div/div[2]/div/div[2]/div'
        customers_dormant_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[3]/div/div/div[2]/div/div[3]/div/div[2]/div'
        ads_cpc_sales_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[1]/div[2]/div[1]/div/div[2]/div'
        ads_cpc_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[1]/div[2]/div[2]/div/div[2]/div'
        ads_cpc_mo_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[1]/div[2]/div[3]/div/div[2]/div'
        ads_cpc_spend_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[1]/div[2]/div[4]/div/div[2]/div'
        ads_cba_impressions_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[2]/div[2]/div[1]/div/div[2]/div'
        ads_cba_mo_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[4]/div/div/div[2]/div/div[2]/div[2]/div[2]/div/div[2]/div'
        discount_sales_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div/div/div[2]/div/div[1]/div/div[2]/div'
        discount_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div/div/div[2]/div/div[2]/div/div[2]/div'
        discount_amount_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[5]/div/div/div[2]/div/div[3]/div/div[2]/div'
        visibility_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[6]/div/div/div[2]/div/div[1]/div/div[2]/div'
        kpt_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[6]/div/div/div[2]/div/div[2]/div/div[2]/div'
        food_ready_accuracy_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[6]/div/div/div[2]/div/div[3]/div/div[2]/div'
        delayed_orders_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[6]/div/div/div[2]/div/div[4]/div/div[2]/div'
        menu_score_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[7]/div/div/div[2]/div/div[1]/div/div[2]/div'
        items_with_photos_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[7]/div/div/div[2]/div/div[2]/div/div[2]/div'
        items_with_descriptions_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[2]/div[3]/div[7]/div/div/div[2]/div/div[3]/div/div[2]/div'

        # Step 3: Extract Values
        sales = extract_number_from_text(driver,sales_xpath)
        print(f"Sales : {sales}")

        delivered_orders = extract_number_from_text(driver,delivered_orders_xpath)
        print(f"Delivered Orders : {delivered_orders}")

        aov = extract_number_from_text(driver,aov_xpath)
        print(f"Aov : {aov}")

        cancelled_orders = extract_number_from_text(driver,cancelled_orders_xpath)
        print(f"Cancelled Orders : {cancelled_orders}")

        cancelled_orders_loss = extract_number_from_text(driver,cancelled_orders_loss_xpath)
        print(f"Cancelled Orders Loss : {cancelled_orders_loss}")

        rated_orders = extract_number_from_text(driver,rated_orders_xpath)
        print(f"Rated Orders : {rated_orders}")

        poor_rated_orders = extract_number_from_text(driver,poor_rated_orders_xpath)
        print(f"Poor Rated Orders : {poor_rated_orders}")

        complaints_orders = extract_number_from_text(driver,complaints_orders_xpath)
        print(f"Complaints Orders : {complaints_orders}")

        complaints_unresolved = extract_number_from_text(driver,complaints_unresolved_xpath)
        print(f"Complaints Unresolved : {complaints_unresolved}")

        complaints_wrong_missing_items = extract_number_from_text(driver,complaints_wrong_missing_items_xpath)
        print(f"Complaints Wrong Missing Items : {complaints_wrong_missing_items}")

        complaints_quality_quantity = extract_number_from_text(driver,complaints_quality_quantity_xpath)
        print(f"Complaints Quality Quantity : {complaints_quality_quantity}")

        complaints_packaging_spillage = extract_number_from_text(driver,complaints_packaging_spillage_xpath)
        print(f"Complaints Packaging Spillage : {complaints_packaging_spillage}")

        bolt_orders = extract_number_from_text(driver,bolt_orders_xpath)
        print(f"Bolt Orders : {bolt_orders}")

        bolt_aov = extract_number_from_text(driver,bolt_aov_xpath)
        print(f"Bolt Aov : {bolt_aov}")

        bolt_kpt = extract_number_from_text(driver,bolt_kpt_xpath)
        print(f"Bolt Kpt : {bolt_kpt}")

        bolt_orders_kpt_under_6 = extract_number_from_text(driver,bolt_orders_kpt_under_6_xpath)
        print(f"Bolt Orders With Kpt Under 6 : {bolt_orders_kpt_under_6}")

        funnel_impressions = extract_number_from_text(driver,funnel_impressions_xpath)
        print(f"Funnel Impressions : {funnel_impressions}")

        funnel_menu_opens = extract_number_from_text(driver,funnel_menu_opens_xpath)
        print(f"Funnel Menu Opens : {funnel_menu_opens}")

        funnel_cart_builds = extract_number_from_text(driver,funnel_cart_builds_xpath)
        print(f"Funnel Cart Builds : {funnel_cart_builds}")

        funnel_orders_placed = extract_number_from_text(driver,funnel_orders_placed_xpath)
        print(f"Funnel Orders Placed : {funnel_orders_placed}")

        customers_new = extract_number_from_text(driver,customers_new_xpath)
        print(f"Customers New : {customers_new}")

        customers_repeat = extract_number_from_text(driver,customers_repeat_xpath)
        print(f"Customers Repeat : {customers_repeat}")

        customers_dormant = extract_number_from_text(driver,customers_dormant_xpath)
        print(f"Customers Dormant : {customers_dormant}")

        ads_cpc_sales = extract_number_from_text(driver,ads_cpc_sales_xpath)
        print(f"Ads Cpc Sales : {ads_cpc_sales}")

        ads_cpc_orders = extract_number_from_text(driver,ads_cpc_orders_xpath)
        print(f"Ads Cpc Orders : {ads_cpc_orders}")

        ads_cpc_mo = extract_number_from_text(driver,ads_cpc_mo_xpath)
        print(f"Ads Cpc Mo : {ads_cpc_mo}")

        ads_cpc_spend = extract_number_from_text(driver,ads_cpc_spend_xpath)
        print(f"Ads Cpc Spend : {ads_cpc_spend}")

        ads_cba_impressions = extract_number_from_text(driver,ads_cba_impressions_xpath)
        print(f"Ads Cba Impressions : {ads_cba_impressions}")

        ads_cba_mo = extract_number_from_text(driver,ads_cba_mo_xpath)
        print(f"Ads Cba Mo : {ads_cba_mo}")

        discount_sales = extract_number_from_text(driver,discount_sales_xpath)
        print(f"Discount Sales : {discount_sales}")

        discount_orders = extract_number_from_text(driver,discount_orders_xpath)
        print(f"Discount Orders : {discount_orders}")

        discount_amount = extract_number_from_text(driver,discount_amount_xpath)
        print(f"Discount Amount : {discount_amount}")

        visibility = extract_number_from_text(driver,visibility_xpath)
        print(f"Visibility : {visibility}")

        kpt = extract_number_from_text(driver,kpt_xpath)
        print(f"Kpt : {kpt}")

        food_ready_accuracy = extract_number_from_text(driver,food_ready_accuracy_xpath)
        print(f"Food Ready Accuracy : {food_ready_accuracy}")

        delayed_orders = extract_number_from_text(driver,delayed_orders_xpath)
        print(f"Delayed Orders : {delayed_orders}")

        menu_score = extract_number_from_text(driver,menu_score_xpath)
        print(f"Menu Score : {menu_score}")

        items_with_photos = extract_number_from_text(driver,items_with_photos_xpath)
        print(f"Items With Photos : {items_with_photos}")

        items_with_descriptions = extract_number_from_text(driver,items_with_descriptions_xpath)
        print(f"Items With Descriptions : {items_with_descriptions}")


        # Step 6: Insert into database
        try:
            query = """
                INSERT INTO swiggy_daily_metrics (
                    restaurant_id, business_date, 
                    sales, delivered_orders, aov, cancelled_orders, cancelled_orders_loss, 
                    rated_orders, poor_rated_orders, complaints_orders, complaints_unresolved, 
                    complaints_wrong_missing_items, complaints_quality_quantity, complaints_packaging_spillage, 
                    bolt_orders, bolt_aov, bolt_kpt, bolt_orders_kpt_under_6, funnel_impressions, 
                    funnel_menu_opens, funnel_cart_builds, funnel_orders_placed, customers_new, 
                    customers_repeat, customers_dormant, ads_cpc_sales, ads_cpc_orders, ads_cpc_mo, 
                    ads_cpc_spend, ads_cba_impressions, ads_cba_mo, discount_sales, discount_orders, 
                    discount_amount, visibility, kpt, food_ready_accuracy, delayed_orders, menu_score, 
                    items_with_photos, items_with_descriptions
                ) VALUES (%s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (restaurant_id, date, 
                      sales, delivered_orders, aov, cancelled_orders, cancelled_orders_loss, 
                      rated_orders, poor_rated_orders, complaints_orders, complaints_unresolved, 
                      complaints_wrong_missing_items, complaints_quality_quantity, complaints_packaging_spillage, 
                      bolt_orders, bolt_aov, bolt_kpt, bolt_orders_kpt_under_6, funnel_impressions, 
                      funnel_menu_opens, funnel_cart_builds, funnel_orders_placed, customers_new, 
                      customers_repeat, customers_dormant, ads_cpc_sales, ads_cpc_orders, ads_cpc_mo, 
                      ads_cpc_spend, ads_cba_impressions, ads_cba_mo, discount_sales, discount_orders, 
                      discount_amount, visibility, kpt, food_ready_accuracy, delayed_orders, menu_score, 
                      items_with_photos, items_with_descriptions)
            execute_query(query, values)
            connection.commit()
            printLog(f"[SUCCESS] Funnel data saved for {restaurant_id} on {date}")
        except Exception as e:
            printLog(f"[ERROR] Database insertion failed: {e}")
            return

    except Exception as e:
        printLog(f"[FATAL ERROR] Unexpected error in scrape_daily_metrics: {e}")

def process_daily_metrics(driver,restaurant_ids):
    
    """
    Scrapes customer funnel for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to scrape discount performance for.
        restaurants_names (list): List of restaurant names.
        phone_ (str): Phone number associated with the account.
    """
    
    def retry():
        url = f'https://partner.swiggy.com/business-metrics/overview/restaurant/{restaurant_ids[0]}'
        driver.get(url)        
        refresh(driver)
        handle_container_popups(driver)
        popup_close_xpath = '//*[@id="mCSB_1_container"]/ul/li[5]/ul/div/div/div[1]/img[2]'
        try:
        # Check if the popup close button exists and is interactable
            close_button = driver.find_element(By.XPATH, popup_close_xpath)  # Locate the element
            if close_button.is_displayed() and close_button.is_enabled():  # Check visibility and interactivity
                wait_click(driver, popup_close_xpath)  # Click if interactable
            else:
                print("Popup exists but is not interactable or does not need closing")
        except:
            print(f"error handling popup 1")

    for restaurant_id in restaurant_ids:
        
        retry()

        driver.switch_to.frame("mfe-frame")

        #click on filter button
        Outlet_filter_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/button[2]/span'
        wait_click(driver,Outlet_filter_button_xpath)
        
        # de-select all outlets
        Select_all_checkbox_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[2]/div[4]/div[2]/div[1]/div/span'
        wait_click(driver,Select_all_checkbox_xpath)

        # Wait for the parent scrollable filter container to appear
        scroll_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "styled__FilterOptionsContainer-business-metrics-mfe__sc-as1n8o-5"))
        )

        # Scroll in increments to bring the desired div into view
        container_xpath = f'//div[@class="styled__OptionContainer-business-metrics-mfe__sc-as1n8o-6 cLlKxa" and .//div[contains(text(), "RID: {restaurant_id}")]]'
        outer_div = None
        max_scroll_attempts = 20

        for attempt in range(max_scroll_attempts):
            try:
                outer_div = driver.find_element(By.XPATH, container_xpath)
                if outer_div.is_displayed():
                    break
            except:
                pass

            # Scroll slightly
            driver.execute_script("arguments[0].scrollTop += 100;", scroll_container)
            time.sleep(0.2)

        outer_div.click()

        #Click on Apply button
        Apply_button_xpath = '//*[@id="mfe-root"]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[3]/div/button'
        wait_click(driver,Apply_button_xpath)

        #Fetch historic data from dB
        query = """
            SELECT DISTINCT business_date
            FROM swiggy_daily_metrics
            WHERE restaurant_id = %s
            ORDER BY business_date DESC;
        """
        cur.execute(query, (restaurant_id,))
        records = cur.fetchall()
        
        if not records: 
            printLog("--Record not available. Processing 180 days data.")           

            # Many daily metrics are available with a lag of 48 hours
            for i in range(180,1,-1):
                date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')

                check = select_date_funnel(driver,date)

                if check:
                    scrape_daily_metrics(driver,restaurant_id,date)
                    printLog(f"data scraped for {restaurant_id}")
                else:
                    printLog("--Funnel metrics skipped for date: ", date)

            # Ensure we return to the main content                   
            driver.switch_to.default_content()
                
            printLog(f"data scraped for {restaurant_id}")          

        else:
            printLog("--Record available. Downloading new data...")

            scraped_dates = [str(record[0]) for record in records]
            previous_date = records[0][0]
            print(f"previous_date:({previous_date})")
            current_date = datetime.today().date()
            delta_days = (current_date - previous_date).days

            for i in range(delta_days, 1, -1):
                delta = current_date - timedelta(days=i)
                date = delta.strftime('%Y-%m-%d')

                if date in scraped_dates:
                    printLog("Date is already scraped. Skipping date.")
                    continue

                check = select_date_funnel(driver,date)

                if check:
                    scrape_daily_metrics(driver,restaurant_id,date)
                    printLog(f"data scraped for {restaurant_id}")
                else:
                    printLog("--Funnel metrics skipped for date: ", date)

            # Ensure we return to the main content                   
            driver.switch_to.default_content()

### ************ RESTAURANT TIMINGS ************ ###

def process_restaurant_timings(access_token,restaurant_id):
    """
    Scrapes timings for a given restaurant.

    Args:
        access_token (str): Access token for Swiggy API.
        restaurant_id (str): ID of the restaurant to scrape timings for.
    """

    def convert_timestamp_to_time(timestamp):
        import datetime
        hours = timestamp // 100
        minutes = timestamp % 100
        if minutes >= 60:
            minutes = 59
        time_object = datetime.time(hours, minutes)
        return time_object.strftime("%H:%M")

    # Fetch previous records
    cur.execute(f"SELECT * FROM swiggy_restaurant_timings WHERE restaurant_id = '{restaurant_id}' AND status ='True'")
    previous_records = cur.fetchall()

    headers = {
        "authority": "vhc-composer.swiggy.com",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "access_token": access_token,
        "content-type": "application/json",
        "origin": "https://partner-self-client.swiggy.com",
        "referer": "https://partner-self-client.swiggy.com/",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    payload ={
        "query":"\n  query getSlots($rids: [Int64!]!) {\n    getSlots(input: { restaurantIds: $rids }) {\n      rId\n      days {\n        day\n        slots {\n          open_time\n          close_time\n        }\n      }\n    }\n  }\n",
        "variables":{"rids":[restaurant_id]}
        }

    url = 'https://vhc-composer.swiggy.com/query?query=getSlots'

    response = requests.post(url,headers=headers, json=payload)

    body = response.json()['data']

    try:
        check = body['getSlots'][0]['days']
    except Exception as e:
        printLog(f"No data found for restaurant <{restaurant_id}> timings.")
        printError(e)
        return False
    
    new_data = []

    for slot in body['getSlots'][0]['days']:
        for i,s in enumerate(slot['slots'],1):
            new_slot = {
                'restaurant_id': restaurant_id,
                'created_at': get_current_date_formated(),
                'updated_at': get_current_date_formated(),
                'day_of_the_week': slot['day'],
                'slot_no' : i,
                'start_time': convert_timestamp_to_time(s['open_time']),
                'end_time': convert_timestamp_to_time(s['close_time']),
                'status': True
            }
            new_data.append(new_slot)

    if len(previous_records) == 0:
        printLog("--No record found, scraping data for restaurant.")
                
        query = """INSERT INTO swiggy_restaurant_timings 
            (restaurant_id, created_at, updated_at, day_of_the_week, slot_no, start_time, end_time, status)
            VALUES (%s, %s, %s, %s, %s,%s, %s, %s);
        """

        for slot in new_data:
            execute_query(query,tuple(slot.values()))

        connection.commit()

    else:

        for previous_record in previous_records:
            for new_slot in new_data:
                day_check = previous_record[3] != new_slot['day_of_the_week']
                slot_check = previous_record[4] == new_slot['slot_no']
                if day_check:
                    continue
                else:
                    start_check = previous_record[5] != new_slot['start_time']
                    end_check = previous_record[6] != new_slot['end_time']
                    if (slot_check) and (start_check or end_check):
                        
                        printLog("--Slots changeds, updating records.")
                        # Update previous record
                        execute_query("UPDATE swiggy_restaurant_timings SET updated_at = %s, status = %s WHERE restaurant_id = %s AND day_of_the_week = %s AND slot_no = %s",
                                    (get_current_date_formated(), False, restaurant_id, previous_record[3],previous_record[4]))

                        query = """INSERT INTO swiggy_restaurant_timings 
                                    (restaurant_id, created_at, updated_at, day_of_the_week, slot_no, start_time, end_time, status)
                                    VALUES (%s, %s, %s, %s, %s,%s, %s, %s);
                                """

                        execute_query(query,tuple(new_slot.values()))

        connection.commit()

### ************ ORDERS ************ ###
  
def process_orders_details(access_token,start_date,end_date,restaurant_id):

    def check(response):
        try:
            orders_data = json.loads(response.content)
            orders_data['data'][0]['data']['objects']
            return True
        except:
            return False

    cur.execute(f"SELECT * FROM swiggy_order_metrics WHERE restaurant_id = '{restaurant_id}' ORDER BY updated_at DESC")
    records = cur.fetchall()
    scraped_orders = [str(record[0]) for record in records]

    date_ = get_current_date_formated()

    headers = {
        'authority': 'rms.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'accesstoken': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner-self-client.swiggy.com',
        'referer': 'https://partner-self-client.swiggy.com/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }

    offset = 0
    orders = []

    while True:
        
        url = f'https://rms.swiggy.com/orders/v1/history?limit=500&offset={offset}&ordered_time__gte={start_date}&ordered_time__lte={end_date}&restaurant_id={restaurant_id}'
        response = requests.get(url,headers=headers)

        status_ok = check(response)
        print(start_date)

        if status_ok:
            orders_data = json.loads(response.content)
            chunk = orders_data['data'][0]['data']['objects']
        else:
            time.sleep(1)
            response = requests.get(url,headers=headers)
            orders_data = json.loads(response.content)
            chunk = orders_data['data'][0]['data']['objects']

        if len(chunk) > 0 :
            orders.extend(chunk)
            offset += 500
            time.sleep(5)
        else:
            break

    # previous logic
    # status_ok = False

    # for _ in range(3):
    #     status_ok = check(response)

    #     if status_ok:
    #         break
    #     else:
    #         url = f'https://rms.swiggy.com/orders/v1/history?limit=1000&offset=0&ordered_time__gte={start_date}&ordered_time__lte={end_date}&restaurant_id={restaurant_id}'
    #         response = requests.get(url,headers=headers)

    # if not status_ok:
    #     printLog("--No data available for this restaurant.")
    #     return
    
    # orders_data = json.loads(response.content)
    # orders = orders_data['data'][0]['data']['objects']

    printLog("--Total orders to scrape: {}".format(len(orders)))
    orders_list = []
    items_list = []

    for order_chunk in orders:

        if str(order_chunk['order_id']) in scraped_orders:
            continue
        
        order_accepted_at = order_chunk['status'].get('placed_time', None)
        order_placed_at = order_chunk['status'].get('with_partner_time', None)
        order_placed_at1 = order_chunk['status'].get('ordered_time', None)
        dct = {
            'order_id': order_chunk.get('order_id', 0),
            'restaurant_id': restaurant_id,
            'created_at': date_,
            'updated_at': date_,
            'order_placed_at': order_placed_at1 if (order_placed_at in [None, 'Invalid date', '']) else order_placed_at,
            'order_accepted_at': None if order_accepted_at == 'Invalid date' else order_accepted_at,
            'order_delivered_at': order_chunk['status'].get('delivered_time', None),
            'kpt': order_chunk.get('prepTime', 0),
            'order_status': order_chunk['status'].get('order_status', 0),
            'bill_amount': order_chunk.get('bill', 0),
            'service_charge': order_chunk.get('serviceCharge', 0),
            'delivery_charges': order_chunk['cart']['charges'].get('delivery_charge', 0),
            'packaging_charges': order_chunk['cart']['charges'].get('packing_charge', 0),
            'taxes': order_chunk.get('gst', 0),
            'retaurant_trade_discount': order_chunk.get('restaurant_trade_discount', 0),
            'restaurant_coupon_discount_share': order_chunk.get('restaurant_offers_discount', 0),
            'cancelled_reason': order_chunk['status'].get('cancel_reason', 0),
            'food_prepared': order_chunk['status'].get('is_food_prepared', 0),
            'edited_status': order_chunk['status'].get('edited_status', 0),
            'food_ready_signal': order_chunk.get('mfrAccuracy', {}).get('message', 0)
        }

        orders_list.append(dct)

        items = order_chunk['cart']['items']
        
        for item in items:
            order_item = {
                'order_id' : order_chunk['order_id'],
                'restaurant_id' : restaurant_id,
                'created_at': date_,
                'updated_at': date_,
                'item_id': item.get('item_id'),
                'item_name': item.get('name'),
                'quantity': item.get('quantity'),
                'packaging_charges': item.get('packing_charges'),
                'subtotal': item.get('sub_total'),
                'total': item.get('total'),
                'variants': json.dumps(item.get('variants')),
                'addons': json.dumps(item.get('addons'))
            }
            items_list.append(order_item)
        
    base_url = 'https://rms.swiggy.com/orders/v1/history?'
    filters_ = ['wrong_items', 'unsafe_packaging', 'missing_items',
                'quality_issues', 'packaging_issues','quantity_issues']

    filter_lists = {filter_: [] for filter_ in filters_}

    for filter_ in filters_:
        params = {
            'filterReq': filter_,
            'limit': 100,
            'offset': 0,
            'ordered_time__gte': start_date,
            'ordered_time__lte': end_date,
            'restaurant_id': restaurant_id
        }

        r = requests.get(base_url, headers=headers, params=params)
        response = r.json()['data'][0]['data']['objects']
        orders = []

        for order in response:

            try:
                orders.append(str(order['meta_info']['order_id']))
            except:
                try:
                    orders.append(str(order['order_id']))
                except Exception as e:
                    printError(e,True)

        filter_lists[filter_] = orders

    updated_orders = []

    for order in orders_list:
        updated_order = order.copy()
        for filter_, orders in filter_lists.items():
            
            if order['order_id'] in orders:
                updated_order[filter_] = 1
            else:
                updated_order[filter_] = 0
        updated_orders.append(updated_order)
    
    if len(updated_orders) > 0:
        for order in updated_orders:
            query = """
                INSERT INTO swiggy_order_metrics (
                order_id,
                restaurant_id,
                created_at,
                updated_at,
                order_placed_at,
                order_accepted_at,
                order_delivered_at,
                kpt,
                order_status,
                bill_amount,
                service_charge,
                delivery_charges,
                packaging_charges,
                taxes,
                restaurant_trade_discount,
                restaurant_coupon_discount_share,
                cancelled_reason,
                food_prepared,
                edited_status,
                food_ready_signal,
                wrong_items,
                unsafe_packaging,
                missing_items,
                quality_issues,
                packaging_issues,
                quantity_issues
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """

            values= tuple(order.values())
            execute_query(query,values)
            connection.commit()
        
        for item in items_list:

            sql_query = """
                INSERT INTO swiggy_order_item_metrics (
                    order_id,
                    restaurant_id,
                    created_at,
                    updated_at,
                    item_id,
                    item_name,
                    quantity,
                    packaging_charges,
                    subtotal,
                    total,
                    variants,
                    addons
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                );
                """
            values = tuple(item.values())
            execute_query(sql_query,values)
            connection.commit()
    
        return True
    
    else:
        printLog("--No data available to scrape.")
        return False
  
def process_orders(driver,restaurant_id):
    from datetime import datetime
    
    cur.execute(f"SELECT date FROM swiggy_orders_month_track WHERE restaurant_id = '{restaurant_id}'")
    record = cur.fetchone()

    today = datetime.today()

    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    if record is None:
        printLog("--Record not available. Scraping orders data for 6 months.")

        #--------------------------------------#
        query = """
            INSERT INTO swiggy_orders_month_track(
            restaurant_id,
            date            
            ) 
            VALUES(
                %s, %s
                );
            """
        values = (restaurant_id, today.strftime("%Y-%m-%d"))
        execute_query(query,values)
        connection.commit()

        #--------------------------------------#
        
        for days in range(90,-1,-30):

            delta = today - timedelta(days=days)
            month_ = delta.month
            year_ = delta.year

            months = {
            1: 'January',
            2: 'February',
            3: 'March',
            4: 'April',
            5: 'May',
            6: 'June',
            7: 'July',
            8: 'August',
            9: 'September',
            10: 'October',
            11: 'November',
            12: 'December'
            }

            printLog("--Scraping data for {}-{}".format(months[month_],year_))

            current_month,start_date,end_date = get_month_dates(year_,month_)

            process_orders_details(access_token,start_date,end_date,restaurant_id)
            
            query = """
                UPDATE swiggy_orders_month_track
                SET 
                    date = %s
                WHERE 
                    restaurant_id = %s
                """
            values = (end_date, restaurant_id)
            execute_query(query,values)
            connection.commit()

    else:
        printLog("--Record available.")
        start_date = record[0].strftime("%Y-%m-%d")        
        end_date = today.strftime("%Y-%m-%d")
        process_orders_details(access_token,start_date , end_date,restaurant_id)
        # Update date in tracking table after processing orders
        query = """
            UPDATE swiggy_orders_month_track
            SET 
                date = %s
            WHERE 
                restaurant_id = %s
            """
        values = (end_date, restaurant_id)
        execute_query(query, values)
        connection.commit()
  
### ************ REVIEWS ************ ###
def process_reviews(access_token, restaurant_id, start_date, end_date):
    data = []
    orders = []
    cur.execute(f"SELECT * FROM swiggy_restaurant_reviews WHERE restaurant_id = '{restaurant_id}'")
    records = cur.fetchall()
    scraped_orders = [str(record[-3]) for record in records]

    url = 'https://vhc-composer.swiggy.com/query?query=getRestaurantRatingsAndReviews'

    headers = {
        'authority': 'vhc-composer.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'access_token': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner-self-client.swiggy.com',
        'referer': 'https://partner-self-client.swiggy.com/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }

    start_epoch = int(start_date.timestamp() * 1000)
    end_epoch = int(end_date.timestamp() * 1000)

    pagekeys = ''
    while True:
        payload = {
            "query": f"query {{\n    getRestaurantRatingsAndReviews(\n      requestInput:{{\n      restaurantIds: [\"{restaurant_id}\"]\n      startDateEpoch: {start_epoch}\n      endDateEpoch: {end_epoch}\n      pageSize: 5\n      pageKey: \"{pagekeys}\"\n    }}\n    ) {{\n      ordersInfo {{\n          orderID\n          restaurantID\n          rating\n          ratingTimeEpoch\n          orderTimeEpoch\n          ratingState\n          customerReview\n          feedbackExpiryTimeEpoch\n          itemsInfo {{\n                  id\n                  name\n                  quantity\n                  marking\n          }}\n          customerInfo {{\n                  id\n                  name\n                  type\n          }}\n      }}\n      paginationContext {{\n          pageSize\n          pageKey\n      }}\n    }}\n  }}",
            "variables": {}
        }

        response = requests.post(url, headers=headers, json=payload)

        body = response.json()

        try:
            pagekeys = body['data']['getRestaurantRatingsAndReviews']['paginationContext']['pageKey']
            orders_info = body['data']['getRestaurantRatingsAndReviews']['ordersInfo']
            _ = [order_info['orderID'] for order_info in orders_info]
            orders.extend(_)
        except:
            break

        if pagekeys == '':
            break

    for order_id in orders:
        if str(order_id) in scraped_orders:
            continue
        review_data = get_review_order_details(access_token, restaurant_id, order_id)
        
        # Only append non-None values
        if review_data is not None:
            data.append(review_data)

    if len(data) > 0:
        query = """INSERT INTO swiggy_restaurant_reviews (
            restaurant_id, 
            created_at, 
            updated_at,
            review_date, 
            customer_name, 
            customer_type, 
            area_name, 
            orders, 
            all_orders_value, 
            complaints, 
            order_id, 
            rating,
            review_text
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );"""

        for record in data:
            execute_query(query, tuple(record.values()))
        
        connection.commit()
    
    else:
        printLog("--No data available for this restaurant.")
  
def get_review_order_details(access_token,restaurant_id,order_id):
    url = 'https://vhc-composer.swiggy.com/query?query=getOrderRatingDetails'

    headers = {
        'authority': 'vhc-composer.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'access_token': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner-self-client.swiggy.com',
        'referer': 'https://partner-self-client.swiggy.com/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    
    data = {
        "query": f"query{{\n  getOrderRatingDetails(orderId: \"{order_id}\", restaurantId: \"{restaurant_id}\") {{\n    orderInfo{{\n      orderID\n      restaurantID\n      rating\n      ratingTimeEpoch\n      orderTimeEpoch\n      ratingState\n      customerReview\n      ratingExpiryTimeEpoch\n      customerInfo{{\n        id\n        name\n        type\n        orderCount\n        ordersTotalValue\n        complaintsCount\n      }}\n      itemsInfo {{\n        id\n        name\n        marking\n        quantity\n        subTotal\n        category\n        subCategory\n        isVeg\n      }}\n    }}\n    isCustomerCallingConsented\n    billDetails{{\n      itemTotal\n      packagingCharges\n      rxPayableGst\n      swiggyPayableGst\n      discount\n      billTotal\n      couponCode\n    }}\n    discountRecommendations{{\n      discountValue\n      isRecommended\n    }}\n    discountMinimumThreshold\n    discountMinimumOrderValue\n    feedbackResolutionDetails {{\n      offerId\n      couponCode\n      validityStartTimeEpoch\n      validityEndTimeEpoch\n      vendorResponse\n      resolutionAmount\n    }}\n  }}\n}}\n",
        "variables": {}
    }


    response = requests.post(url, headers=headers, json=data)
    dct = response.json()

     # Check if the required data exists in the response
    if dct.get('data') and dct['data'].get('getOrderRatingDetails') and dct['data']['getOrderRatingDetails'].get('orderInfo'):
        orderInfo = dct['data']['getOrderRatingDetails']['orderInfo']
        date_ = get_current_date_formated()

        func = lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d')

        data = {
            'restaurant_id': orderInfo['restaurantID'],
            'created_at': date_,
            'updated_at': date_,
            'review_date': func(orderInfo['ratingTimeEpoch']),
            'customer_name': orderInfo['customerInfo']['name'],
            'customer_type': 'New' if orderInfo['customerInfo']['type'][0] == 'N' else 'Repeated',
            'area_name': 'Not available',
            'orders': orderInfo['customerInfo']['orderCount'],
            'all_orders_value': orderInfo['customerInfo']['ordersTotalValue'],
            'complaints': orderInfo['customerInfo']['complaintsCount'],
            'order_id': orderInfo['orderID'],
            'rating': orderInfo['rating'],
            'review_text': orderInfo['customerReview']
        }

        return data
    else:
        printLog(f"Skipping order {order_id}: Incomplete data received.")
        return None
     
def process_restaurant_reviews(driver,restaurant_id):
    """
    Scrapes reviews for a given restaurant.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_id (str): ID of the restaurant to scrape reviews for.
    """
    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    end_date = datetime.today() 
    start_date = end_date - timedelta(days=60)

    print(f"Scraping reviews from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    process_reviews(access_token,restaurant_id,start_date,end_date)

### ************ RESTAURANT & DAILY METRICS ************ ###
def process_restaurant(access_token,restaurant_id,scratch):
    
    def scrape(start_date,end_date):
        nonlocal access_token
        nonlocal restaurant_id
        nonlocal data
        nonlocal df

        info_dict = {
            'operations_date' : start_date,
            'restaurant_id' : restaurant_id,
            'created_at' : get_current_date_formated(),
            'updated_at' : get_current_date_formated(),
            "food_ready_not_pressed" : 0,
            "food_ready_correct" : 0,
            "food_ready_pressed_early" : 0,
            "wrong_item": 0.0,
            "missing_item": 0.0,
            "quality_quantity_issues": 0.0,
            "package_spillage": 0.0,
            "confirmation_time": 0.0,
            "accept_orders": 0.0,
            "edit_orders": 0.0,
            "cancel_orders": 0.0,
            "orders_confirmed_in_3_min": 0,
            "cancel_item_not_available": 0,
            "cancel_restaurant_closed": 0,
            "cancel_not_accepting_orders": 0
            }

        headers = {
        'authority': 'rms.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'accesstoken': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner-self-client.swiggy.com',
        'referer': 'https://partner-self-client.swiggy.com/',
        'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }
        # MFR
        url = f'https://rms.swiggy.com/reporting/mfr/metrics?endDate={end_date}%2023%3A59%3A59&restaurantId={restaurant_id}&startDate={start_date}%2000%3A00%3A00'
        response = requests.get(url, headers=headers)
        mfr = response.json()['data']

        items_list= {'food_ready_not_pressed': 'mfr_not_pressed', 
                    'food_ready_correct' : 'mfr_pressed_correctly',
                    'food_ready_pressed_early' : 'mfr_pressed_early'}

        for info_key, mfr_key in items_list.items():
            info_dict[info_key] = mfr[mfr_key]

        # Metrics
        summary_url = f'https://rms.swiggy.com/insights/v1/measures/summary?endDate={end_date}%2023%3A59%3A59&restaurantId={restaurant_id}&startDate={start_date}%2000%3A00%3A00&type=igcc_preptime'
        response = requests.get(summary_url, headers=headers)

        response_dict = response.json()
        response_dict = response_dict['data']['igcc_preptime']

        dct_keys = {'wrong_item': 'wrong_item_complaints_rate', 'missing_item': 'missing_item_complaints_rate', 'quality_quantity_issues': 'quality_quantiy_issues_rate', 'package_spillage': 'package_spillage_issues_rate'}

        for key,response_key in dct_keys.items():
            info_dict[key] = response_dict[response_key]


        # Metrics
        body = {"group_by":"daily","end_date":f"{end_date} 23:59:59","start_date":f"{start_date} 00:00:00","rest_id":[f"{restaurant_id}"]}
        url='https://rms.swiggy.com/insights/v2/restaurants/ops-metrics'
        response = requests.post(url, headers=headers, json=body)
        metric = response.json()['data'].pop()

        mapper = {'confirmation_time': 'confirmation_time',
        'accept_rate': 'accept_orders',
        'edit_rate': 'edit_orders',
        'cancel_rate': 'cancel_orders'}

        for metric_key,info_key in mapper.items():
            info_dict[info_key] = metric[metric_key]

        if start_date in df.index:
            info_dict['orders_confirmed_in_3_min'] = int(df.loc[start_date]['ordersAcceptedWoDelay'])
            info_dict['cancel_item_not_available'] = int(df.loc[start_date]['cancelItemNA'])
            info_dict['cancel_restaurant_closed'] = int(df.loc[start_date]['cancelRestClosed'])
            info_dict['cancel_not_accepting_orders'] = int(df.loc[start_date]['cancelRestNotAccepting']) 
            
        data.append(info_dict)

    data = []

    start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')

    cur.execute(f"SELECT * FROM swiggy_operations_metrics WHERE restaurant_id = '{restaurant_id}'")
    records = cur.fetchall()
    scraped_dates = [record[1].strftime('%Y-%m-%d') for record in records]

    payload = {
        "query": f"""
        {{
            getOpsMetrics(groupBy: "daily", restId: {restaurant_id}, startDate: "{start_date}", endDate: "{end_date}") {{
                __typename
                ... on OpsMetricsDaily {{
                    overview {{
                        cancelItemNA
                        cancelRestClosed
                        cancelRestNotAccepting
                        ordersEdited
                        ordersDelivered
                        ordersCancelled
                    }}
                    data {{
                        cancelItemNA
                        cancelItemNARate
                        cancelRestClosed
                        cancelRestNotAccepting
                        ordersEdited
                        ordersCancelled
                        ordersDelivered
                        ordersAcceptedWoDelay
                        totalOrders
                        editedOrdersRate
                        orderAcceptanceRate
                        ordersCancellationRate
                        day
                    }}
                }}
            }}
        }}
        """,
        "variables": {}
    }

    headers = {
        "authority": "vhc-composer.swiggy.com",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "access_token": access_token,
        "content-type": "application/json",
        "origin": "https://partner.swiggy.com",
        "referer": "https://partner.swiggy.com/",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    success_transaction = False
    
    for _ in range(3):

        url = 'https://vhc-composer.swiggy.com/query?=undefined'

        r = requests.post(url, headers=headers,json=payload)
        response = r.json()

        try:
            ops_list = response['data']['getOpsMetrics']['data']
            success_transaction = True
            break
        except:
            time.sleep(2)
            pass
            # printError(e)
    
    if not success_transaction:
        printLog("--No data available for the duration: {}-{}".format(start_date,end_date))
        return
    
    elif len(ops_list) == 0:
        printLog("--No data available for the duration: {}-{}".format(start_date,end_date))
        return
    
    df = pd.DataFrame(ops_list)
    df.set_index('day',inplace=True)

    if scratch:
        for i in range(0,30,1):
            date_ = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
            if date_ in scraped_dates:
                continue
            else:
                try:
                    scrape(date_,date_)
                except Exception as e:
                    printError(e)
                    try:
                        scrape(date_,date_)
                    except Exception as e:
                        printError(e,True)
                        continue
    
    else:
        date_ = datetime.today().strftime('%Y-%m-%d')
        cur.execute(f"select operations_date from swiggy_operations_metrics where restaurant_id = '{restaurant_id}' order by operations_date desc")
        
        previous_date = cur.fetchone()[0]
        delta = datetime.today().date() - previous_date
        days_passed = delta.days
        
        if days_passed > 1:
            for i in range(0,days_passed,1):
                date_ = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
                if date_ in scraped_dates:
                    continue
                else:
                    try:
                        scrape(date_,date_)
                    except Exception as e:
                        printError(e)

        else:
            if date_ in scraped_dates:
                printLog("--Already updated")
                return
            else:
                scrape(date_,date_)
        
    if len(data) > 1:
        query = """
            INSERT INTO swiggy_operations_metrics (
                operations_date,
                restaurant_id,
                created_at,
                updated_at,
                food_ready_not_pressed,
                food_ready_correct,
                food_ready_pressed_early,
                wrong_item,
                missing_item,
                quality_quantity_issues,
                package_spillage,
                confirmation_time,
                accept_orders,
                edit_orders,
                cancel_orders,
                orders_confirmed_in_3_min,
                cancel_item_not_available,
                cancel_restaurant_closed,
                cancel_not_accepting_orders
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
            """

        for record in data:
            execute_query(query,tuple(record.values()))
        connection.commit()
        printLog("--Data scraped successfully.")
    else:
        printLog("--No record found for this restaurant.")

def process_operations_metrics(driver,restaurant_id):
    """
    Scrapes daily metrics for a given restaurant.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_id (str): ID of the restaurant to scrape metrics for.
    """
    cur.execute(f"SELECT * FROM swiggy_operations_metrics WHERE restaurant_id = '{restaurant_id}'")
    record = cur.fetchone()

    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    if record is None:
        printLog("--No record found for this restaurant. Scraping complete month.")
        process_restaurant(access_token,restaurant_id,True)
    else:
        printLog("--Record found for this restaurant. Scraping Today's data.")
        process_restaurant(access_token,restaurant_id,False)

def process_restaurant_metrics(access_token, restaurant_ids, restaurants_names):
    """
    Scrapes metrics for all outlets.

    Args:
        access_token (str): Access token for Swiggy API.
        restaurant_ids (list): List of restaurant IDs.
        restaurants_names (list): List of restaurant names.
    """
    cur.execute("SELECT * FROM swiggy_restaurant_metrics")
    records = cur.fetchall()
    scraped = [str(record[0]) for record in records]

    headers = {
        "authority": "vhc-composer.swiggy.com",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "access_token": access_token,
        "content-type": "application/json",
        "origin": "https://partner-self-client.swiggy.com",
        "referer": "https://partner-self-client.swiggy.com/",
        "sec-ch-ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    formatted_ids = [f'"{id}"' for id in restaurant_ids]
    restaurant_list = ','.join(formatted_ids)

    payload = {
        "query": "query { restaurantRatings(input: {restaurantIds: [%s],  period: \"CURRENT_WEEK\"}) { ratings { restaurantId globalRating globalRatingCount rating ratingCount ratingTrend } missingRatings { restaurantId globalRating globalRatingCount } } }" % restaurant_list,
        "variables": {}
    }
    
    url = 'https://vhc-composer.swiggy.com/query?query=restaurantRatings'

    try:
        response = requests.post(url, headers=headers, json=payload)
        rating_body = response.json()['data']
        ratings = rating_body['restaurantRatings']['ratings']
        ratings = {str(rest['restaurantId']): [rest['globalRating'], rest['globalRatingCount']] for rest in ratings}

    except Exception as e:
        printLog("No data found against this account.")
        printError(e)
        return

    headers = {
        'authority': 'rms.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'accesstoken': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner.swiggy.com',
        'referer': 'https://partner.swiggy.com/',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    for i in range(len(restaurant_ids)):
        restaurant_id = restaurant_ids[i]

        if restaurant_id in scraped:
            continue
        
        city = restaurants_names[i].split(',')[-1].strip()
        area = restaurants_names[i].split(',')[-2].strip()

        url = f'https://rms.swiggy.com/profile/v1/restaurantInfo/?rest_id={restaurant_id}'
        response = requests.get(url, headers=headers)
        body = response.json()
        restaurantDetails = body['restaurantDetails']

        url = f'https://rms.swiggy.com/restaurant-info/v1/fssai?restaurantId={restaurant_id}'
        response = requests.get(url, headers=headers)
        body = response.json()

        avg_rating, lifetime_votes = ratings.get(restaurant_id, [0, 0])

        previous_record = cur.execute("SELECT avg_rating, lifetime_votes FROM swiggy_restaurant_metrics WHERE restaurant_id = %s ORDER BY created_at DESC LIMIT 1", (restaurant_id,))
        previous_record = cur.fetchone()

        if previous_record:
            previous_avg_rating, previous_lifetime_votes = previous_record
            if avg_rating != previous_avg_rating or lifetime_votes != previous_lifetime_votes:
                printLog("--Restaurant has update. Updating record.")
                # Insert into swiggy_restaurant_metrics_logs
                execute_query("INSERT INTO swiggy_restaurant_metrics_logs (restaurant_id, created_at, updated_at, on_swiggy_since, area_name, city, fssai, avg_rating, lifetime_votes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (restaurant_id, get_current_date_formated(), get_current_date_formated(), restaurantDetails['onboardingDate'], area, city, body['fssaiDetails']['fssai_licence_number'], avg_rating, lifetime_votes))

                connection.commit()

            execute_query("UPDATE swiggy_restaurant_metrics SET updated_at = %s, on_swiggy_since = %s, area_name = %s, city = %s, fssai = %s, avg_rating = %s, lifetime_votes = %s WHERE restaurant_id = %s",
                        (get_current_date_formated(), restaurantDetails['onboardingDate'], area, city, body['fssaiDetails']['fssai_licence_number'], avg_rating, lifetime_votes, restaurant_id))
            connection.commit()
        
        else:
            printLog("--No record found. Adding new record.")
            # Insert or update into swiggy_restaurant_metrics
            query = """INSERT INTO swiggy_restaurant_metrics (restaurant_id, created_at, updated_at, on_swiggy_since, area_name, city, fssai, avg_rating, lifetime_votes) 
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                          """
            values = (restaurant_id, get_current_date_formated(), get_current_date_formated(), restaurantDetails['onboardingDate'], area, city, body['fssaiDetails']['fssai_licence_number'], avg_rating, lifetime_votes)
            execute_query(query,values)

            connection.commit()

### ************ FINANCE REPORT ************ ###
def download_report_from_url(download_url):
    response = requests.get(download_url)

    if response.status_code == 200:
        # Step 2: Unzip the file
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            # List all files in the ZIP archive
            file_list = thezip.namelist()
            print("Files in the ZIP archive:", file_list)
            csv_files = [file for file in file_list if file.endswith('.csv')]
            file_name = csv_files[0]

            # Initialize an empty list to store DataFrames
            df_list = []

            # Step 3: Read the specific file into a DataFrame
            with thezip.open(file_name) as thefile:
                df = pd.read_csv(thefile)
                printLog("Successfully read the CSV file into a DataFrame.")
        
                return df
    else:
        raise ValueError(f"Failed to download the file: status code {response.status_code}")

def download_report(driver, restaurant_ids):    
    """
    Initiates the download of financial reports for given restaurant IDs.

    Args:
        driver (webdriver): Selenium WebDriver instance.
        restaurant_ids (list): List of restaurant IDs to download reports for.
    """

    for cookie in driver.get_cookies():
        if cookie['name'] == 'Swiggy_Session-alpha':
            access_token = cookie['value']
            break

    query = """
        SELECT order_date
        FROM swiggy_order_finance_metrics
        WHERE restaurant_id IN %(restaurant_list)s ORDER BY order_date DESC;
        """
    cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})
    record = cur.fetchone()

    if record is None:
        printLog("--No data found for this account. Downloading past data...")
        start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')

    else:
        printLog("--Record available. Downloading new data...")
        start_date = record[0].strftime("%Y-%m-%d")
        end_date = datetime.today().strftime('%Y-%m-%d')

    payload = {
        "query": "query DownloadFinanceReport($rids: [Int64!]!, $type: String!, $fromDate: String!, $toDate: String!, $email: String!) {\n  downloadFinanceReport(\n    input: {rids: $rids, fromDate: $fromDate, toDate: $toDate, email: $email, type: $type}\n  ) {\n    statusCode\n    message\n    __typename\n  }\n}\n",
        "operationName": "DownloadFinanceReport",
        "variables": {
            "rids": restaurant_ids,
            "type": "CONSOLIDATED_ANNEXURE",
            "fromDate": start_date,
            "toDate": end_date,
            "email": "jampacked.ops@gmail.com"
        }
    }

    headers = {
        'authority': 'vhc-composer.swiggy.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'access_token': access_token,
        'content-type': 'application/json',
        'origin': 'https://partner.swiggy.com',
        'referer': 'https://partner.swiggy.com/',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    url = 'https://vhc-composer.swiggy.com/query?query=DownloadFinanceReport'
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        time.sleep(100)
        try:
            process_report()
        except Exception as e:
            printError(e,True)
    else:
        printLog("--Unable to fetch data, check dates or code.")

def process_report():
    import email
    import pandas as pd
    import imaplib
    import email
    
    def check_for_email():
        mail_server = "imap.gmail.com"
        mail_port = 993
        mail_user = "jampacked.ops@gmail.com"
        mail_password = "ybdn reny lytt sptr"

        # Connect to the Gmail IMAP server
        mail = imaplib.IMAP4_SSL(mail_server, mail_port)

        # Login to your Gmail account
        mail.login(mail_user, mail_password)

        mail.select("inbox")

        # Search for unseen emails from the specific sender
        status, messages = mail.search(None, '(UNSEEN FROM "payments@swiggy.in")')
        
        email_ids = messages[0].split()
        if email_ids:
            email_id = email_ids[-1]

            status, msg_data = mail.fetch(email_id, "(RFC822)")
            raw_email = msg_data[0][1]

            email_message = email.message_from_bytes(raw_email)

            email_body = ""

            if email_message.is_multipart():
                for part in email_message.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))

                    if "attachment" not in content_disposition:
                        # Decode the payload if it exists
                        payload = part.get_payload(decode=True)
                        if payload is not None:
                            charset = part.get_content_charset() or "utf-8"
                            email_body += payload.decode(charset, errors="ignore")
            else:
                payload = email_message.get_payload(decode=True)
                if payload is not None:
                    charset = email_message.get_content_charset() or "utf-8"
                    email_body = payload.decode(charset, errors="ignore")

            soup = BeautifulSoup(email_body, features='lxml')
            return soup
        else:
            return None

    
    start_time = time.time()
    while (time.time() - start_time) < 600:  # 600 seconds = 10 minutes
        soup = check_for_email()
        if soup:
            printLog("Email received and processed.")
            break
        else:
            printLog("No email found. Checking again in 2 minutes...")
            time.sleep(120)  # Wait for 2 minutes before checking again
    else:
        printLog("No email received within 10 minutes.")
        return

    try:
        download_url = soup.find('a')['href']
        # df = pd.read_csv(download_url)
        df = download_report_from_url(download_url)
        print(f"--Report downloaded. Exctracting content...")
        df.fillna(0,inplace=True)
        print(f"--{len(df)} records available.")

        cleaned_columns = ['RID',
        'Order Date',
        'Order No',
        'Order Status',
        'Order Category',
        'Cancelled By?',
        "Item's total",
        'Packing & Service charges',
        'Merchant Discount',
        'Net Bill Value (without taxes)',
        'GST liability of  Merchant',
        'Customer payable (Net bill value after taxes & discount)',
        'Swiggy Platform Service Fee Chargeable On',
        'Swiggy Platform Service Fee %',
        'Discount on Swiggy Platform Service Fee',
        'Total Long Distance Subscription Fees',
        'Total Discount on Long Distance Subscription Fees',
        'Total Effective Long Distance Subscription Fees',
        'Collection Charges',
        'Access Charges',
        'Merchant Cancellation Charges',
        'Call Center Service Fees',
        'Total Swiggy Service fee (without taxes)',
        'Delivery fee (sponsored by merchant)',
        'Taxes on Swiggy fee',
        'Total Swiggy fee (including taxes)',
        'Cash Prepayment to Merchant',
        'Merchant Share of Cancelled Orders',
        'GST Deduction U/S 9(5)',
        'Refund for Disputed Order',
        'Disputed Order Remarks',
        'Total of Order Level Adjustments',
        'Net Payable Amount (before TCS deduction)',
        'Net Payable Amount (after TCS and TDS deduction)',
        'MFR Pressed?',
        'Cancellation Policy Applied',
        'Coupon Code Sourced',
        'Discount Campaign ID',
        'Is_replicated',
        'Base order ID',
        'MRP Items',
        'Order Payment Type',
        'Cancellation time',
        'Pick Up Status',
        'Coupon code applied by customer',
        'Nodal UTR',
        'Current UTR',
        'Long Distance Applicable',
        'Last Mile Distance'
    ]

        for col in cleaned_columns:
            for df_col in df.columns:
                if col in df_col:
                    df.rename(columns = {df_col : col},inplace=True)
                    found = True
                    break
            
            if not found:
                print(False)
            
    except Exception as e:
        
        e_type = type(e).__name__
        if e_type == 'EmptyDataError':
            printError(e,False)
        printLog("--No data available for this date.")
        return False

    data = []

    for i in range(len(df)):
        record = df.iloc[i]
        
        # Only process records where 'Order Category' is 'Regular'
        if record['Order Category'].lower() == 'regular':
            dct = {
                "order_id": str(record['Order No']),
                "restaurant_id": str(record['RID']),
                "order_date": record['Order Date'],
                "created_at": get_current_date_formated(),
                "updated_at": get_current_date_formated(),
                "order_status": record['Order Status'],
                "order_category": record['Order Category'],
                "cancelled_by": record["Cancelled By?"],
                "item_total": record["Item's total"],
                "packing_and_service_charges": record["Packing & Service charges"],
                "merchant_discount": record["Merchant Discount"],
                "net_amount_without_taxes": record["Net Bill Value (without taxes)"],
                "gst_liability_of_merchant": record["GST liability of  Merchant"],
                "customer_payable_net_after_tax_and_discount": record["Customer payable (Net bill value after taxes & discount)"],
                "platform_fee_chargeable_on": record["Swiggy Platform Service Fee Chargeable On"],
                "platform_fee_percent": record["Swiggy Platform Service Fee %"],
                "platform_fee": record["Swiggy Platform Service Fee G"],
                "discount_on_platform_fee": record["Discount on Swiggy Platform Service Fee"],
                "collection_charges": record["Collection Charges"],
                "access_charges": record["Access Charges"],
                "merchant_cancellation_charges": record["Merchant Cancellation Charges"],
                "call_centre_service_fee": record["Call Center Service Fees"],
                "swiggy_service_fee_without_tax": record["Total Swiggy Service fee (without taxes)"],
                "tax_on_swiggy_service_fee": record["Taxes on Swiggy fee"],
                "merchant_share_of_cancelled_orders": record["Merchant Share of Cancelled Orders"],
                "refund_on_disputed_orders": record["Refund for Disputed Order"],
                "disputed_orders_remarks": record["Disputed Order Remarks"],
                "order_level_adjustments": record["Total of Order Level Adjustments"],
                "net_payable_amount": record["Net Payable Amount (before TCS deduction)"],
                "net_payable_amount_after_tcs_tds": record["Net Payable Amount (after TCS and TDS deduction)"],
                "mfr_pressed": record["MFR Pressed?"],
                "cancellation_policy_applied": record["Cancellation Policy Applied"],
                "coupon_code_sourced": record["Coupon Code Sourced"],
                "discount_campaign_id": record["Discount Campaign ID"],
                "is_replicated": record["Is_replicated"],
                "base_order_id": record["Base order ID"],
                "mrp_items": record["MRP Items"],
                "order_payment_type": record["Order Payment Type"],
                "cancellation_time": record["Cancellation time"],
                "pick_up_status": record["Pick Up Status"],
                "coupon_code_applied_by_customer": record["Coupon code applied by customer"],
                "long_distance_applicable": record["Long Distance Applicable"],
                "last_mile_distance": record["Last Mile Distance"],
                "long_distance_subscription_fees": record["Total Long Distance Subscription Fees"],
                "discount_on_long_distance_subscription_fees": record["Total Discount on Long Distance Subscription Fees"],
                "net_long_distance_subscription_fees": record["Total Effective Long Distance Subscription Fees"]
            }

            data.append(dct)

    if len(data) > 0:
        
        query = """INSERT INTO swiggy_order_finance_metrics (
                order_id,
                restaurant_id,
                order_date,
                created_at,
                updated_at,
                order_status,
                order_category,
                cancelled_by,
                item_total,
                packing_and_service_charges,
                merchant_discount,
                net_amount_without_taxes,
                gst_liability_of_merchant,
                customer_payable_net_after_tax_and_discount,
                platform_fee_chargeable_on,
                platform_fee_percent,
                platform_fee,
                discount_on_platform_fee,
                collection_charges,
                access_charges,
                merchant_cancellation_charges,
                call_centre_service_fee,
                swiggy_service_fee_without_tax,
                tax_on_swiggy_service_fee,
                merchant_share_of_cancelled_orders,
                refund_on_disputed_orders,
                disputed_orders_remarks,
                order_level_adjustments,
                net_payable_amount,
                net_payable_amount_after_tcs_tds,
                mfr_pressed,
                cancellation_policy_applied,
                coupon_code_sourced,
                discount_campaign_id,
                is_replicated,
                base_order_id,
                mrp_items,
                order_payment_type,
                cancellation_time,
                pick_up_status,
                coupon_code_applied_by_customer,
                long_distance_applicable,
                last_mile_distance,
                long_distance_subscription_fees,
                discount_on_long_distance_subscription_fees,
                net_long_distance_subscription_fees
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id,restaurant_id)
            DO NOTHING;
            """

        for record in data:
            record = {key : str(value).replace('%','') for key,value in record.items()}
            execute_query(query,tuple(record.values()))

        connection.commit()

    else:
        printLog("--No data to save.")
    
### ************ MAIN CODE HANDLING ALL STUFF ************ ###
def process_account(phone_,pass_):
    """
    Handles the process for each account, including logging in and calling subsequent scraping functions.

    Args:
        phone_ (str): Phone number of the account.
        pass_ (str): Password of the account.
    """

    cookies = get_cookies(phone_)
    driver = init_driver(cookies,False)
    
    driver.refresh()
    driver.maximize_window()

    time.sleep(4)

    printLog("\n#------ Scraping account: {} ------#".format(phone_))

    if driver.current_url == 'https://partner.swiggy.com/login/':

        # This code for generating cookies does not save the cookies adequately. Use create_session.py to generate cookies.
        printLog("--Cookies expired. Creating new session.")
        
        phone_placeholder = wait_visible(driver, '//input[@id="Enter Restaurant ID / Mobile number"]')
        phone_placeholder.send_keys(phone_)

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
    # https://partner.swiggy.com/login?next=orders
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
        printLog("--Something went wrong. Please check code.")
        driver.quit()
        return
    
    for restaurant_id in restaurant_ids:
        
        log_msg = "\n<<****** Scraping data for restaurant: {} ******>>".format(restaurant_id)
        printLog(log_msg)
        
        ### ************ Processing orders ************ ###
        printLog("\n# ****** Scraping Restaurant Orders ****** #\n")
        process_orders(driver,restaurant_id)

        ### ************ Processing Operations metrics ************ ###
        printLog("\n# ****** Scraping Restaurant Operations Metrics ****** #\n")
        process_operations_metrics(driver,restaurant_id)

        ### ************ Processing Restaurant Reviews ************ ###
        printLog("\n# ****** Scraping Restaurant Reviews ****** #\n")
        process_restaurant_reviews(driver,restaurant_id)

        ### ************ Processing Restaurant Timings ************ ###
        printLog("\n# ****** Scraping Restaurant Timings ****** #\n")
        process_restaurant_timings(access_token,restaurant_id)
        
        ### ************ Processing Finance Report ************ ###
        printLog("\n# ****** Scraping Finance Report for all outlets. ****** #\n")
        download_report(driver,restaurant_id)

    ### ************ Processing Restaurant Metrics ************ ###
    printLog("\n# ****** Scraping Restaurant metrics all outlets. ****** #\n")
    process_restaurant_metrics(access_token,restaurant_ids,restaurants_names)
    
    ### ************ Processing Ad Performance ************ ###
    printLog("\n# ****** Scraping Ad Performance for all outlets. ****** #\n")
    process_ad_performance(driver,restaurant_ids)

    # ### ************ Processing Discount Performance ************ ###
    printLog("\n# ****** Scraping Discount Performance for all outlets. ****** #\n")
    process_discount_performance(driver,restaurant_ids,restaurants_names,phone_)

    # ### ************ Processing Discount Campaigns Performance ************ ###
    printLog("\n# ****** Scraping Discount campaigns Performance for all outlets. ****** #\n")
    process_discount_campaign_performance(driver, restaurant_ids, restaurants_names)

    # ### ************ Processing Discount Campaigns ************ ###
    printLog("\n# ****** Scraping Discount campaigns for all outlets. ****** #\n")
    process_discount_campaigns(driver,restaurant_ids,restaurants_names)
    
    # ### ************ Processing Daily Metrics ************ ###
    printLog("\n# ****** Scraping Daily Metrics for all outlets. ****** #\n")
    process_daily_metrics(driver,restaurant_ids)

    driver.quit()

def clearFolders():
    """
    Clears and recreates the required directories for downloads and data.
    """
    if os.path.exists('downloads'):
        shutil.rmtree('downloads')
    os.makedirs('downloads', exist_ok=True)

    if os.path.exists('data'):
        shutil.rmtree('data')
    os.makedirs('data', exist_ok=True)

def main():
    """
    Main function to read account details and process each account.
    """

    df = pd.read_excel("accounts.xlsx")

    for i in range(len(df)):
        print(i)
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password

        clearFolders()
        
        process_account(phone_,pass_)
    
    printLog("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()
