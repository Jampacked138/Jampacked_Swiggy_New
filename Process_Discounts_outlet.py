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
        compare_button_xpath = '//button[text()="Compare Performance Of Outlets"]'
        compare_button = wait_visible(driver,compare_button_xpath,10)
        if compare_button is None:
            retry()
        
        try:
            wait_click(driver, compare_button_xpath)
            city_pane = wait_visible(driver,'//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div')
            if city_pane is None:
                city_pane = driver.find_element(By.CLASS_NAME, 'Stack__StackBody-b66d5h-0 dQWwyk SelectFilterList__StyledStack-sc-105m70e-1 jFgQhN')
            cities = city_pane.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
            
            # Locate and use the search bar to search for the city
            search_bar = city_pane.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')  # Adjust placeholder if needed
            search_bar.clear()  # Clear any pre-filled text if necessary
            search_bar.send_keys(cities_[i])  # Type the city name
            
            # Wait for the search results to load and select the desired city
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
                print("Continue button clicked")
            except Exception as e:
                print("Continue button click failed, retrying city click...")

                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                cities = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')
                for city in cities:
                    if city.text == cities_[i]:
                        city.click()
                        break

                try:
                    wait_click(driver, continue_btn_xpath)
                    print("Continue button clicked after retry")
                except Exception as e:
                    print("Continue button click failed even after retry")

            single_account = False

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

            print("Extracted RIDs:", res_ids)

            # Enter first RID in the outlet search bar

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
                    print(f"Error clicking outlet: {type(e).__name__} - {e}")

            # Try clicking Confirm
            confirm_button_xpath = '//div[contains(@class, "SelectFilterList__StyledFooter-sc-105m70e-9")]//button[text()="Confirm" and not(@disabled)]'
            try:
                wait_click(driver, confirm_button_xpath)
                print("Confirm button clicked")
            except Exception as e:
                print("Confirm button click failed, retrying outlet click...")

                # Refresh outlet list
                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                # Retry clicking first outlet
                try:
                    if outlets:
                        outlets[0].click()
                        print("Clicked first visible outlet")
                    else:
                        print("No outlets found on retry")
                except Exception as e:
                    print(f"Retry outlet click failed: {type(e).__name__} - {e}")

                # Retry confirm button
                try:
                    wait_click(driver, confirm_button_xpath)
                    print("Confirm button clicked after retry")
                except Exception as e:
                    print("Confirm button click failed even after retry")


            Outlet_Filter_button_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[1]/div[2]/div[2]/button[2]'
            wait_click(driver, Outlet_Filter_button_xpath)

            search_bar = driver.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')
            search_bar.clear()
            search_bar.send_keys(res_ids[0]) 

            outlet_checkboxes = driver.find_elements(By.CSS_SELECTOR, '.Checkbox__CheckboxInput-qnhwul-0.jwzxuu')

            for outlet_checkbox in outlet_checkboxes:
                try:
                    # Check if the checkbox is selected (checked)
                    is_checked = outlet_checkbox.is_selected()
                    
                    if is_checked:
                        print("Outlet is checked, clicking it.")
                        outlet_checkbox.click()
                    else:
                        print("Outlet is not checked.")
                except Exception as e:
                    print(f"Error interacting with checkbox: {e}")

            for res_id in res_ids:
                print(f"scraping discount performance for res id: {res_id}")

                search_bar = driver.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')
                search_bar.clear()
                search_bar.send_keys(res_id) 

                outlet_checkboxes = driver.find_elements(By.CSS_SELECTOR, '.Checkbox__CheckboxInput-qnhwul-0.jwzxuu')

                for outlet_checkbox in outlet_checkboxes:
                    try:
                        # Check if the checkbox is selected (checked)
                        is_checked = outlet_checkbox.is_selected()                        
                        if not is_checked:
                            print("Outlet is not checked, clicking it.")
                            outlet_checkbox.click()
                        else:
                            print("Outlet is checked.")
                    except Exception as e:
                        print(f"Error interacting with checkbox: {e}")

                wait_click(driver, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[3]/button')                        

                query = """
                    SELECT DISTINCT date
                    FROM swiggy_discount_month_track
                    WHERE restaurant_id = %s;
                    """
                cur.execute(query, (int(res_id),))

                record = cur.fetchone()                   
            
                if record is None:
                    printLog("--Record not available. Processing 90 days data.")

                    for i in range(91,1,-1):
                        date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')

                        check = select_date(driver,date)

                        if not check:
                            driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                            continue
                            
                        check = get_data_rev(driver,date)
                        if not check:
                            get_data_rev(driver,date)
                                
                        driver.switch_to.default_content()

                        query = """
                            INSERT INTO swiggy_discount_month_track (restaurant_id, date)
                            VALUES (%s, %s)
                            ON CONFLICT (restaurant_id)
                            DO UPDATE SET date = EXCLUDED.date
                            """
                        values = (res_id,date)
                        execute_query(query, values)
                        connection.commit()
                
                else:
                    printLog("--Record available. Downloading new data...")

                    query = """
                        SELECT DISTINCT business_date
                        FROM swiggy_discount_metrics
                        WHERE restaurant_id = %s
                        and campaign = 'Overall'
                        ORDER BY business_date DESC;
                    """
                    cur.execute(query, (res_id,))
                    records = cur.fetchall()
                    scraped_dates = [str(record[0]) for record in records]
                    print(res_id)
                    print(scraped_dates)
                    previous_date = record[0]
                    print(f"previous_date:({previous_date})")
                    current_date = datetime.today().date()
                    delta_days = (current_date - previous_date).days

                    for i in range(delta_days, 0, -1):
                        delta = current_date - timedelta(days=i)
                        date = delta.strftime('%Y-%m-%d')

                        if date in scraped_dates:
                            printLog("Date is already scraped. Skipping date.")
                            continue

                        check = select_date(driver,date)
                        if not check:
                            driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                            continue

                        check = get_data_rev(driver,date)
                        if not check:
                            get_data_rev(driver,date)

                        print(f"data scraped for {res_id}")

                        query = """
                            UPDATE swiggy_discount_month_track
                            SET 
                                date = %s
                            WHERE 
                                restaurant_id = %s
                            """
                        values = (date, res_id)
                        execute_query(query, values)
                        connection.commit()

                    print(f"Swiggy_months_track updated for {res_id}")

                Outlet_Filter_button_xpath = '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[1]/div[2]/div[2]/button[2]'
                Outlet_Filter_button = wait_visible(driver, Outlet_Filter_button_xpath)

                if Outlet_Filter_button:
                    wait_click(driver,Outlet_Filter_button_xpath)
                    print(f"clicked on outlet filter")
                else:
                    print(f"couldn not find outlet filter button switching to iframe and trying again")
                    iframe = wait_visible(driver, '//*[@id="metrics-powerbi-frame"]',15)
                    driver.switch_to.frame(iframe)
                    wait_click(driver,Outlet_Filter_button_xpath)
                
                print(f"clicked on outlet filter button")

                search_bar = driver.find_element(By.XPATH, '//*[@id="powerbi-overview-mfe-root"]/div/div/div/div[2]/div/div[2]/div[1]/div[1]/input')
                search_bar.clear()
                search_bar.send_keys(res_id) 

                print(f"entered {res_id} in search bar")

                outlet_checkboxes = driver.find_elements(By.CSS_SELECTOR, '.Checkbox__CheckboxInput-qnhwul-0.jwzxuu')

                for outlet_checkbox in outlet_checkboxes:
                    try:
                        # Check if the checkbox is selected (checked)
                        is_checked = outlet_checkbox.is_selected()
                        
                        if is_checked:
                            print("Outlet is checked, clicking it.")
                            outlet_checkbox.click()
                        else:
                            print("Outlet is not checked.")
                    except Exception as e:
                        print(f"Error interacting with checkbox: {e}")

                    
            refresh(driver)
                    
        except Exception as e:
            if len(restaurant_ids) > 1:
                printLog(f"Unable to scrape for city: {cities_[i]}")
                continue
            elif len(restaurant_ids) == 1:
                printLog("ALERT!!! Account is having single restaurant. Scraping data.")
                single_account = True

                   
            query = """
                SELECT DISTINCT date
                FROM swiggy_discount_month_track
                WHERE restaurant_id IN %(restaurant_list)s
                ;
                """
            cur.execute(query, {"restaurant_list": tuple(restaurant_ids)})
            record = cur.fetchone()             

            if record is None:
                printLog("--Record not available. Processing 90 days data.")
                
                for i in range(91,1,-1):

                    driver.switch_to.default_content()
                    date = (datetime.today() - timedelta(days=i)).strftime('%Y-%m-%d')
                    check = select_date(driver,date)
                    if not check:
                        driver.switch_to.default_content()  # Ensure we return to the main content even if `select_date` fails
                        continue
                        
                    check = get_rev_single_res(driver,restaurant_ids[0],date)
                    if not check:
                        get_rev_single_res(driver,restaurant_ids[0],date)
                        driver.switch_to.default_content()

                    query = """
                        INSERT INTO swiggy_discount_month_track (restaurant_id, date)
                        VALUES (%s, %s)
                        ON CONFLICT (restaurant_id)
                        DO UPDATE SET date = EXCLUDED.date
                        """
                    values = (restaurant_id,date)
                    execute_query(query, values)
                    connection.commit()

            else:
                ids_str = ','.join(map(str, restaurant_ids))
                cur.execute(f"select distinct business_date from swiggy_discount_metrics where restaurant_id in ({ids_str}) order by business_date desc")
                records = cur.fetchall()
                scraped_dates = [str(record[0]) for record in records]
                previous_date = record[0]
                print(f"previous_date:({previous_date})")
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
                    
                    check = get_rev_single_res(driver,restaurant_ids[0],date)
                    if not check:
                        get_rev_single_res(driver,restaurant_ids[0],date)
   
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
       
            driver.switch_to.default_content()

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
        printLog("--Cookies expired. Creating new session.")
        
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
    
    # ### ************ Processing Discount Performance ************ ###
    printLog("\n# ****** Scraping Discount Performance for all outlets. ****** #\n")
    process_discount_performance(driver,restaurant_ids,restaurants_names,phone_)

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

    df = pd.read_excel("new_accounts.xlsx")

    for i in range(len(df)):
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password

        clearFolders()
        
        process_account(phone_,pass_)
    
    printLog("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()