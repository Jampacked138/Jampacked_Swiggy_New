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
            print(f"Waiting for data in frame {frame}: Found {len(rows)} rows")
            if len(rows) > 0:
                break
            elif time_lapsed >= 40:
                print("Timeout: Data not found in frame")
                return False
            time_lapsed += 1
            time.sleep(1)
        return True

    data = {}
    print("Fetching data from the first pane...")
    data_available = wait(0)
    if not data_available:
        print("Data not fetched from 1st pane.")
        return False

    refresh(driver, frame=1)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.find_all("div", attrs={"aria-rowindex": re.compile("\\d+")})
    print(f"Found {len(rows)} rows in first pane.")

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
                    print(f"Clicked expand button for row {row_index}")
                    time.sleep(1)

                    # Capture the expanded content
                    expanded_content_xpath = f'//div[@aria-rowindex="{int(row_index) + 1}"]//div[contains(@class, "expandableContent pivotTableCellWrap tablixAlignLeft")]'
                    
                    # Wait for the expanded content to appear
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.XPATH, expanded_content_xpath))
                    )
                    
                    campaign_text_detail = driver.find_element(By.XPATH, expanded_content_xpath).text.strip()
                    print(f"Fetched campaign_text_detail: {campaign_text_detail}")
                    
                    expanded = True

                    # Collapse the row back
                    expand_button = driver.find_element(By.XPATH, expand_button_xpath)
                    expand_button.click()
                    print(f"Collapsed row {row_index}")
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
                print(f"Processed data for campaign {campaign_text}: {data[campaign_text]}")
            
            row_counter += 1

    except Exception as e:
        print(f"Error: {e}")
        return False

    print("Fetching data from the second pane...")
    data_available = wait(1)
    if not data_available:
        print("Data not fetched from 2nd pane.")
        return False

    refresh(driver, frame=2)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.find_all("div", attrs={"aria-rowindex": re.compile("\\d+")})
    print(f"Found {len(rows)} rows in second pane.")

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
                    print(f"Updated data for campaign {campaign_text}: {data[campaign_text]}")
    except Exception as e:
        print(f"Error: {e}")
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
                    print(f"Error clicking outlet: {type(e).__name__} - {e}")

            # Try clicking Confirm
            confirm_button_xpath = '//div[contains(@class, "SelectFilterList__StyledFooter-sc-105m70e-9")]//button[text()="Confirm" and not(@disabled)]'
            try:
                wait_click(driver, confirm_button_xpath)
                print("Confirm button clicked")
            except Exception as e:
                print("Confirm button click failed, retrying outlet click...")

                wait_visible(driver, '//*[contains(@class, "SelectFilterList__ListItem-sc-105m70e-12")]')
                outlets = driver.find_elements(By.CLASS_NAME, 'SelectFilterList__ListItem-sc-105m70e-12')

                try:
                    if outlets:
                        outlets[0].click()
                        print("Clicked first visible outlet")
                    else:
                        print("No outlets found on retry")
                except Exception as e:
                    print(f"Retry outlet click failed: {type(e).__name__} - {e}")

                try:
                    wait_click(driver, confirm_button_xpath)
                    print("Confirm button clicked after retry")
                except Exception as e:
                    print("Confirm button click failed even after retry")


            for res_id in res_ids:

                print(f"scraping discount performance for restaurant id: {res_id}")

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

                    print(f"data scraped for {res_id}")
                    print(f"Swiggy_months_track updated for {res_id}")

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
                    print(f"previous_date:({previous_date})")
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


                    print(f"data scraped for {res_id}")
                    print(f"Swiggy_months_track updated for {res_id}")
                
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
    process_discount_campaign_performance(driver, restaurant_ids, restaurants_names)

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
        account_ = df.iloc[i]
        phone_ = str(account_.Phone)
        pass_ = account_.Password

        clearFolders()
        
        process_account(phone_,pass_)
    
    printLog("\n# ****** Automation Completed ****** #\n")

if __name__ == "__main__":
    main()