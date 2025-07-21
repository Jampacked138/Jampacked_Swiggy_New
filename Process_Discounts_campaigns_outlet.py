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
import psycopg2
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
    return re.sub(r'(st|nd|rd|th)', '', date_str)

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
                print(dates)
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

                print(f"campaign date extracted")
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
                capping_match = re.search(r'up\s*to\s*Rs\.?(\d+)', discount_text, re.IGNORECASE)
                if capping_match:
                    capping = int(capping_match.group(1))   
                else:
                    print(f"capping not scraped")

                # Extract MOV
                mov_match = re.search(r'orders above Rs\.?(\d+)', discount_text, re.IGNORECASE)
                if mov_match:
                    mov = int(mov_match.group(1))                   
                else:
                    print(f"mov not scraped")

                # Extract target
                target_match = re.search(r'(\w+)\susers', discount_text, re.IGNORECASE)
                if target_match:
                    target_text = f"{target_match.group(1)} users"  
                    target = [e.strip() for e in target_text.split(",")]                 
                else:
                    print(f"target not scraped")

                time.sleep(0.5) 

                # Extract target
                target_element = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[1]/div[2]')
                # Extract the text
                target_1_text = target_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.bQUVRR.TSSQe').text
                target_1 = [e.strip() for e in target_1_text.split(",")]
                time.sleep(0.5) 

                #Extract offer share
                Cost_sharing_element = driver.find_element(By.XPATH,'//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[2]/div[2]')
                Offer_Share_text = Cost_sharing_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.bQUVRR.TSSQe').text
                Offer_Share_find= re.search(r'\d+%', Offer_Share_text)
                if Offer_Share_find:
                    Offer_Share = Offer_Share_find.group(0)  # Get the matched percentage
                else:
                    print("No percentage found in the text.")
                
                time.sleep(0.5) 

                # Extract Menu
                Menu_inclusion_element = driver.find_element(By.XPATH,'//*[@id="mfe-root"]/div[2]/div[2]/div/div/div/div/div[1]/div[3]/div[2]')
                Menu = Menu_inclusion_element.find_element(By.CLASS_NAME, 'text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.bQUVRR.TSSQe').text
                
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
                RID_Element_text = RID_Element.find_element(By.CSS_SELECTOR, '.text__Text-ssd-app__sc-1imor3w-0.styled__DiscountDetailInfoSubtitle-ssd-app__sc-5gi7jb-23.bQUVRR.eNDuN').text
                # Extract Restaurant_Id using regular expression
                match = re.search(r'RID\s*:\s*(\d+)', RID_Element_text)
                if match:
                    restaurant_id = match.group(1)  # Capture the number after "RID :"
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
                print(f"Restaurant_Id: {restaurant_id}")

                if target_1 == ['Custom'] :  
                    target_value = target
                else:
                    target_value = target_1

                print(f"target_value: {target_value}")
                # Prepare the data dictionary
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
                    print(f"Data for Restaurant ID {data['restaurant_id']} inserted successfully.")

                except Exception as e:
                    print(f"Error inserting data: {e}")
                    connection.rollback()
                    
                time.sleep(5) 
            except Exception as e:
                print(f"Error clicking campaign button {index + 1}, {e}")
        
    except Exception as e:
        print(f"No discount campaigns to scrape.")

def scroll_to_element(driver, element):
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)

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

            # Scroll the container till the track discounts button is visible
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

            # Click the track discounts button
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
                Outlet_pane = wait_visible(driver,'//*[@id="mfe-root"]/div[3]/div')

                # Locate all elements containing RIDs
                elements = driver.find_elements(By.CLASS_NAME, "styled__SubTitle-ssd-app__sc-1ahblbp-6")

                # Extract and print the RIDs
                rids = []
                for element in elements:
                    text = element.text
                    if "RID" in text:
                        rids.append(text.split(":")[1].strip())  # Extract the RID value after "RID :"

                # Print the list of RIDs
                print("Extracted RIDs:", rids)

                confirm_button_xpath = '//*[@id="mfe-root"]/div[3]/div/button' #confirm
                wait_click(driver,confirm_button_xpath)

                for i in range(len(rids)):
                    print(f"scraping for rid: {rids[i]}")
                    Fileter_by_RID_button_Xpath = '//*[@id="mfe-root"]/div[1]/div[2]/div[2]/button[2]'
                    wait_click(driver,Fileter_by_RID_button_Xpath)
                    select_all_RIDs_checkbox = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[3]/div/div[2]/div[2]/label/input')
                    is_checked = select_all_RIDs_checkbox.is_selected()
                    if is_checked:
                        select_all_RIDs_checkbox.click()
                    else:
                        continue

                    search_bar_2 = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[3]/div/div[2]/div[1]/input') 
                    search_bar_2.clear()  # Clear any existing text
                    search_bar_2.send_keys(rids[i])  # Type the RID into the search bar

                    outlets = driver.find_elements(By.CLASS_NAME, 'styled__SubTitle-ssd-app__sc-1ahblbp-6')
                    for outlet in outlets:
                        outlet.click()
                    
                    confirm_button_xpath = '//*[@id="mfe-root"]/div[3]/div/button' #confirm

                    wait_click(driver, confirm_button_xpath)

                    time.sleep(2) 

                    process_campaigns_details(driver)

                    time.sleep(2)

                    Fileter_by_RID_button_Xpath = '//*[@id="mfe-root"]/div[1]/div[2]/div[2]/button[2]'
                    Fileter_by_RID_button = wait_visible(driver,Fileter_by_RID_button_Xpath)
                    if not Fileter_by_RID_button:
                        print(f"filter button not visible")
                    wait_click(driver,Fileter_by_RID_button_Xpath)

                    select_all_RIDs_checkbox = driver.find_element(By.XPATH, '//*[@id="mfe-root"]/div[3]/div/div[2]/div[2]/label/input')
                    is_checked = select_all_RIDs_checkbox.is_selected()
                    if not is_checked:
                        select_all_RIDs_checkbox.click()
                    else:
                        continue
                    
                    confirm_button_xpath = '//*[@id="mfe-root"]/div[3]/div/button' #confirm
                    wait_click(driver,confirm_button_xpath)

                    time.sleep(2)

            except Exception as e :
                print(f"{cities_[i]} has single outlet.")
                time.sleep(2) 
                process_campaigns_details(driver)

                time.sleep(2)

        except Exception as e:
            print(f"Account has single outlet.")
            time.sleep(2) 
            process_campaigns_details(driver)

            time.sleep(2)

        


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
    
    # ### ************ Processing Discount Campaign ************ ###
    printLog("\n# ****** Scraping Discount Performance for all outlets. ****** #\n")
    process_discount_campaigns(driver,restaurant_ids,restaurants_names)

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