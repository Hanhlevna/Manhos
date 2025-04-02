from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
import json
import time
import requests
import numpy as np
import pandas as pd
import multiprocessing as mp
import math
import blinker

pd.options.mode.chained_assignment = None  # default='warn'

# ********************************************************************************************************************************************
def extract_hotel_links(url, processed_links):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    property_cards = soup.find_all("div", {"data-testid": "property-card"})
    properties = []
    for property in property_cards:
        new_property = {}
        try:
            new_property['link'] = property.find('a', {'data-testid': 'title-link'}).get('href').replace("en-gb.html", "vi.html")
        except:
            new_property['link'] = np.nan
        # Skip if duplicate
        if new_property['link'] in processed_links:
            continue
        try:
            new_property['hotel_name'] = property.find('div', {'data-testid': 'title'}).text
        except:
            new_property['hotel_name'] = np.nan
        try:
            review_score, review_count = property.find('div', {'data-testid': 'review-score'})
            new_property['overall_rating'] = review_score.text.strip()
            new_property['type_rating'] = review_count.text.split(" ")[-3]
        except:
            new_property['overall_rating'] = np.nan
            new_property['type_rating'] = np.nan
        try:
            new_property['price'] = property.find('span', {'data-testid': 'price-and-discounted-price'}).text
        except:
            new_property['price'] = np.nan
        try:
            new_property['address'] = property.find('span', {'data-testid': 'address'}).text
        except:
            new_property['address'] = np.nan
        try:
            new_property['image'] = property.find('img', {'data-testid': 'image'}).get('src')
        except:
            new_property['image'] = np.nan
        properties.append(new_property)
        processed_links.append(new_property['link'])
    return properties, processed_links

# ********************************************************************************************************************************************
def extract_hotel_links_html(url, processed_links):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    # resp = requests.get(url, headers=headers)
    soup = BeautifulSoup(url, 'html.parser')
    property_cards = soup.find_all("div", {"data-testid": "property-card"})
    properties = []
    for property in property_cards:
        new_property = {}
        try:
            new_property['link'] = property.find('a', {'data-testid': 'title-link'}).get('href').replace("en-gb.html", "vi.html")
        except:
            new_property['link'] = np.nan
        # Skip if duplicate
        if new_property['link'] in processed_links:
            continue
        try:
            new_property['hotel_name'] = property.find('div', {'data-testid': 'title'}).text
        except:
            new_property['hotel_name'] = np.nan
        try:
            review_score, review_count = property.find('div', {'data-testid': 'review-score'})
            new_property['overall_rating'] = review_score.text.strip()
            new_property['type_rating'] = review_count.text.split(" ")[-3]
        except:
            new_property['overall_rating'] = np.nan
            new_property['type_rating'] = np.nan
        try:
            new_property['price'] = property.find('span', {'data-testid': 'price-and-discounted-price'}).text
        except:
            new_property['price'] = np.nan
        try:
            new_property['address'] = property.find('span', {'data-testid': 'address'}).text
        except:
            new_property['address'] = np.nan
        try:
            new_property['image'] = property.find('img', {'data-testid': 'image'}).get('src')
        except:
            new_property['image'] = np.nan
        properties.append(new_property)
        processed_links.append(new_property['link'])
    return properties, processed_links

# ********************************************************************************************************************************************
def extract_hotel_properties(target_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    resp = requests.get(target_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')

    o = {}
    o['link'] = str(target_url)
    try:
        o["name"] = soup.find("h2", {"class": "pp-header__title"}).text
    except:
        o["name"] = np.nan
    try:
        address_div = soup.find("div", {"class": "a53cbfa6de f17adf7576"})
        if address_div:
            address = address_div.contents[0].strip() 
            o["address"] = address
        else:
            o["address"] = np.nan
    except:
        o["address"] = np.nan
    try:
        o["content"] = soup.find("div", {"class": "hp_desc_main_content"}).text.strip()
    except:
        o["content"] = np.nan
    try:
        rs, rc = soup.find('div', {'data-testid': 'review-score-component'})
        o['review_score'] = rs.text.strip()
    except:
        o['review_score'] = np.nan
    try:
        o['review_count'] = rc.text.split("\xa0·\xa0")[-1]
    except:
        o['review_count'] = np.nan
        
    # Star rating
    star_container = soup.find("span", attrs={"data-testid": "rating-stars"})
    if star_container is not None:
        child_spans = star_container.find_all("span", recursive=False)
        o['star_rating'] = len(child_spans)
    else:
        o['star_rating'] = None

    try:
        fac = soup.find("div", {"data-testid": "property-most-popular-facilities-wrapper"})
        fac_arr = []
        for i in range(len(fac.find_all("span"))):
            all_fac = fac.find_all("span")
            text = all_fac[i].text.strip()
            if text != "" and text not in fac_arr:
                fac_arr.append(text)
        o['most_facility'] = str(fac_arr)
    except:
        o['most_facility'] = np.nan

    # All facilities
    try:
        all_facilities = []
        for each in soup.find_all("div",{"class": "f1e6195c8b"}):
            t = each.find('div', {'class':'d1ca9115fe'}).text
            rs=[]
            for a in each.find_all("span"):
                x = a.text.replace(t,'').strip()
                if x !='' and x not in rs:
                    rs.append(a.text.replace(t,'').strip())
            all_facilities.append({'key':t, 'value':rs})
        o['all_facilities'] = all_facilities
    except:
        o['all_facilities'] = np.nan

    # Assect score
    try:
        sub_score = []
        for each in soup.find_all("div",{"class": "b817090550 a7cf1a6b1d"}):
            t = each.find('div', {'class':'c72df67c95'}).text
            rs=[]
            for a in each.find_all('div', {'class':'ccb65902b2'}):
                x = a.text.replace(t,'').strip()
                if x !='' and x not in rs:
                    rs.append(a.text.replace(t,'').strip())
            sub_score.append({'aspect':t, 'value':rs})
        o['sub_score'] = sub_score
    except:
        o['sub_score'] = np.nan

    try:
        tr = soup.find_all("tr", recursive=True)
    except:
        tr = None
    # print(tr)    
    
    # Facility for basic room
    fac_holder = soup.find("div", class_="wholesalers_table__facilities_holder wholesalers_table__facilities_holder_initial")

    if fac_holder:
        facility_spans = fac_holder.find_all("span", class_="hprt-facilities-facility")
        facilities = [span.get_text(strip=True) for span in facility_spans if span.get_text(strip=True)]
    else:
        facilities = []

    dataset = []
    temp = {}
    for y in range(0, len(tr)):
        try:
            datapoint = {}
            id = tr[y].get('data-block-id')
            maxp = tr[y].find("span", {"class": "bui-u-sr-only"}).text.strip()
            # room_fac_basic = tr[y].find_all("span", {"class": "hprt-facilities-facility"})
            room_fac = tr[y].find_all("div", {"class": "hprt-facilities-facility"})
            try:
                room_price = tr[y].find("span", {"class": "prco-valign-middle-helper"}).text.strip()
                room_price = room_price.replace("\xa0", " ")
            except:
                room_price = np.nan
            try:
                roomtype = tr[y].find("a", {"class": "hprt-roomtype-link"}).text.strip()
            except:
                roomtype = np.nan
        except:
            id = None

        if (id is not None) and id !='':
            datapoint['id'] = id
            datapoint['roomtype'] = roomtype
            datapoint['room_facs'] = [facilities] + [rf.text for rf in room_fac[1:]]
            datapoint['price/max_person'] = [str(room_price) + " / " + str(maxp)]
            dataset.append(datapoint)
    o['rooms'] = dataset
    
    o['rooms_type'] = list(dict.fromkeys([room['roomtype'] for room in o['rooms']]))
    
    return o

# ********************************************************************************************************************************************
def extract_reviews_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    reviews = []
    review_cards = soup.find_all('div', {'data-testid': 'review-card'})
    for card in review_cards:
        try:
            user = card.find('div', {'class': 'a3332d346a e6208ee469'}).text.strip()
        except:
            user = None
        try:
            country = card.find('span', {'class': 'afac1f68d9 a1ad95c055'}).text.strip()
        except:
            country = None
        try:
            date = card.find('span', {'data-testid': 'review-date'}).text.strip()
        except:
            date = None
        try:
            title = card.find('h3', {'data-testid': 'review-title'}).text.strip()
        except:
            title = None
        try:
            score = card.find('div', {'data-testid': 'review-score'}).text.strip()
        except:
            score = None
        try:
            positive_comment = card.find('div', {'data-testid': 'review-positive-text'}).text.strip()
        except:
            positive_comment = None
        try:
            negative_comment = card.find('div', {'data-testid': 'review-negative-text'}).text.strip()
        except:
            negative_comment = None
            
        reviews.append({
            'user': user,
            'country': country,
            'date': date,
            'title': title,
            'score': score,
            'positive_comment': positive_comment,
            'negative_comment': negative_comment
        })
    return reviews

# ********************************************************************************************************************************************
import math
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from seleniumwire import webdriver
from bs4 import BeautifulSoup

def extract_reviews(url):
    print(f"Extracting reviews for: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    options = Options()
    options.add_argument("start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    time.sleep(random.uniform(7, 10))
    reviews = []

    # Page counts
    try:
        no_reviews = driver.find_element(By.XPATH,'/html/body/div[4]/div/div[4]/div[1]/div[1]/div[1]/div[1]/div[1]/div/nav/div/ul/li[6]/a/span/div/span').text.split(' ')[2].strip('()')
        total_reviews = int(no_reviews.split(' ')[0])
        pages = math.ceil(total_reviews / 10)
    except:
        pages = 0
    time.sleep(random.uniform(1, 3))
    
    if pages < 1:
        return reviews
    
    try:
        # Click to expand all reviews
        expand = driver.find_element(By.XPATH, './/button[contains(@data-testid, "fr-read-all-reviews")]')
        driver.execute_script("arguments[0].click();", expand)
        time.sleep(random.uniform(5, 8))
    
        id_url = url.split('?')[0].split('/')[-1]
        for _ in range(pages):
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            review_cards = soup.find_all('div', {'data-testid': 'review-card'})
            for card in review_cards:
                try:
                    date = card.find('span', {'data-testid': 'review-date'}).text.strip()
                except:
                    date = None
                try:
                    title = card.find('h4', {'data-testid': 'review-title'}).text.strip()
                except:
                    title = None
                try:
                    content = card.find('div', {'data-testid': 'review-positive-text'}).text.strip()
                except:
                    content = None
                try:
                    score = card.find('div', {'data-testid': 'review-score'}).text.strip()
                except:
                    score = None
                try:
                    user = card.find('div', {'class': 'a3332d346a e6208ee469'}).text.strip()
                except:
                    user = None
                try:
                    country = card.find('span', {'class': 'afac1f68d9 a1ad95c055'}).text.strip()
                except:
                    country = None
    
                # Append review data as dictionary
                reviews.append({
                    "date": date,
                    "title": title,
                    "content": content,
                    "score": score,
                    "user": user,
                    "country": country,
                    "url": url,
                    "id_url": id_url
                })
    
            time.sleep(random.uniform(4, 6))
    
            try:
                next_page_btn = driver.find_element(By.XPATH, '//button[contains(@aria-label, "Next page")]')
                driver.execute_script("arguments[0].click();", next_page_btn)
                time.sleep(random.uniform(5, 8))  # Randomized delay before clicking next page
            except Exception:
                break

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        driver.quit()

    return reviews

# ********************************************************************************************************************************************
def crawling_from_booking(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=headers)
    page_soup = BeautifulSoup(resp.text, 'html.parser')

    try:
        total_pages = int(page_soup.find_all("div", {"data-testid": "pagination"})[0].find_all('li')[-1].text)
    except:
        total_pages = 1

    options = Options()
    options.add_argument("start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.get(url)

    properties = []
    processed_links = []
    for current_page in range(total_pages):
        new_properties, processed_links = extract_hotel_links(driver.current_url, processed_links)
        properties.extend(new_properties)
        if total_pages > 1:
            try:
                next_page_btn = driver.find_element(By.XPATH, '//button[contains(@aria-label, "Next page")]')
                driver.execute_script("arguments[0].click();", next_page_btn)
                time.sleep(1)
            except:
                break
    driver.quit()

    data_hotels = []
    for link in tqdm([prop['link'] for prop in properties]):
        data_hotels.append(extract_hotel_properties(link))

    reviews = []
    with mp.Pool(6) as p:
        reviews.extend(p.map(extract_reviews, [prop['link'] for prop in properties]))

    return {
        "hotels": data_hotels,
        "reviews": reviews
    }

# ********************************************************************************************************************************************
def crawling_from_booking_optional(url, max_hotels=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=headers)
    page_soup = BeautifulSoup(resp.text, 'html.parser')

    try:
        total_pages = int(page_soup.find_all("div", {"data-testid": "pagination"})[0].find_all('li')[-1].text)
    except:
        total_pages = 1

    options = Options()
    options.add_argument("start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.get(url)

    properties = []
    processed_links = []
    for current_page in range(total_pages):
        new_properties, processed_links = extract_hotel_links(driver.current_url, processed_links)
        properties.extend(new_properties)

        # Limit the number of hotels if max_hotels is specified
        if max_hotels and len(properties) >= max_hotels:
            properties = properties[:max_hotels]  # Trim the list if it exceeds the maximum
            break

        if total_pages > 1:
            try:
                next_page_btn = driver.find_element(By.XPATH, '//button[contains(@aria-label, "Next page")]')
                driver.execute_script("arguments[0].click();", next_page_btn)
                time.sleep(1)
            except:
                break
    driver.quit()

    # Debugging: Check if properties contain valid links
    print(f"Total properties collected: {len(properties)}")
    for i, prop in enumerate(properties):
        print(f"Property {i+1}: {prop['link']}")

    data_hotels = []
    for link in tqdm([prop['link'] for prop in properties]):
        data_hotels.append(extract_hotel_properties(link))

    # Debugging: Check if hotel details are collected
    print(f"Total hotels collected: {len(data_hotels)}")

    reviews = []
    if properties:  # Ensure there are properties to process
        with mp.Pool(4) as p:
            reviews = p.map(extract_reviews, [prop['link'] for prop in properties])

    # Debugging: Check if reviews are collected
    for i, review_list in enumerate(reviews):
        print(f"Property {i+1} has {len(review_list)} reviews")

    return {
        "hotels": data_hotels,
        "reviews": reviews
    }
    
# ********************************************************************************************************************************************
def click_expand(driver, wait_time=1, scroll_step=300, scroll_pause=0.2):
    click_count = 0  

    try:
        # **Step 1: roll page down to the bottom slowly**
        last_height = driver.execute_script("return document.body.scrollHeight")
        current_position = 0

        while current_position < last_height:
            current_position += scroll_step  
            driver.execute_script(f"window.scrollTo(0, {current_position});")
            time.sleep(scroll_pause)  
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height > last_height:
                last_height = new_height  

        print("Finished scrolling. Now clicking 'Load more results' button...")

        # **Step 2: Click the 'Load more results' box**
        while True:
            try:
                span_xpath = "//span[@class='e4adce92df' and text()='Load more results']"
                span_element = driver.find_element(By.XPATH, span_xpath)

                button = span_element.find_element(By.XPATH, "./ancestor::button")

                driver.execute_script("arguments[0].scrollIntoView();", button)

                driver.execute_script("arguments[0].click();", button)
                click_count += 1
                print(f"Clicked {click_count} times")

                time.sleep(wait_time)
                
            except:
                print(f"No more 'Load more results' button found. Total clicks: {click_count}")
                break

    except Exception as e:
        print(f"Error: {e}")

    return driver 
    
# ********************************************************************************************************************************************
def crawling_from_booking_all(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"}
    resp = requests.get(url, headers=headers)
    page_soup = BeautifulSoup(resp.text, 'html.parser')

    try:
        total_pages = int(page_soup.find_all("div", {"data-testid": "pagination"})[0].find_all('li')[-1].text)
    except:
        total_pages = 1

    options = Options()
    options.add_argument("start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    
    # Click to load more results
    driver = click_expand(driver, wait_time=1, scroll_step=300, scroll_pause=0.2)

    properties = []
    processed_links = []
    for current_page in range(total_pages):
        del driver.requests # processed_links to avoid duplicate Hotel on search pages
        html_source = driver.page_source  # Lấy toàn bộ HTML sau khi đã mở rộng
        new_properties, processed_links = extract_hotel_links_html(html_source, processed_links)
        properties.extend(new_properties)
        
        if total_pages > 1:
            try:
                next_page_btn = driver.find_element(By.XPATH, '//button[contains(@aria-label, "Next page")]')
                driver.execute_script("arguments[0].click();", next_page_btn)
                time.sleep(1)
            except:
                break
    driver.quit()

    # Debugging: Check if properties contain valid links
    print(f"Total properties collected: {len(properties)}")
    for i, prop in enumerate(properties):
        print(f"Property {i+1}: {prop['link']}")

    data_hotels = []
    for link in tqdm([prop['link'] for prop in properties]):
        data_hotels.append(extract_hotel_properties(link))

    # Debugging: Check if hotel details are collected
    print(f"Total hotels collected: {len(data_hotels)}")

    reviews = []
    if properties:  # Ensure there are properties to process
        with mp.Pool(4) as p:
            reviews = p.map(extract_reviews, [prop['link'] for prop in properties])

    # Debugging: Check if reviews are collected
    for i, review_list in enumerate(reviews):
        print(f"Property {i+1} has {len(review_list)} reviews")

    return {
        "hotels": data_hotels,
        "reviews": reviews
    }

# ********************************************************************************************************************************************
def crawl_until_done(url):
    try:
        start_time = time.time()
        crawling_from_booking(url)
        print("Time process: ", time.time() - start_time)
    except:
        print("Time process: ", time.time() - start_time)
        print("Recrawl")
        crawl_until_done(url)