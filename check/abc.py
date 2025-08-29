import concurrent.futures
import gzip

import pymysql
from curl_cffi import requests
import re
import json
import html
import os
import urllib.parse
from parsel import Selector
import hashlib
from dateutil import parser
import amazon_pdp_search_stationary.db_config as db
from amazon_pdp_search_stationary.headers_cookies import review_cookie1, review_header1

def pagesave_fun(asin=None, file_key=None, resposne=None):
    pagesave_path = f'{db.MAIN_PATH}\\REVIEW\\'
    os.makedirs(pagesave_path, exist_ok=True)
    file_name = f"{pagesave_path}{asin}_{file_key}.html.gz"
    with gzip.open(file_name, 'wb')as f:
        f.write(resposne.encode('utf-8'))
def format_date_auto(date_str):
    """Format any date string to 'YYYY-MM-DD'. Returns original if parsing fails."""
    try:
        date_obj = parser.parse(date_str.strip())
        return date_obj.strftime("%Y-%m-%d")
    except Exception as format_date_error:
        print(format_date_error)
        return date_str.strip()

def mk_hash(text: str) -> str:
    """
    Generates a SHA-256 hash for the given text.

    Args:
        text (str): The input string to be hashed.

    Returns:
        str: The SHA-256 hash of the input text as a hexadecimal string.
    """
    # Encode the text to bytes, as hashlib functions require bytes as input
    text_bytes = text.encode('utf-8')

    # Create a SHA-256 hash object
    sha256_hasher = hashlib.sha256()

    # Update the hash object with the text bytes
    sha256_hasher.update(text_bytes)

    # Get the hexadecimal representation of the hash
    hashed_text = sha256_hasher.hexdigest()

    return hashed_text
def extract_review(response, asin=None, total_reviews=None):
    blocks = response.split("&&&")
    review_html_blocks = []
    for block in blocks:
        try:
            parsed = json.loads(block)
            if parsed[0] == "append" and parsed[1] == "#cm_cr-review_list":
                review_html_blocks.append(parsed[2])
        except Exception:
            continue
    for j in review_html_blocks:
        selector = Selector(text=j)
        for review in selector.xpath("//div[contains(@id, 'customer_review-')]"):
            item = dict()
            review_title = review.xpath(
                ".//a[@data-hook='review-title']//span[@class='a-letter-space']/following-sibling::span/text()").get()
            reviwer_name = review.xpath(".//span[@class='a-profile-name']/text()").get()
            temp_rank = review.xpath(".//i[@data-hook='review-star-rating']//text()").get()
            temp_rank = temp_rank.split('out of')[0].strip() if temp_rank else None
            review_date = review.xpath(".//span[@data-hook='review-date']//text()").get()
            text = review.xpath(".//span[@data-hook='review-body']//text()").getall()
            item['review_text'] = re.sub(r'\s+', " ", " ".join(text).strip()) if text else "N/A"
            item['review_title'] = review_title if review_title else "N/A"
            item['reviewer_name'] = reviwer_name if reviwer_name else "N/A"
            item['reviewer_rating_given'] = temp_rank.replace(".0", '').strip() if temp_rank else "N/A"
            item['review_type'] = 'Review'
            item['total_reviews'] = total_reviews
            item['pdp_id'] = asin
            try:
                if review_date:
                    review_date = format_date_auto(review_date.split("on ")[-1])
                else:
                    review_date = "N/A"
            except:
                review_date = "N/A"
            item['review_date'] = review_date
            text = str(item)
            hash_key = mk_hash(text)
            item['hash_key'] = hash_key
            print(item)
            insert_item(item)

DB_CONFIGS = {
    'user':'root',
    'host':'localhost',
    'password':'actowiz',
    'database':db.db_name
}
def insert_item(item=None):
    con  = pymysql.connect(**DB_CONFIGS)
    cur = con.cursor()
    cols = ", ".join(item.keys()).strip(', ')
    values = tuple(item.values())
    try:
        insert_active = f"""INSERT INTO {db.db_name}.{db.review_table} ({cols}) VALUES {values}"""
        cur.execute(insert_active)
        con.commit()
        print('inserted')
    except Exception as e:
        pass

def fetch_reviews(asin=None):
    temp_total_review = 0
    con = pymysql.connect(**DB_CONFIGS)
    cur = con.cursor()
    url = f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews'
    encoded_url = urllib.parse.quote(url, safe='')
    proxy_url = f"https://api.scrape.do/?token={db.scrape_do_key}&customHeaders=true&url={encoded_url}"
    response = requests.get(
        url = proxy_url,
        cookies=review_cookie1,
        headers=review_header1,
    )
    html_content = response.text
    if response.status_code == 200:
        pagesave_fun(asin=asin, file_key="main_request", resposne=html_content)
        res = Selector(response.text)
        stars = ['two_star', 'three_star', 'four_star', 'five_star', 'one_star']
        total_reviews = res.xpath("//div[@data-hook='cr-filter-info-review-rating-count']//text()").get()
        if total_reviews:
            total_reviews = total_reviews.strip().replace(",",'').strip()
            try:
                temp_total_review = int(re.search(r'\d+', total_reviews.strip()).group())
                total_reviews = round(temp_total_review / 10)
                if total_reviews == 2 or total_reviews == 1:
                    total_reviews = 2
                if total_reviews == 0 and temp_total_review >0:
                    total_reviews = 2
            except:
                total_reviews = None
        match = re.search(r'data-state=(["\'])(.*?)\1', html_content)
        if not match:
            return
        clean_json = html.unescape(match.group(2))
        data = json.loads(clean_json)
        token = data.get("reviewsCsrfToken")
        if not token:
            return
        if total_reviews:
            print(f"Found Total Reviews: {temp_total_review} ==> {asin}")
            if temp_total_review > 100:
                for star in stars:
                    page_star = 1
                    while page_star:
                        payload = {
                            'sortBy': 'recent',
                            'reviewerType': 'all_reviews',
                            'formatType': '',
                            'mediaType': '',
                            'filterByStar': '',
                            'filterByAge': '',
                            'pageNumber': str(page_star),
                            'filterByLanguage': '',
                            'filterByKeyword': '',
                            'shouldAppend': 'undefined',
                            'deviceType': 'desktop',
                            'canShowIntHeader': 'undefined',
                            'reftag': f'cm_cr_getr_d_paging_btm_next_{page_star}',
                            'pageSize': '10',
                            'asin': asin,
                            'scope': 'reviewsAjax0',
                        }
                        # payload ={
                        #     'sortBy': '',
                        #     'reviewerType': 'all_reviews',
                        #     'formatType': '',
                        #     'mediaType': '',
                        #     'filterByStar': star,
                        #     'filterByAge': '',
                        #     'pageNumber': str(page_star),
                        #     'filterByLanguage': '',
                        #     'filterByKeyword': '',
                        #     'shouldAppend': 'undefined',
                        #     'deviceType': 'desktop',
                        #     'canShowIntHeader': 'undefined',
                        #     'reftag': 'cm_cr_arp_d_viewopt_sr',
                        #     'pageSize': '10',
                        #     'asin': asin,
                        #     'scope': 'reviewsAjax0',
                        # }

                        review_header2[
                            'refer'] = f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&filterByStar={star}&pageNumber={page_star}'
                        review_header2['anti-csrftoken-a2z'] = token
                        r = requests.post(
                            f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_paging_btm_next_{page_star}',
                            # f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_viewopt_sr',
                            cookies=review_cookie2,
                            headers=review_header2,
                            json=payload,
                        )
                        pagesave_fun(asin=asin, file_key=f"{star}_{page_star}", resposne=r.text)
                        # print(f"📄 Fetching page {page_star} for {asin} star: [{star}]")
                        if r.status_code == 200:
                            print("Accurate response")
                            response = r.text
                            extract_review(response, asin=asin, total_reviews=temp_total_review)
                        else:
                            print(f"Wrong Response: [{star}] ==> ", r.status_code)
                        page_star+=1
                        if page_star==11:
                            break
                keyword_list = ['excellent', 'bad', 'good', 'great', 'quality', 'recommend', 'awsome','value for money', 'worth', 'best']
                for each_key in keyword_list:
                    page_star2 = 1
                    while page_star2:
                        payload = {
                            'sortBy': 'recent',
                            'reviewerType': 'all_reviews',
                            'formatType': '',
                            'mediaType': '',
                            'filterByStar': '',
                            'filterByAge': '',
                            'pageNumber': str(page_star2),
                            'filterByLanguage': '',
                            'filterByKeyword': each_key,
                            'shouldAppend': 'undefined',
                            'deviceType': 'desktop',
                            'canShowIntHeader': 'undefined',
                            'reftag': f'cm_cr_getr_d_paging_btm_next_{page_star2}',
                            'pageSize': '10',
                            'asin': asin,
                            'scope': 'reviewsAjax0',
                        }
                        # payload = {
                        #     'sortBy': '',
                        #     'reviewerType': 'all_reviews',
                        #     'formatType': '',
                        #     'mediaType': '',
                        #     'filterByStar': '',
                        #     'filterByAge': '',
                        #     'pageNumber': str(page_star2),
                        #     'filterByLanguage': '',
                        #     'filterByKeyword': each_key,
                        #     'shouldAppend': 'undefined',
                        #     'deviceType': 'desktop',
                        #     'canShowIntHeader': 'undefined',
                        #     'reftag': 'cm_cr_arp_d_viewopt_kywd',
                        #     'pageSize': '10',
                        #     'asin': asin,
                        #     'scope': 'reviewsAjax0',
                        # }
                        review_header2[
                            'refer'] = f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber={page_star2}'
                        review_header2['anti-csrftoken-a2z'] = token
                        r = requests.post(
                            f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_paging_btm_next_{page_star2}',
                            # f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_viewopt_kywd',
                            cookies=review_cookie2,
                            headers=review_header2,
                            json=payload,
                        )
                        print(f"📄 Fetching page {page_star2} for {asin}: key [{each_key}]")
                        pagesave_fun(asin=asin, file_key=f"{each_key}_{page_star2}", resposne=r.text)
                        if r.status_code == 200:
                            response = r.text
                            extract_review(response, asin=asin, total_reviews=temp_total_review)
                        else:
                            print(f"Wrong Response: [{each_key}] ==> ", r.status_code)
                        page_star2 += 1
                        if page_star2 == 11:
                            break
            else:
                for page in range(1, total_reviews):
                    print(f"📄 Fetching page {page} for {asin}")
                    payload = {
                        'sortBy': 'recent',
                        'reviewerType': 'all_reviews',
                        'formatType': '',
                        'mediaType': '',
                        'filterByStar': '',
                        'filterByAge': '',
                        'pageNumber': str(page),
                        'filterByLanguage': '',
                        'filterByKeyword': '',
                        'shouldAppend': 'undefined',
                        'deviceType': 'desktop',
                        'canShowIntHeader': 'undefined',
                        'reftag': f'cm_cr_getr_d_paging_btm_next_{page}',
                        'pageSize': '10',
                        'asin': asin,
                        'scope': 'reviewsAjax0',
                    }
                    # payload =  {
                    #         'sortBy': '',
                    #         'reviewerType': 'all_reviews',
                    #         'formatType': '',
                    #         'mediaType': '',
                    #         'filterByStar': '',
                    #         'filterByAge': '',
                    #         'pageNumber': str(page),
                    #         'filterByLanguage': '',
                    #         'filterByKeyword': '',
                    #         'shouldAppend': 'undefined',
                    #         'deviceType': 'desktop',
                    #         'canShowIntHeader': 'undefined',
                    #         'reftag': 'cm_cr_arp_d_viewopt_sr',
                    #         'pageSize': '10',
                    #         'asin': asin,
                    #         'scope': 'reviewsAjax0',
                    #     }
                    review_header2['refer'] = f'https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_getr_d_paging_btm_next_2?ie=UTF8&reviewerType=all_reviews&pageNumber={page}'
                    review_header2['anti-csrftoken-a2z'] = token
                    r = requests.post(
                        f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_paging_btm_next_{page}',
                        # f'https://www.amazon.com/hz/reviews-render/ajax/reviews/get/ref=cm_cr_arp_d_viewopt_sr',
                        cookies=review_cookie2,
                        headers=review_header2,
                        json=payload,
                    )
                    if r.status_code == 200:
                        response = r.text
                        extract_review(response, asin=asin, total_reviews=temp_total_review)
                    else:
                        print("Request for: 2 page", r.status_code)
            # cur.execute(f"update {db.db_name}.{db.link_table} set status_review='Done' where pid='{asin}'")
            # con.commit()
    else:
        print(f"status Main: {response.status_code}")
def fetch_link_db():
    con = pymysql.connect(**DB_CONFIGS)
    cur = con.cursor(pymysql.cursors.DictCursor)
    cur.execute(f"select * from {db.db_name}.{db.link_table} where status_review='pending' limit 1")
    links = cur.fetchall()
    links = [x['pid'] for x in links]
    return links
if __name__ == '__main__':
    data = fetch_link_db()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_reviews, asin) for asin in data]
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"❌ Error processing ASIN: {e}")