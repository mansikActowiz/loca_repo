
import pymysql
import requests
from scrapy.cmdline import execute
from ics_v1.items import IcsV1PricingItem, IcsV1PDPItem, IcsV1AssetItem
import hashlib
import os
import re
import scrapy
from scrapy import Selector
import json
import ics_v1.db_config as db

from datetime import datetime
from itertools import product as iter_product

cookies = {
    'JSESSIONID': '73567160382A3048A1C3B74882CB986C',
    'sapCustomer': 'false',
    'usa-cart': '463d27bb-680a-4e17-be86-ef14a5eb8eb2',
    '_gcl_au': '1.1.1076136083.1710782783',
    'CookieConsent': '{stamp:%27DTo5d5hI6cJ2rC7k5MPincAtPox5YyAA5JyBIFqFD9eXUQbWGofb4Q==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:3%2Cutc:1710782789137%2Cregion:%27in%27}',
    'AKA_A2': 'A',
    'magnoliaSessionID': 'E8ADC9DB0CAEB829E2D0AE5425EF5099',
    'RT': '"z=1&dm=belimo.com&si=tgqiy99bt98&ss=lunrixep&sl=0&tt=0"',
    '_ga_X37CXEPPQF': 'GS1.1.1712387876.3.1.1712387876.60.0.0',
    '_ga': 'GA1.2.473565451.1710782783',
    '_gid': 'GA1.2.420434429.1712387877',
    '_uetsid': 'cacc2310f3e511ee8e3ebbcd6faa4e52',
    '_uetvid': 'a419acb0e54c11eebfe78f5e608ce083',
    '_dc_gtm_UA-90721347-11': '1',
}

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cache-control': 'max-age=0',
    # 'cookie': 'JSESSIONID=73567160382A3048A1C3B74882CB986C; sapCustomer=false; usa-cart=463d27bb-680a-4e17-be86-ef14a5eb8eb2; _gcl_au=1.1.1076136083.1710782783; CookieConsent={stamp:%27DTo5d5hI6cJ2rC7k5MPincAtPox5YyAA5JyBIFqFD9eXUQbWGofb4Q==%27%2Cnecessary:true%2Cpreferences:true%2Cstatistics:true%2Cmarketing:true%2Cmethod:%27explicit%27%2Cver:3%2Cutc:1710782789137%2Cregion:%27in%27}; AKA_A2=A; magnoliaSessionID=E8ADC9DB0CAEB829E2D0AE5425EF5099; RT="z=1&dm=belimo.com&si=tgqiy99bt98&ss=lunrixep&sl=0&tt=0"; _ga_X37CXEPPQF=GS1.1.1712387876.3.1.1712387876.60.0.0; _ga=GA1.2.473565451.1710782783; _gid=GA1.2.420434429.1712387877; _uetsid=cacc2310f3e511ee8e3ebbcd6faa4e52; _uetvid=a419acb0e54c11eebfe78f5e608ce083; _dc_gtm_UA-90721347-11=1',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
}

def remove_extra_spaces(text):
    clean_text = re.sub('\s+', ' ', text)
    return clean_text.strip()
def remove_substring_between_words(text, start_word, end_word):
    pattern = re.escape(start_word) + r'(.*?)' + re.escape(end_word)
    return re.sub(pattern, '', text)
def remove_a_and_img(text):
    result = []
    for text in text:
        a = remove_substring_between_words(text, '<a', '>')
        b = remove_substring_between_words(a, '<img', '>')
        clean_text = b.replace("</a>", "").replace("</img>", "")
        result.append(clean_text)
    return ' '.join(result)


class Data2Spider(scrapy.Spider):
    name = 'data_tanvi'
    VENDOR_ID = 'ACT-B8-008'
    VENDOR_NAME = 'Belimo'
    # C:/Users/mansi.k/Projects/Data_Store/Pagesave/belimo/15_08_24
    page_save = 'C:/Users/mansi.k/Projects/Data_Store/Pagesave/belimo/15_08_24' + VENDOR_ID + "-" + VENDOR_NAME + '/july24/'

    # path = 'D:/Tanvi/page_save/' + VENDOR_ID + "-" + VENDOR_NAME + "/"

    def __init__(self, name=None, start='', end='', **kwargs):
        super().__init__(name, **kwargs)
        # DATABASE CONNECTION
        self.con = pymysql.connect(host=db.db_host, user=db.db_user, password=db.db_password, db=db.db_name)
        self.cursor = self.con.cursor()
        if not os.path.exists(self.page_save):
            os.makedirs(self.page_save)
        self.start = start
        self.end = end

    def start_requests(self):
        select_query = [
            f"select id, product_urls from {db.sitemap_table} where",
            f"vendor_id = '{self.VENDOR_ID}'",
            # f"and status = 'pending'",
            # f"and status = 'Noname'",
            f"and id between {self.start} and {self.end}"
            # f"and product_urls = 'https://www.belimo.com/us/en_US/products/valves/product-documentation/piping-packages'"
            # f" and product_urls = 'https://www.belimo.com/us/shop/en_US/Valves/Butterfly-Valves/F6100HDU%2BGRX24-3/p?code=F6100HDU%2BGRX24-3'"
            # f" and product_urls = 'https://www.belimo.com/us/shop/en_US/Valves/Characterized-Control-Valves/B307B%2BLF24-S-US/p?code=B307B%2BLF24-S+US'"
            # f"and id between 7000 and 8001"
        ]
        # print(" ".join(select_query))
        self.cursor.execute(" ".join(select_query))

        for data in self.cursor.fetchall():
            filename = f'{self.page_save}{data[0]}.html'
            # if not os.path.exists(filename):
            yield scrapy.Request(
                url=data[1],headers=headers,
                cb_kwargs={
                    "id": data[0],
                    "url": data[1]
                },
                callback=self.pdp,
                dont_filter=True
            )
            if data[1] == "https://www.belimo.com/us/en_US/products/valves/product-documentation/piping-packages":
                yield scrapy.Request(
                    url=data[1],
                    headers=headers,
                    callback=self.piping,
                    dont_filter=True,
                    cb_kwargs={
                        "id": data[0],
                        "url": data[1]
                    }
                )
            break

    def pdp(self, response, **kwargs):
        #pass
        item = IcsV1PDPItem()
        id = kwargs['id']
        try:
            item['id'] = id
        except:
            item['id'] = ''
        open(self.page_save + str(id) + ".html", "wb").write(response.body)

        pdp_url = kwargs['url']
        try:
            item['pdp_url'] = pdp_url
        except:
            item['pdp_url'] = ''

        vendor_id = 'ACT-B8-008'
        try:
            item['vendor_id'] = vendor_id
        except:
            item['vendor_id'] = ''

        vendor_name = 'Belimo'
        try:
            item['vendor_name'] = vendor_name
        except:
            item['vendor_name'] = ''

        hash_key = hashlib.sha256(pdp_url.encode()).hexdigest()
        item['hash_key'] = hash_key
        sku = "".join(response.xpath('//div[@class="column two-third"]//h1//text()').getall())
        item['sku'] = sku
        item['manufacturer'] = vendor_name

        # name = response.xpath('//div[@class="column two-third"]/p//text()[following-sibling::br] | //div[@class="column two-third"]/p//br//following-sibling::text()').get()
        # name = response.xpath('//div[@class="column two-third"]/p[1]/text()[following-sibling::br]'| ).getall()
        name = response.xpath('//div[@class="column two-third"]/p[1]//text()').getall()
        if name is None:
            name = response.xpath('//div[@class="column two-third"]/p[1]//text()').get()
        if not name:
            item['name'] =' '
        else:
            item['name'] = ' '.join(name)

        description = response.xpath("//div[@class='column two-third']/div[@class='pdp-product-note']//p").getall()
        description = [text.strip() for text in description if text.strip()]
        if description:
            desc = ' '.join(description).strip()
            sele_xpath = Selector(text = desc)
            sele_xpath.xpath('//a').extract()
            final_desc = sele_xpath.xpath('//text()').getall()
            description_text = ' '.join(final_desc).strip()
            description_text = re.sub(r'\s+', ' ', description_text).strip()
            item['description'] = description_text
            # item['name'] = description_text if description_text else None
        else:
            item['description'] = None

        description_html_1 = response.xpath("//div[@class='column two-third']/div[@class='pdp-product-note']//p").getall()
        description_html_1 = remove_a_and_img(description_html_1)
        description_html_1 = ''.join(description_html_1)
        description_html_1= f'<p>{description_html_1}</p>' if description_html_1 else None

        if description_html_1:
            item['description_html'] = description_html_1
        else:
            item['description_html'] = None

        category = list()
        category = response.xpath("//div[@class='breadcrumbs']//li//a//text()").getall()
        category_url = response.xpath("//div[@class='breadcrumbs']//li//a/@href").getall()
        cat_list = list()
        scrape_metadata = dict()
        scrape_metadata['url'] = kwargs['url']
        scrape_metadata['date_visited'] = str(datetime.now()).replace(" ", "T")[:-3] + "Z"
        for mm, nn in zip(category, category_url):
            if nn.startswith("https://www.belimo.com"):
                cat_list.append({
                    "name": mm.strip(),
                    "url":  nn.strip(),
                })
            else:
                cat_list.append({
                    "name": mm.strip(),
                    "url": "https://www.belimo.com" + nn.strip(),
                })
        scrape_metadata['breadcrumbs'] = cat_list
        item['_scrape_metadata'] = json.dumps(scrape_metadata)
        item['status'] = 'Done'

        item['mpn'] = None

        category_ls = list()
        category_ls = response.xpath("//div[@class='breadcrumbs']//li//a//text()").getall()
        if 'Home' in category_ls:
            category_ls.remove('Home')
        try:
            item['category'] = json.dumps(category_ls, ensure_ascii=False)
        except:
            item['category'] = ''

        avaliable_to_checkout = response.xpath("//form[contains(@id,'addToCartFormL')]//div[@class='form-control form-button']//button | //div[@class='product-cart']//div[@class='form-control form-button']//button").get()
        if avaliable_to_checkout:
            item['available_to_checkout'] = True
            item['in_stock'] = True
        else:
            item['available_to_checkout'] = False
            item['in_stock'] = False

        myattribute_list = []
        for key_value in response.xpath("//div[@class='cpq-groups'] |//div[@class='product-specification']"):
            keys = key_value.xpath('.//dt//label/text() |.//dt/text()').getall()
            key = []
            for i in keys:
                if '\n' not in i and '\r' not in i and '\t' not in i:
                    key.append(i)
                else:
                    pass
            value = key_value.xpath('.//dd')
            if key and value:
                attributes = []
                for k, value in zip(key, value):
                    key_text = k.strip()
                    values = value.xpath(".//option/text() |.//label/text() |./text() |.//li/text()").getall()
                    values = [v.strip() for v in values if v.strip()]
                    if values:
                        for v in values:
                            attributes.append({
                                "name": key_text,
                                "value": v,
                                "group": "Specifications"
                            })
                myattribute_list.extend(attributes)


        item['attributes'] = json.dumps(myattribute_list, ensure_ascii=True)

        yield item
#---------------------------------------------PRICING-------------------------------------------
        item1 = IcsV1PricingItem()
        pricing_elements = response.xpath("//div[@class='product-cart']//dl//dd//text()").getall()
        if pricing_elements:
            price = ''.join(pricing_elements).replace('$','').replace(',','').strip()
            item1['vendor_id'] = vendor_id
            item1['sku'] = sku
            item1['hash_key'] = hash_key
            item1['price'] = price
            item1['currency'] = 'USD'
            item1['min_qty'] = '1'
            yield item1
        else:
            item1['vendor_id'] = vendor_id
            item1['sku'] = sku
            item1['hash_key'] = hash_key
            item1['price_string'] = "Call For Price"
            item1['currency'] = 'USD'
            item1['min_qty'] = '1'
            yield item1
# ---------------------------------------------ASSET-------------------------------------------
        img_elements = response.xpath("//div[@class='carousel image-gallery__image js-gallery-image']//img")
        if img_elements:
            image_count = 1
            original_images_found = True
            for img_element in img_elements:
                source_url = img_element.xpath('./@srcset').re_first(r'([^,]+)\s[0-9]+w')
                i_name = img_element.xpath('./@alt').get()
                if i_name:
                    title=i_name.strip()
                else:
                    title = None

                if source_url and not source_url.startswith("https://www.belimo.com"):
                    source_url = "https://www.belimo.com" + source_url
                image_item = IcsV1AssetItem()
                image_item['vendor_id'] = self.VENDOR_ID
                image_item['sku'] = sku
                image_item['hash_key'] = hash_key
                image_item['name'] = title
                image_item['type'] = "image/product"
                image_item['source'] = source_url
                image_item['file_name'] = source_url.split("?")[0].split("/")[-1]
                if image_count == 1:
                    image_item['is_main_image'] = True
                else:
                    image_item['is_main_image'] = False
                yield image_item
                image_count += 1

        elements = response.xpath("//div//ul[@class='documents-list']//li[@class='pdhi-document']")
        try:
            for element in elements:
                pdf_element = element.xpath('.//a[strong]/@href').get()
                pdf_title = element.xpath('.//a/strong//text()').get()
                allowed_extensions = ['.pdf', '.zip', '.doc', '.docx', '.ppt', '.txt', '.dmg', '.exe', '.tar.gz', '.tar.xz', '.step',
                                  '.dxf', '.lbr', '.dwg', '.pkg', '.msi', '.mpeg', '.fmi', '.stp', '.igs' , '.sat']

                if not any(pdf_element.endswith(ext) for ext in allowed_extensions):
                    continue
                pdf_item = IcsV1AssetItem()
                pdf_item['vendor_id'] = self.VENDOR_ID  # Use the first element if VENDOR_ID is a list
                pdf_item['sku'] = sku.strip()
                pdf_item['hash_key'] = hash_key
                pdf_item['name'] = pdf_title
                if "data sheet" in pdf_title.lower():
                    pdf_item['type'] = 'document/spec'
                elif "brochure" in pdf_title.lower():
                    pdf_item['type'] = 'document/manual'
                elif "format" in pdf_title.lower():
                    pdf_item['type'] = 'document'
                elif "documentation" in pdf_title.lower():
                    pdf_item['type'] = 'document/catalog'
                else:
                    pdf_item['type'] = 'document'
                # pdf_item['type'] = 'document/pdf'
                pdf_item['source'] = pdf_element
                pdf_item['file_name'] = pdf_element.split('/')[-1]
                yield pdf_item
        except:
            pass

# ----------------------------------------------------- FOR CATEGORY PIPING------------------------------------------------------------------

    def piping(self, response, **kwargs):
        item2 = IcsV1PDPItem()
        id = kwargs['id']
        pdp_url = kwargs['url']
        vendor_id = 'ACT-B8-008'
        vendor_name = 'Belimo'
        # hash_key = hashlib.sha256(f"{pdp_url}_{id}".encode()).hexdigest()
        name = response.xpath('//div[@class="teaser-list"]//div[@class="grid teaser-row "]//div[@class="column one-third teaser-column teaser-column"]//article//h3[@class="teaser-article-title"]//text()').getall()
        filtered_name = [item.strip() for item in name if item.strip()]
        words_to_remove = ['Reliability', 'Installation Efficiency', 'Dependable']
        filtered_name = [item for item in filtered_name if item not in words_to_remove]
        desired_output = [filtered_name[i:i + 2] for i in range(0, len(filtered_name), 2)]
        # print(desired_output)

        img_elements = response.xpath(
            '//div[@class="teaser-list"]//div[@class="grid teaser-row "]//div[@class="column one-third teaser-column teaser-column"]//article//div[@class="image "]//img')[3:]
        doc_elements = response.xpath(
            '//div[@class="teaser-list"]//div[@class="grid teaser-row "]//div[@class="column one-third teaser-column teaser-column"]//article//span[@class="link"]/a')

        for index, (product_name, img_element, doc_element) in enumerate(zip(desired_output, img_elements, doc_elements)):
            if product_name != ' ':
                item2['name'] = ' '.join(product_name)
                pdp_url = kwargs['url']
                item2['id'] = id
                item2['pdp_url'] = pdp_url
                item2['vendor_id'] = vendor_id
                item2['vendor_name'] = vendor_name
                item2['manufacturer'] = vendor_name
                hash_key = hashlib.sha256(f"{pdp_url}_{product_name}".encode()).hexdigest()
                item2['hash_key'] = hash_key
                item2['sku'] = ' '
                item2['description'] = None
                item2['description_html'] = None
                scrape_metadata = {"url": "https://www.belimo.com/us/en_US/products/valves/product-documentation/piping-packages", "breadcrumbs": [{"url": "https://www.belimo.com/us/shop/en_US/", "name": "Home"}, {"url": "https://www.belimo.com/us/en_US/products/valves.html", "name": "Valves"}, {"url": "https://www.belimo.com/us/en_US/products/valves/product-documentation/piping-packages", "name": "Piping Packages"}], "date_visited": f'{str(datetime.now()).replace(" ", "T")[:-3] + "Z"}'}
                item2['_scrape_metadata'] = json.dumps(scrape_metadata)
                item2['mpn'] = None
                item2['category'] = '["Valves"]'
                item2['available_to_checkout'] = None
                item2['in_stock'] = None
                item2['attributes'] = None
                yield item2

                source_url = img_element.xpath('./@srcset').re_first(r'([^,]+)\s[0-9]+w') if img_element.xpath(
                    './@srcset') else None
                i_name = img_element.xpath('./@alt').get()
                if source_url and not source_url.startswith("https://www.belimo.com"):
                    source_url = "https://www.belimo.com" + source_url.replace('\n', '')

                image_item = IcsV1AssetItem()
                image_item['vendor_id'] = self.VENDOR_ID
                image_item['sku'] = ' '
                image_item['hash_key'] = hash_key
                image_item['name'] = i_name
                image_item['type'] = "image/product"
                image_item['source'] = source_url
                image_item['file_name'] = source_url.split("?")[0].split("/")[-1]
                image_item['is_main_image'] = True
                # print([item ,image_item])
                yield image_item

                doc_name = doc_element.xpath('./text()').get()
                doc_name = doc_name.strip().replace('\n', '').replace('\t', '')
                doc_url = ''.join(doc_element.xpath('./@href').getall()).strip().replace('\n', '').replace('\t', '')
                doc_url = ' '.join(doc_url.split())
                if doc_url and not doc_url.startswith("https://www.belimo.com"):
                    doc_url = "https://www.belimo.com" + doc_url

                document_item = IcsV1AssetItem()
                document_item['vendor_id'] = self.VENDOR_ID
                document_item['sku'] = ' '
                document_item['hash_key'] = hash_key
                document_item['name'] = doc_name
                document_item['type'] = "document/product"
                document_item['source'] = doc_url
                document_item['file_name'] = doc_url.split("?")[0].split("/")[-1]
                # print([item, image_item, document_item])
                yield document_item
                # print([document_item])

                item1 = IcsV1PricingItem()
                item1['vendor_id'] = 'ACT-B8-008'
                item1['sku'] = None
                item1['hash_key'] = hash_key
                item1['price_string'] = "Call For Price"
                item1['currency'] = 'USD'
                item1['min_qty'] = '1'
                yield item1
                # break


if __name__ == '__main__':
    execute(f'scrapy crawl data_tanvi -a start=85040 -a end=85040'.split())
    # execute(f'scrapy crawl data_tanvi'.split())
 