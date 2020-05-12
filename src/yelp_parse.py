#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2020/05/5 19:25
# @Author  : jianghui@skieer.com
# @Software: PyCharm

import logging.handlers
import traceback
import pymongo
import time
import glom
from yelp_spider import YelpSpider

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/yelp")
mongo_database = mongo_client.get_database()


def exec():
    start_number = 0
    yelp_spider = YelpSpider()
    while True:
        try:
            page = yelp_spider.get_page(start_number=start_number)
        except Exception:
            logger.error(traceback.format_exc())
            # sleep 10 minutes if except exception and then continue
            time.sleep(10 * 60)
            continue
        item_list = glom.glom(page, "searchPageProps.searchResultsProps.searchResults")
        for item in item_list:
            try:
                # print(item)
                if "markerKey" in item and "ad_business" not in str(item["markerKey"]):
                    detail_doc = dict()
                    detail_doc["name"] = item["searchResultBusiness"]["name"]
                    detail_doc["rating"] = item["searchResultBusiness"]["rating"]
                    detail_doc["reviewCount"] = item["searchResultBusiness"]["reviewCount"]
                    detail_doc["phone"] = item["searchResultBusiness"]["phone"]
                    detail_doc["priceRange"] = item["searchResultBusiness"]["priceRange"]
                    detail_doc["address"] = item["searchResultBusiness"]["formattedAddress"]
                    detail_doc["neighborhoods"] = item["searchResultBusiness"]["neighborhoods"]
                    url_suffix = item["snippet"]["readMoreUrl"]
                    detail_doc["url"] = "https://www.yelp.com" + url_suffix
                    try:
                        detail = yelp_spider.get_detail(url_suffix)
                        time.sleep(2)
                    except Exception:
                        logger.error(traceback.format_exc())
                    if detail is not None:
                        detail_doc["address_detail"] = detail["address_detail"]
                        detail_doc["street_detail"] = detail["street_detail"]
                        detail_doc["label_list"] = detail["label_list"]
                        # detail_doc["html"] = detail["html"]
                    print(detail_doc)
            except Exception:
                logger.error(traceback.format_exc())
        start_number += 30


if __name__ == '__main__':
    exec()
