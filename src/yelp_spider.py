#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2020/05/5 19:25
# @Author  : jianghui@skieer.com
# @Software: PyCharm

import json
import re

import copyheaders
import requests
import tenacity


class YelpSpider:
    def __init__(self):
        # get proxy
        pass

    """
        start_number 为偏移量，30的倍数，从0开始
    """

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(0.5))
    def get_page(self, start_number):
        url = "https://www.yelp.com/search/snippet?find_desc=bars&find_loc=New%20York%2C%20NY%2C%20United%20States&start={}&parent_request_id=dfcaae5fb7b44685&request_origin=user" \
            .format(start_number)
        headers_str = b"""
            cache-control: max-age=0, must-revalidate, no-cache, no-store, private
            cache-control: no-transform
            cf-cache-status: DYNAMIC
            cf-ray: 58b26184fbd76c86-SJC
            cf-request-id: 02635b471c00006c86b019b200000001
            content-encoding: gzip
            content-security-policy: report-uri https://www.yelp.com/csp_block?id=bf59639897830a99&page=enforced_by_default_directives&policy_hash=7b6f2d6630868fdb2698dac44731677c&site=www&timestamp=1588093661; object-src 'self'; base-uri 'self' https://*.yelpcdn.com https://*.adsrvr.org https://6372968.fls.doubleclick.net; font-src data: 'self' https://*.yelp.com https://*.yelpcdn.com https://fonts.gstatic.com https://connect.facebook.net https://cdnjs.cloudflare.com https://apis.google.com https://www.google-analytics.com https://use.typekit.net https://player.ooyala.com https://use.fontawesome.com https://maxcdn.bootstrapcdn.com https://fonts.googleapis.com
            content-security-policy-report-only: report-uri https://www.yelp.com/csp_report_only?id=bf59639897830a99&page=csp_report_frame_directives%2Cfull_site_ssl_csp_report_directives&policy_hash=9dd00a1a6fbb402584b7ce0c1fdb4d14&site=www&timestamp=1588093661; frame-ancestors 'self' https://*.yelp.com; default-src https:; img-src https: data: https://*.adsrvr.org; script-src https: data: 'unsafe-inline' 'unsafe-eval' blob:; style-src https: 'unsafe-inline' data:; connect-src https:; font-src data: 'self' https://*.yelp.com https://*.yelpcdn.com https://fonts.gstatic.com https://connect.facebook.net https://cdnjs.cloudflare.com https://apis.google.com https://www.google-analytics.com https://use.typekit.net https://player.ooyala.com https://use.fontawesome.com https://maxcdn.bootstrapcdn.com https://fonts.googleapis.com; frame-src https: yelp-webview://* yelp://* data:; child-src https: yelp-webview://* yelp://*; media-src https:; object-src 'self'; base-uri 'self' https://*.yelpcdn.com https://*.adsrvr.org https://6372968.fls.doubleclick.net; form-action https: 'self'
            content-type: application/json; charset=utf-8
            date: Tue, 28 Apr 2020 17:07:42 GMT
            expect-ct: max-age=604800, report-uri="https://report-uri.cloudflare.com/cdn-cgi/beacon/expect-ct"
            expires: Tue, 28 Apr 2020 17:07:41 GMT
            pragma: no-cache
            referrer-policy: origin-when-cross-origin
            server: cloudflare
            status: 200
            strict-transport-security: max-age=31536000; includeSubDomains; preload
            vary: User-Agent
            vary: Accept-Encoding
            x-b3-sampled: 0
            x-content-type-options: nosniff
            x-mode: ro
            x-node: www_all
            x-node: 10-69-179-105-uswest2bprod-9c0a6478-895a-11ea-98c5-b6d34d770
            x-proxied: 10-69-159-164-uswest2bprod
            x-routing-service: 10-69-187-145-uswest2bprod; site=www
            x-xss-protection: 1; report=https://www.yelp.com/xss_protection_report
            x-zipkin-id: 9a87fa4730749a04
        """
        headers = copyheaders.headers_raw_to_dict(headers_str)
        try:
            response = requests.get(url=url, headers=headers, timeout=10)
            return json.loads(response.text)
        except Exception as exception:
            # change proxy if has proxy
            raise exception

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_fixed(1))
    def get_detail(self, url_suffix):
        url = "https://www.yelp.com" + url_suffix
        headers_str = b"""
            authority: www.yelp.com
            method: GET
            path: /biz/district-social-new-york?hrid=295oISkILBXnFFf_ZVDTQw&osq=bars
            scheme: https
            accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9
            accept-encoding: gzip, deflate, br
            accept-language: zh-CN,zh;q=0.9
            cache-control: max-age=0
            cookie: __cfduid=d3207597b7f9964b5d7382fe9d4fea4361588046941; hl=en_US; wdi=1|FECB55555242BF09|0x1.7a9eb1776f972p+30|7f7c45b3a6763c87; _ga=GA1.2.FECB55555242BF09; _gid=GA1.2.1527499598.1588046945; __adroll_fpc=3ea8f1b4e033921303ce5044baa07e08-1588047141657; bse=9e5786973cd34b82b87984dd21b642b0; recentlocations=New+York; location=%7B%22max_longitude%22%3A+-73.7938%2C+%22address3%22%3A+%22%22%2C+%22min_longitude%22%3A+-74.1948%2C+%22neighborhood%22%3A+%22%22%2C+%22address1%22%3A+%22%22%2C+%22place_id%22%3A+%221208%22%2C+%22min_latitude%22%3A+40.5597%2C+%22county%22%3A+null%2C+%22unformatted%22%3A+%22New+York%2C+NY%2C+United+States%22%2C+%22display%22%3A+%22New+York%2C+NY%22%2C+%22borough%22%3A+%22%22%2C+%22polygons%22%3A+null%2C+%22max_latitude%22%3A+40.8523%2C+%22city%22%3A+%22New+York%22%2C+%22isGoogleHood%22%3A+false%2C+%22language%22%3A+null%2C+%22zip%22%3A+%22%22%2C+%22parent_id%22%3A+975%2C+%22country%22%3A+%22US%22%2C+%22provenance%22%3A+%22YELP_GEOCODING_ENGINE%22%2C+%22longitude%22%3A+-74.0072%2C+%22location_type%22%3A+%22locality%22%2C+%22confident%22%3A+null%2C+%22state%22%3A+%22NY%22%2C+%22latitude%22%3A+40.713%2C+%22usingDefaultZip%22%3A+false%2C+%22address2%22%3A+%22%22%2C+%22accuracy%22%3A+4%7D; sc=7861677975; adc=K_e7_aNgghFLZ2zvjFSNkQ%3ArHulTzqonV_UFGbrz301Tg%3A1588131876; xcj=1|nL8omGMlArkoAcUYz2qIiFk0PEVJdicJty4IkWleRno; __ar_v4=7YX6SJQ4RZAMPB6LZ7CHFF%3A20200428%3A12%7CQB5JPFIKRZDSBOZSULG4YB%3A20200428%3A12%7CBHPKS4B4ONEJJMGH4QCJZR%3A20200428%3A12; _gat_www=1
            sec-fetch-mode: navigate
            sec-fetch-site: none
            sec-fetch-user: ?1
            upgrade-insecure-requests: 1
            user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36
        """
        headers = copyheaders.headers_raw_to_dict(headers_str)
        try:
            response = requests.get(url=url, headers=headers, timeout=10)
            text = response.text
            address_detail = re.search("\"addressLines\":(.*?),\"formattedCrossStreets\"", text).groups()[0]
            street_detail = re.search("\"formattedCrossStreets\":(.*?)\\},", text).groups()[0]
            if "&amp;" in street_detail:
                street_detail = street_detail.replace("&amp;", "&")
            detail = dict()
            detail["address_detail"] = eval(address_detail)
            detail["street_detail"] = street_detail

            text = text.replace("\n", "").replace("\r", "")
            script_list = re.findall("<script type=\"application/ld\\+json\">(.*?)</script>", text)
            label_list = list()
            for script_item in script_list:
                script_dic = json.loads(script_item)
                if script_dic["@type"] == "BreadcrumbList":
                    item_list_element = script_dic["itemListElement"]
                    label_list.append(item_list_element[len(item_list_element) - 1]["item"]["name"])
            detail["label_list"] = label_list
            detail["html"] = text
            return detail
        except Exception as exception:
            # change proxy if has proxy
            raise exception
