import requests
import urllib
import time
import decimal
import json

from selenium import webdriver

import datetime

import logging
import boto3

# ログ設定
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
# DynamoDBオブジェクト
dynamodb = boto3.resource('dynamodb')
# S3オブジェクト
s3 = boto3.resource('s3')
# 郵便番号を取得するAPIのエンドポイント
HR_GEO_API = 'http://geoapi.heartrails.com/api/json?method=getPrefectures'
# 気象情報を取得するWebサイトの該当ページ
TENKI_URL = 'https://tenki.jp/search/?keyword='

# 連番を更新して返す関数
# def next_seq(table, tablename):
#     response = table.update_item(
#         Key={
#             'tablename' : tablename
#         },
#         UpdateExpression="set seq = seq + :val",
#         ExpressionAttributeValues= {
#             ':val' : 1
#         },
#         ReturnValues='UPDATED_NEW'
#     )
#     return response['Attributes']['seq']

# Lambda関数呼び出し時に最初に呼ばれる関数
def lambda_handler(event, context):
    try:

        options = webdriver.ChromeOptions()
        options.binary_location = "./bin/headless-chromium"
        options.add_argument('--headless')
        options.add_argument("--no-sandbox")
        options.add_argument("--single-process")
        #options.add_argument('--disable-gpu')
        #browser = webdriver.Chrome(options=options)
        # browser = webdriver.Chrome("./bin/chromedriver",chrome_options=options)
        browser = webdriver.Chrome(
            "./bin/chromedriver",
            chrome_options=options)
 
        #browser.implicitly_wait(5)

        # 見直し案
        # 都道府県に存在する市区町村を取得し、市区町村毎に町域情報を取得。取得したリストから、「town」が「（その他）」の郵便番号を利用して気象情報を取得。

        # 都道府県一覧を取得：「HeartRails Geo API」のサービスを利用する（郵便番号／住所／緯度経度などの地理情報を無料で提供 http://geoapi.heartrails.com/）
        # 全都道府県だとスクレイピングが大変なので、関東、もしくは東京に限定してみる
        prefectures_response = requests.get('http://geoapi.heartrails.com/api/json?method=getPrefectures',headers={'Content-Type': 'application/json'})
        prefectures = prefectures_response.json()['response']['prefecture']
        # テスト用コードで東京都に限定
        prefectures = [ prefecture for prefecture in prefectures_response.json()['response']['prefecture'] if prefecture == '東京都']
        #print(prefectures)
        towns = []
        for prefecture in prefectures:
            # 取得した都道府県一覧から、町域情報を取得
            params = {}
            params['prefecture'] = prefecture
            json_param = json.dumps(params).encode('utf-8')
            towns_response = requests.get('http://geoapi.heartrails.com/api/json?method=getTowns',data=json_param,headers={'Content-Type': 'application/json'})
            cities = towns_response.json()['response']['location']
            # テスト用コードで新宿区に限定　しかし、そもそも都道府県指定で検索した際の町域情報が足りない。。。無料プランだからか
            #cities = [ testCity for testCity in towns_response.json()['response']['location'] if testCity['city'] == '新宿区']
            #print(cities)
            # townsのLIST内に、取得した都道府県毎の町域辞書を追加する（0～46）
            towns.append(cities)

        townsWeatherKeys = ['prefuctureName', 'cityName', 'townName', 'latitude', 'longitude', 'postalCode', 'date', 'hour', 'weather', 'temperature', 'probPrecip', 'precipitation', 'humidity', 'windBlow', 'windSpeed']
        townsWeathers = []

        # 町域毎の郵便番号を元に、気象情報のスクレイピングを始める
        for town in towns:
            for city in town:
                
                townsWeatherValues = []
                
                city['postal'] = city['postal'][:3] + '-' + city['postal'][3:]
                # print(city['postal'])

                # 郵便番号から該当ページのリンクを踏む
                postalCode = city['postal']
                url = 'https://tenki.jp/search/?keyword=' + postalCode
                browser.get(url)
                
                # 郵便番号をテキストに持つ要素を見つけ、その親要素のaタグから該当郵便番号の気象データページを開くまで！
                postal_elem = browser.find_element_by_xpath("//span[@class='zipcode' and contains(text(), $postalCode)]/parent::a")
                postal_elem.click()
                
                # 1時間毎の気象データページを開く
                hours = browser.find_element_by_class_name('forecast-select-1h')
                hours.find_element_by_css_selector('a').click()
                
                # class="past"が設定されていない最初の情報を取得する
                # ページ表示後特定するclassは、「hour:取得する時間」、「weather:天気」、「temperature:気温」、「prob-precip:降水確率」、「precipitation:降水量」、「humidity:湿度」、「wind-blow:風向」、「wind-speed:風速」
                hour = browser.find_element_by_xpath("//tr[@class='hour']/td/span[not(@class='past')]")
                weather = browser.find_element_by_xpath("//tr[@class='weather']/td/p[not(@class='past')]")
                temperature = browser.find_element_by_xpath("//tr[@class='temperature']/td/span[not(@class='past')]")
                probPrecip = browser.find_element_by_xpath("//tr[@class='prob-precip']/td/span[not(@class='past')]")
                precipitation = browser.find_element_by_xpath("//tr[@class='precipitation']/td/span[not(@class='past')]")
                humidity = browser.find_element_by_xpath("//tr[@class='humidity']/td/span[not(@class='past')]")
                windBlow = browser.find_element_by_xpath("//tr[@class='wind-blow']/td/p[not(@class='past')]")
                windspeed = browser.find_element_by_xpath("//tr[@class='wind-speed']/td/span[not(@class='past')]")
                
                # 地域名称など取得したデータをdictに設定しLISTに格納
                prefuctureName = city['prefecture']
                cityName = city['city']
                townName = city['town']
                longitude = city['x']
                latitude = city['y']
                postalCode = city['postal']
                date = datetime.date.today()
                date = date.strftime('%Y%m%d')
                townsWeatherValues.append(prefuctureName)
                townsWeatherValues.append(cityName)
                townsWeatherValues.append(townName)
                townsWeatherValues.append(latitude)
                townsWeatherValues.append(longitude)
                townsWeatherValues.append(postalCode)
                townsWeatherValues.append(date)
                townsWeatherValues.append(hour.text)
                townsWeatherValues.append(weather.text)
                townsWeatherValues.append(temperature.text)
                townsWeatherValues.append(probPrecip.text)
                townsWeatherValues.append(precipitation.text)
                townsWeatherValues.append(humidity.text)
                townsWeatherValues.append(windBlow.text)
                townsWeatherValues.append(windspeed.text)
                
                townsWeatherDict = dict(zip(townsWeatherKeys, townsWeatherValues))
                
                townsWeathers.append(townsWeatherDict)
        
        # S3バケットの設定
        bucket = 'jdmc2019-weather'
        key = 'weather_' + datetime.now().strftime('%Y-%m-%d-%H-%M-%S') + '.txt'

        # 取得した気象データをjson形式で保存
        #file = open('weather.json', 'w', 1, 'utf-8')
        files = json.dumps(townsWeathers, indent=4, sort_keys=True, separators=(',', ': '))
        #file = json.dump(townsWeathers, ensure_ascii=False, indent=4)

        # 取得した気象データをDynamoDBに一括保存する。
        tablename = "weather"
        table = dynamodb.Table(tablename)
        with table.batch_writer() as batch:
            for file in files:
                batch.put_item(
                    Item={
                        'prefuctureName': file['prefuctureName']

                    }
                )


        obj = s3.Object(bucket,key)
        obj.put(Body=files)

        # 後始末
        browser.close()
        browser.quit()

        return

    except Exception as error:
        LOGGER.error(error)
        raise error
