"""
OpenWeatherMapから現在の気象データを取得して表示する。
"""

import boto3
import requests
import json
import datetime
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN


def getWheather(city_code):
    API_KEY = "0fd87b9c9ef2468c0d70b15481b04ec9"
    # api = "http://api.openweathermap.org/data/2.5/weather?units=metric&q={city}&APPID={key}"
    api = "https://api.darksky.net/forecast/{key}/{city}?lang=ja&exclude=hourly,daily,flags"

    url = api.format(city=city_code, key=API_KEY)
    print(url)

    response = requests.get(url)
    # APIレスポンスの表示
    jsonText = json.dumps(response.json(), indent=2)
    print(jsonText)

    return response.json(parse_float=Decimal)


def formatter(city_name, response):
    data = response
    # 表示用時刻
    DIFF_JST_FROM_UTC = 9
    unix = data['currently']['time']
    now = datetime.datetime.fromtimestamp(
        unix) + datetime.timedelta(hours=DIFF_JST_FROM_UTC)

    # 保存するデータを作成
    item = {
        'city_name': city_name,  # プライマリパーティションキー
        'timestamp': data['currently']['time'],   # プライマリソートキー
        'datetime': now.strftime("%Y-%m-%dT%H:%M:%S"),
        'latitude': data['latitude'],
        'longitude': data['longitude'],
        '温度': calcTemperature(data['currently']['temperature']),
        '体感温度': calcTemperature(data['currently']['apparentTemperature']),
        '降水量': data['currently']['precipIntensity'],
        '降水確率': data['currently']['precipProbability'],
        '湿度': data['currently']['humidity'],
        '気圧': data['currently']['pressure'],
        '風速': data['currently']['windSpeed'],
        '雲': data['currently']['cloudCover'],
        'UV指数': data['currently']['uvIndex'],
        'data': data['currently']
    }
    return item


def calcTemperature(fahrenheit):
    tmp = round(float(fahrenheit), 2)
    celsius = (5/9)*(tmp-32)
    return Decimal(str(celsius))


def insert(items):
    # データベース接続の初期化
    """
    session = boto3.session.Session(
        region_name='ap-northeast-1',
        aws_access_key_id='AKIATTDX6S2JJRFCV5RC',
        aws_secret_access_key='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'
    )
    dynamodb = session.resource('dynamodb')
    """
    dynamodb = boto3.resource("dynamodb")

    # テーブルと接続
    table_name = 'darkSkyApp2'
    table = dynamodb.Table(table_name)

    for item in items:
        # 追加する
        response = table.put_item(
            TableName=table_name,
            Item=item
        )
        if response['ResponseMetadata']['HTTPStatusCode'] is not 200:
            # 失敗処理
            print(response)
        else:
            # 成功処理
            print('Successed :', item['city_name'])
    return


def weather_api(cities):
    items = []

    for city_code, city_name in cities.items():
        response = getWheather(city_code)

        '''
        if response['cod'] is not 200:
            print(city_name, ', status code :',
                  response['cod'], response['sys']['message'])
            continue
        '''
        item = formatter(city_name, response)
        print(item)

        items.append(item)

    insert(items)
    return True


def lambda_handler(event, context):
    # 都市と送信先を指定する。
    # OWMで指定可能な都市名は以下で確認する。
    # https://openweathermap.org/weathermap?basemap=map&cities=true&layer=temperature&lat=44.0079&lon=144.2487&zoom=12
    cities = {
        '35.681515, 139.767103': "Tokyo",
        '34.702715, 135.495897': "Osaka-shi",
        '34.794861, 135.449335': "Toyonaka",
        '34.736277, 135.824927': "kizugawa"
    }
    weather_api(cities)

    return


if __name__ == "__main__":
    lambda_handler(None, None)
