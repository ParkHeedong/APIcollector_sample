#-*- coding:utf-8 -*-
import urllib.request
import json
import time

from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

# 데이터 수집, 재시도 5회 코드 포함
def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 60초 대기 중...".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공...".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패...".format(i+1))
                if(i==4):
                    return None

def main():
    serviceKey = "SEgDr%2FYfqIy3tcVNcNig53XdZI1%2FH4ab1uvtyOvmZscb1FgQqDvCansKw32gueJ75vcmMLPnYK%2FBWKYRTlGKAw%3D%3D"
    pageNo = 0
    maxPage = 10
    while(pageNo < maxPage):
        pageNo += 1
        url = "http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo?year=2018&pageNo={0}&numOfRows=100&returnType=json&serviceKey={1}".format(pageNo, serviceKey)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode==200):
            try:
                response_body = json.loads(response.read())
                maxPage = response_body['response']['body']['totalCount']//100
                data_list = response_body['response']['body']['items']
                df = json_normalize(data_list)
                df.to_sql("getUlfqtcaAlarmInfo", engine, if_exists='append', index='false', chunksize=1000)
                print("{}/{} 페이지 수집/적재 성공".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))
                
        else:
            print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))
           
if __name__ == "__main__":
    main()