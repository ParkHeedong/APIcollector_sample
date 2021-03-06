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
    year = 2018
    while(year < 2022):
        serviceKey = "key"
        pageNo = 0
        maxPage = 10
        while(pageNo < maxPage):
            pageNo += 1
            url = "http://apis.data.go.kr/B552584/UlfptcaAlarmInqireSvc/getUlfptcaAlarmInfo?year={0}&pageNo={1}&numOfRows=100&returnType=json&serviceKey={2}".format(year, pageNo, serviceKey)
            request = urllib.request.Request(url)
            response = get_response(request)
            rescode = response.getcode()
            if(rescode==200):
                try:
                    response_body = json.loads(response.read())
                    maxPage = response_body['response']['body']['totalCount']//100 + 1
                    data_list = response_body['response']['body']['items']
                    df = json_normalize(data_list)
                    df.to_sql("getUlfqtcaAlarmInfo", engine, if_exists='append', index=False, chunksize=1000)
                    print("{0}년도 {1}/{2} 페이지 수집/적재 성공".format(year, pageNo, maxPage))
                except Exception as e:
                    print("{0}년도 {1}/{2} 페이지 수집/적재 실패".format(year, pageNo, maxPage))

            else:
                print("{0}년도 {1}/{2} 페이지 수집/적재 실패".format(year, pageNo, maxPage))
        year += 1

if __name__ == "__main__":
    main()