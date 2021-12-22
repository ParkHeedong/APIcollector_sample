#-*- coding:utf-8 -*-
import urllib.request
import json
import time
import urllib.parse

from sqlalchemy import create_engine
from pandas import json_normalize
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

ocean_nm = urllib.parse.quote("서해")
stnpnt_korean_nm = urllib.parse.quote("새만금")

def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 대기 중...".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공..".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패..".format(i+1))
                if (i==4):
                    return None
def main():
    pageNo = 0
    maxPage = 10
    serviceKey = "key"
    while(pageNo < maxPage):
        pageNo += 1
        url = "http://apis.data.go.kr/B551979/service/OceansWemoService/getOceansWemo?pageNo={}&numOfRows=10&resultType=json&OCEAN_NM={}&STNPNT_KOREAN_NM={}&sdate=20180101&edate=20180130&ServiceKey={}".format(pageNo, ocean_nm, stnpnt_korean_nm, serviceKey)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode == 200):
            try:
                response_body = json.loads(response.read())
                maxPage = response_body['getOceansWemo']['totalCount'] // 10 + 1
                data_list = response_body['getOceansWemo']['item']
                df = json_normalize(data_list)
                df.to_sql("getOceansWemo", engine, if_exists='append', index=False, chunksize=1000)
                print("{}/{} 페이지 수집 / 적재 성공".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))
        
        else:
            print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))
if __name__: "__main__"
main()