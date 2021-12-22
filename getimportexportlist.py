#-*- coding:utf-8 -*-
import psycopg2
import urllib.request
import json
import time

from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

# 데이터 수집, 재시도 5회코드 포함
def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 60초 대기중...".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공...".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패...".format(i+1))
                if (i==4):
                    return None

def main():
    serviceKey = "key"
    pageNo = 0
    maxPage = 10
    while(pageNo < maxPage):
        pageNo += 1
        url = "http://apis.data.go.kr/1192000/openapi/service/ManageExpNationItemService/getExpNationItemList?ServiceKey={0}&baseDt=201712&pageNo={1}&numOfRows=100&type=json".format(serviceKey, pageNo)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode==200):
            try:
                response_body = json.loads(response.read()) #데이터
                maxPage = response_body['responseJson']['header']['totalCount']//100 + 1 #maxPage 계산
                data_list = response_body['responseJson']['body']['item'] 
                df = json_normalize(data_list) #dict 형식의 데이터 dataframe 형식으로 변환(테이블)
                df.to_sql("getimportexportlist", engine, if_exists='append', index=False, chunksize=1000) #dataframe 형식의 데이터 DB로 적재
                print("{}/{} 페이지 수집/적재 완료".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))
        else:
            print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))

if __name__ == "__main__":
    main()