#-*- coding:utf-8 -*-
import urllib.request
import json
import time

from sqlalchemy import create_engine
from pandas import json_normalize
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 대기 중..".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패".format(i+1))
                if (i==4):
                    return None

def main():
    baseDt = 1982
    serviceKey = "SEgDr%2FYfqIy3tcVNcNig53XdZI1%2FH4ab1uvtyOvmZscb1FgQqDvCansKw32gueJ75vcmMLPnYK%2FBWKYRTlGKAw%3D%3D"
    while(baseDt < 2022):
        pageNo = 0
        maxPage = 10
        while(pageNo < maxPage):
            pageNo += 1
            url = "http://apis.data.go.kr/1192000/ManageAcst7010Service/getAcst7010List?ServiceKey={}&pageNo={}&numOfRows=10&type=json&baseDt={}".format(serviceKey, pageNo, baseDt)
            request = urllib.request.Request(url)
            response = get_response(request)
            rescode = response.getcode()
            if(rescode == 200):
                try:
                    response_body = json.loads(response.read())
                    maxPage = response_body['responseJson']['header']['totalCount']//10 + 1
                    data_list = response_body['responseJson']['body']['item']
                    df = json_normalize(data_list)
                    df.to_sql("getprocessedproduct", engine, if_exists='append', index='false', chunksize=1000)
                    print("{}년도 {}/{} 페이지 수집 / 적재 성공".format(baseDt, pageNo, maxPage))
                except Exception as e:
                    print("{}년도 {}/{} 페이지 수집 / 적재 실패".format(baseDt, pageNo, maxPage))

            else:
                print("{}년도 {}/{} 페이지 수집 / 적재 실패".format(baseDt, pageNo, maxPage)) 
        baseDt += 1        
if __name__ == "__main__":
    main()