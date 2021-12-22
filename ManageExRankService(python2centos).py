#-*- coding:utf-8 -*-
from urllib import response
import urllib.request
import json
import time
from urllib import parse

from sqlalchemy import create_engine
from pandas import json_normalize
from urllib.error import HTTPError

from sqlalchemy.sql.expression import false

engine = create_engine("postgresql://192.168.2.132:5432/?user=postgres&password=postgres")

def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 대기중".format(i+1))
            time.sleep(60)
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패".format(i+1))
                if(i==4):
                    return None
def main():
    pageNo = 0
    maxPage = 10
    serviceKey = "key"
    while(pageNo < maxPage):
        pageNo += 1
        url = "http://apis.data.go.kr/1192000/openapi/service/ManageExpRankService/getExpRankList?ServiceKey={}&pageNo={}&numOfRows=10&type=json&baseDt=201510".format(serviceKey, pageNo)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode==200):
            try:
                response_body = json.loads(response.read())
                maxPage = response_body['responseJson']['header']['totalCount'] // response_body['responseJson']['header']['numOfRows'] + 1
                data_list = response_body['responseJson']['body']['item']
                df = json_normalize(data_list)
                df.to_sql("ManageExRankService", engine, if_exists='append', index=False, chunksize=1000)
                print("{}/{} 페이지 수집 / 적재 성공".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))
        else:
            print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))

if __name__ : "__main__"
main()