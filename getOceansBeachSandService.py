#-*- coding:utf-8 -*-
from urllib import response
import urllib.request
import json
import time
from urllib import parse

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
            time.sleep(60)
            print("{}차 재시도 전 대기중...".format(i+1))
            try:
                response = urllib.request.urlopen(request)
                print("{}차 재시도 성공".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패".format(i+1))
                if(i==4):
                    return None

def main():
    sido_list = ['부산', '인천', '울산', '강원', '충남', '전북', '전남', '경북', '경남', '제주']

    for sidonm in sido_list:
        sido = urllib.parse.quote(sidonm)

        pageNo = 0
        maxPage = 10
        serviceKey = "SEgDr%2FYfqIy3tcVNcNig53XdZI1%2FH4ab1uvtyOvmZscb1FgQqDvCansKw32gueJ75vcmMLPnYK%2FBWKYRTlGKAw%3D%3D"
        while(pageNo < maxPage):
            pageNo += 1
            url = "http://apis.data.go.kr/1192000/service/OceansBeachSandService1/getOceansBeachSandInfo1?pageNo={}&numOfRows=10&resultType=json&SIDO_NM={}&RES_YEAR=2016&ServiceKey={}".format(pageNo, sido, serviceKey)
            request = urllib.request.Request(url)
            response = get_response(request)
            rescode = response.getcode()
            if(rescode==200):
                try:
                    response_body = json.loads(response.read())
                    maxPage = response_body['getOceansBeachSandInfo']['totalCount'] // response_body['getOceansBeachSandInfo']['numOfRows'] + 1
                    data_list = response_body['getOceansBeachSandInfo']['item']
                    df = json_normalize(data_list)
                    df.to_sql("getOceansBeachSandInfo", engine, if_exists='append', index=False, chunksize=1000)
                    print("{}시 {}/{}페이지 수집 / 적재 성공".format(sidonm, pageNo, maxPage))
                except Exception as e:
                    print("{}시 {}/{}페이지 수집 / 적재 실패".format(sidonm, pageNo, maxPage))
            else:
                print("{}시 {}/{}페이지 수집 / 적재 실패".format(sidonm, pageNo, maxPage))

if __name__ : "__main__"
main()