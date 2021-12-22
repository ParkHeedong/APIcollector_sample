import urllib.request
import json
import time
import urllib.parse

from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

biz_nam = urllib.parse.quote("현대")

def get_response(request):
    try:
        response = urllib.request.urlopen(request)
        return response
    except HTTPError as ex:
        for i in range(5):
            print("{}차 재시도 전 대기중...".format(i+1))
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
        url = "http://apis.data.go.kr/1192000/service/OceansUseService2/getOceansUseInfo2?serviceKey={}&resultType=json&pageNo={}&numOfRows=10&ACP_YEAR=2013&BIZ_NAM={}".format(serviceKey, pageNo, biz_nam)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode == 200):
            try:
                response_body = json.loads(response.read())
                maxPage = response_body['getOceansUseInfo']['totalCount'] // response_body['getOceansUseInfo']['numOfRows'] + 1
                date_list = response_body['getOceansUseInfo']['item']
                df = json_normalize(date_list)
                df.to_sql("getOceansUseInfo", engine, if_exists='append', index=False, chunksize=1000)
                print("{}/{}페이지 수집 / 적재 성공".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{}페이지 수집 / 적재 실패".format(pageNo, maxPage))
        else:
            print("{}/{}페이지 수집 / 적재 실패".format(pageNo, maxPage))

if __name__ : "__main__"
main()