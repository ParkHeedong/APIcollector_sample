#-*- coding:utf-8 -*-
import urllib.request
import json
import time
from numpy import append
import pandas
from datetime import datetime
from datetime import timedelta

from pandas import json_normalize
from pandas.core.dtypes import dtypes
from sqlalchemy import create_engine
from urllib.error import HTTPError

engine = create_engine("postgresql://127.0.0.1:5432/postgres?user=postgres&password=1234")

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
                print("{}차 재시도 성공...".format(i+1))
                return response
            except HTTPError as ex:
                print("{}차 재시도 실패...".format(i+1))
                if(i == 4):
                    return None


def main():
    now = datetime.now()

    #날짜별 데이터 수집하기 위한 날짜 생성
    date_index = pandas.date_range(start='20210601', end=now.strftime("%Y%m%d"))
    date_list = date_index.strftime("%Y%m%d").tolist()

    start_dt_str = '20210101'
    end_dt_str = '20210102'

    #한번에 조회되는 날짜가 최대 7일이므로 시작일자 종료일자 재생성(url)
    while(datetime.strptime(end_dt_str, "%Y%m%d") <= now):
        end_dt = datetime.strptime(start_dt_str, "%Y%m%d") + timedelta(days=6)
        end_dt_str = end_dt.strftime("%Y%m%d")

        pageNo = 0
        maxPage = 10
        serviceKey = "SEgDr%2FYfqIy3tcVNcNig53XdZI1%2FH4ab1uvtyOvmZscb1FgQqDvCansKw32gueJ75vcmMLPnYK%2FBWKYRTlGKAw%3D%3D"

        while(pageNo < maxPage):
            pageNo += 1
            url = "http://apis.data.go.kr/1192000/openapi/service/ManageAcst0110Service/getAcst0110List?ServiceKey={}&pageNo={}&numOfRows=100&fromDt={}&toDt={}&type=json".format(serviceKey, pageNo, start_dt_str, end_dt_str)
            request = urllib.request.Request(url)
            response = get_response(request)
            rescode = response.getcode()
            if(rescode==200):
                try:
                    response_body = json.loads(response.read())
                    maxPage = response_body['responseJson']['header']['totalCount']//100 + 1
                    data_list = response_body['responseJson']['body']['item']
                    df = json_normalize(data_list)
                    df.to_sql("getAcstList", engine, if_exists='append', index=False, chunksize=1000)
                    print("{}/{} 페이지 수집/적재 성공".format(pageNo, maxPage))
                except Exception as ex:
                    print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))
            else:
                print("{}/{} 페이지 수집/적재 실패".format(pageNo, maxPage))

    #날짜는 바뀌면 안되므로 한단계 밖 for문으로 꺼냄
    #시작일자 = 종료일자 + 1 (다음 날 계산하기 위함)
    start_dt = end_dt + timedelta(days=1)
    start_dt_str = start_dt.strftime("%Y%m%d")
    
if __name__ == "__main__":
    main()
