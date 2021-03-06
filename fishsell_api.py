#-*- coding:utf-8 -*-
from numpy import insert
import psycopg2
import urllib.request
import urllib
import json
import time
import pandas
from datetime import datetime
from datetime import timedelta

#Try to connect DB
try:
    conn = psycopg2.connect(host="127.0.0.1", dbname="postgres", user="postgres", password="1234")

    cur=conn.cursor()
    cur.execute("SELECT * FROM fishsell;")
    rows = cur.fetchall()

except:
    print("Not Connected!")

#인증키
encodingkey = "key"

now = datetime.now()

#날짜별 데이터 수집하기 위한 날짜 생성
date_index = pandas.date_range(start='20210601', end=now.strftime("%Y%m%d"))#최신날짜 업데이트 되는 구문 추가
date_list = date_index.strftime("%Y%m%d").tolist()

#기간검색 시작일자와 종료일자 계산
start_dt_str = '20210101'
end_dt_str = '20210102'

#한번에 조회되는 날짜가 최대 7일이므로 시작일자 종료일자 재생성(url)
while(datetime.strptime(end_dt_str, "%Y%m%d") <= now):
    end_dt = datetime.strptime(start_dt_str, "%Y%m%d") + timedelta(days=6)
    end_dt_str = end_dt.strftime("%Y%m%d") #url에서 읽을 수 있게 type을 string으로 바꿔줌

    #페이지 계산을 위한 초기값(아무거나)
    pageNo = 1
    maxPage = 10

    #현재 페이지가 총 페이지 수를 벗어나지 않는 동안 적재
    while(pageNo < maxPage):

        #request url 정의
        url = "http://apis.data.go.kr/1192000/openapi/service/ManageAcst0110Service/getAcst0110List?ServiceKey={0}&pageNo={1}&numOfRows=100&fromDt={2}&toDt={3}&type=json".format(encodingkey, pageNo, start_dt_str , end_dt_str)
        request = urllib.request.Request(url)

        #request 보내기 (header 정보를 포함한 request 객체를 전달)
        response = urllib.request.urlopen(request)

        #결과 코드 정의 (getcode 메소드를 통해서 response의 HTTP status code를 확인)
        rescode = response.getcode()

        #정상 호출인 경우 utf-8 형식으로 디코딩
        if(rescode==200):
            response_body = response.read().decode('utf-8')

        else:
            print("Error Code:" + rescode)

        #딕셔너리 구조로 파싱
        jsonobj = json.loads(response_body)
        
        #totalcount 구하기
        totalcount = jsonobj['responseJson']['header']['totalCount']
        
        #마지막 페이지 계산
        maxPage = (totalcount // 100) + 1

        #responseJson/body/item부터 수집
        items = jsonobj['responseJson']['body']['item']
        
        for item in items:
            
            #쿼리 지정
            #query = "INSERT INTO fishsell(csmtDe, mxtrCode, mxtrNm, csmtmktCode, csmtmktNm, mprcStdCode, mprcStdCodeNm, kdfshSttusCode, kdfshSttusNm, fshrNm,  csmtQy, csmtWt, csmtUntpc, csmtAmount, goodsStndrdNm, goodsUnitNm) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            query = "INSERT INTO fishsell VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

            #items(딕셔너리)를 배열로 넣어줘야한다.
            #배열 생성 및 주입
            items_arr = []

            for column_nm in item:
                items_arr.append(item[column_nm])       
            cur.execute(query, items_arr)
            conn.commit()

        print('{} 적재 성공'.format(start_dt_str))

        #url에 들어가는 pageNo 재생성
        pageNo += 1

    #날짜는 바뀌면 안되므로 한단계 밖 for문으로 꺼냄
    #시작일자 = 종료일자 + 1 (다음 날 계산하기 위함)
    start_dt = end_dt + timedelta(days=1)
    start_dt_str = start_dt.strftime("%Y%m%d")    