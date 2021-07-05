#-*- coding:utf-8 -*-
import psycopg2
import urllib.request
import urllib
import bs4
import json


#Try to connect DB
try:
    conn = psycopg2.connect(host="127.0.0.1", dbname="postgres", user="postgres", password="1234")

    cur=conn.cursor()
    cur.execute("select * from testdb;")
    rows = cur.fetchall()

except:
    print("Not Connected!")

encodingKey = "dXMdC0X%2BDRPxGEQ2sYQG5xKCJCFzRrcmxMmOq1qrB1RrV9FaWdA8tFGTsRXx1TLIxqEQTKdOWhTmFsYz0qyb%2Fg%3D%3D"  #인코딩 인증키

#시도별 데이터 수집하기 위한 배열 선언
sido_arr = ['서울', '부산', '대구', '인천', '광주', '울산', '세종', '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']

#url에 한글이 들어가는 경우 인코딩(ex: 제주 -> %EC%A0%9C%EC%A3%BC)
for sido_column in sido_arr:
    sido = urllib.parse.quote(sido_column)


    #request url 정의
    url = "http://apis.data.go.kr/1192000/service/OceansBeachInfoService1/getOceansBeachInfo1?pageNo=1&numOfRows=10&resultType=json&SIDO_NM={0}&ServiceKey={1}".format(sido, encodingKey)
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
    print(jsonobj)
    
    #getOceansBeachInfo 밑에 item부터 수집
    items = jsonobj['getOceansBeachInfo']['item']
    for item in items:
    
        #보통 쿼리를 따로 지정, 자동 포맷 %s
        query = "INSERT INTO testdb(num, sidoNm, gugunNm, staNm, beachWid, beachLen, beachKnd, linkAddr, linkNm, linkTel, lat, lon) VALUES(%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        #items(딕셔너리)를 배열로 넣어줘야한다.

        #배열 생성 및 주입
        items_arr = []
        
        for column_nm in item:
            if (column_nm != 'beach_img'):
                items_arr.append(item[column_nm])
                print(items_arr)
        #하나의 item마다 쿼리문 수행
        cur.execute(query, items_arr)
        conn.commit()




