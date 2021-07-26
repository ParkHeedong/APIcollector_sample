import urllib.request
import json
import time
import urllib.parse
import xmltodict

from pandas import json_normalize
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
                print("{}회 재시도 성공".format(i+1))
                return response
            except HTTPError as ex:
                print("{}회 재시도 실패".format(i+1))
                if(i==4):
                    return None
def main():
    pageNo = 0
    maxPage = 10
    serviceKey = "SEgDr%2FYfqIy3tcVNcNig53XdZI1%2FH4ab1uvtyOvmZscb1FgQqDvCansKw32gueJ75vcmMLPnYK%2FBWKYRTlGKAw%3D%3D"
    while(pageNo < maxPage):
        pageNo += 1
        url = "http://apis.data.go.kr/1192000/DgstInqire3/Info?serviceKey={}&stdt=2014&pageNo={}&numOfRows=50".format(serviceKey, pageNo)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode==200):
            try:
                response_xml = json.dumps(xmltodict.parse(response), ensure_ascii=False)
                response_body = json.loads(response_xml)
                maxPage = int(response_body['response']['body']['totalCount']) // 50 + 1
                data_list = response_body['response']['body']['items']['item']
                df = json_normalize(data_list)
                df.to_sql("getDangerInfo", engine, if_exists='append', index=False, chunksize=1000)
                print("{}/{} 페이지 수집 / 적재 성공".format(pageNo, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))
        else:
            print("{}/{} 페이지 수집 / 적재 실패".format(pageNo, maxPage))

if __name__:"__main__"
main()