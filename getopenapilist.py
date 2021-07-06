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
    serviceKey = "dXMdC0X%2BDRPxGEQ2sYQG5xKCJCFzRrcmxMmOq1qrB1RrV9FaWdA8tFGTsRXx1TLIxqEQTKdOWhTmFsYz0qyb%2Fg%3D%3D"
    page = 0
    perPage = 3000
    maxPage = 10
    while(page < maxPage):
        page += 1
        url = "https://api.odcloud.kr/api/15077093/v1/open-data-list?page={0}&perPage={1}&returnType=JSON&serviceKey={2}".format(page, perPage, serviceKey)
        request = urllib.request.Request(url)
        response = get_response(request)
        rescode = response.getcode()
        if(rescode==200):
            try:
                response_body = json.loads(response.read())  # 데이터
                maxPage = response_body['totalCount']//perPage + 1  # maxPage 계산
                data_list = response_body['data']
                df = json_normalize(data_list)  # dict 형식의 데이터 dataframe으로 변환
                df.to_sql("getopenapilist", engine, if_exists='append', index=False, chunksize=1000)  # dataframe형식의 데이터 바로 DB로 넣기
                print("{}/{} 페이지 수집/적재 완료".format(page, maxPage))
            except Exception as e:
                print("{}/{} 페이지 수집/적재 실패".format(page, maxPage))
        else:
            print("{}/{} 페이지 수집/적재 실패".format(page, maxPage))

if __name__ == "__main__":
    main()