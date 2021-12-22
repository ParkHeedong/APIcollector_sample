#-*- coding:utf-8 -*-
from os import error
import psycopg2
import urllib.request
import json
import time
import xmltodict

from pandas import json_normalize
from sqlalchemy import create_engine
from urllib.error import HTTPError

engine = create_engine("postgresql://localhost:35432/postgres?user=postgres&password=1234")

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

    serviceKey = "key"
    
    url = "http://apis.data.go.kr/1192000/apVhdService_OpzEc/getOpnOpzEcWFS?serviceKey={}&bbox=1114089.46348226,1629184.00080454,1153802.53371555,1684626.75837843&maxFeatures=10&ntfc_year=2020&area_id=0006".format(serviceKey)
    request = urllib.request.Request(url)
    response = get_response(request)
    rescode = response.getcode()
    if(rescode==200):
        try:
            response_xml = json.dumps(xmltodict.parse(response), ensure_ascii=False)
            response_body = json.loads(response_xml)
            data_list = response_body['wfs:FeatureCollection']['gml:featureMembers']['ofbd-DB:opn_os_prpos_zone_ec']
            data_list_two = response_body['wfs:FeatureCollection']['gml:featureMembers']['ofbd-DB:opn_os_prpos_zone_ec']['gml:MultiSurface']
            print(data_list_two) 
            df = json_normalize(data_list)
            print(df)
            df.to_sql("getOpnOpzEcWFS", engine, if_exists='append', index=False, chunksize=1000)
            print("수집 / 적재 성공")
        except Exception as e:
            print(error)
    else:
        print("수집 / 적재 실패")
if __name__ : "__main__"
main()