#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import csv
from datetime import datetime
import os
import sys

import openapi
import zipfile
import shutil
import shapefile
from shapely.geometry import shape


#해양수산정보 공동활용체계

exitcode = 0

def getFilename_txt(opaque):
    global file_list_txt
    file_list = os.listdir(opaque) 
    if not file_list:
        # 디렉터리에 파일이 없어도 배치결과 'success'로 남기기 위한 핸들링
        print("작업 대상이 없음")
        conf = openapi.config()
        batch_date = sys.argv[1]
        cnnc_manage_no = openapi.connection(sys.argv[2])
        table_eng_nm_list = openapi.tables(cnnc_manage_no, sys.argv[3:])
        tablename = list(table_eng_nm_list.keys()).pop(0)
        table_eng_nm = table_eng_nm_list[tablename]
        jdbc = openapi.jdbc2json.new(conf, cnnc_manage_no)
        hive = openapi.jdbc2json.new(conf, conf["hive"], cnnc_manage_no)
        java = openapi.javagate.new(conf)
        openapi.begin_dt(table_eng_nm)
        openapi.results[table_eng_nm["TABLE_ENG_NM"]]["MD5"] = {}
        openapi.results[table_eng_nm["TABLE_ENG_NM"]]["Records"] = 0
        openapi.end_dt(table_eng_nm)
        contents = None
        openapi.print_results()
        sys.exit(0)
    else :
        file_list_txt =[file for file in file_list if file.endswith(".txt")]
        return file_list_txt

def getFilename_shp(opaque):
    global file_list_txt
    file_list = os.listdir(opaque) 
    if not file_list:
        # 디렉터리에 파일이 없어도 배치결과 'success'로 남기기 위한 핸들링
        print("작업 대상이 없음")
        conf = openapi.config()
        batch_date = sys.argv[1]
        cnnc_manage_no = openapi.connection(sys.argv[2])
        table_eng_nm_list = openapi.tables(cnnc_manage_no, sys.argv[3:])
        tablename = list(table_eng_nm_list.keys()).pop(0)
        table_eng_nm = table_eng_nm_list[tablename]
        jdbc = openapi.jdbc2json.new(conf, cnnc_manage_no)
        hive = openapi.jdbc2json.new(conf, conf["hive"], cnnc_manage_no)
        java = openapi.javagate.new(conf)
        openapi.begin_dt(table_eng_nm)
        openapi.results[table_eng_nm["TABLE_ENG_NM"]]["MD5"] = {}
        openapi.results[table_eng_nm["TABLE_ENG_NM"]]["Records"] = 0
        openapi.end_dt(table_eng_nm)
        contents = None
        openapi.print_results()
        sys.exit(0)
    else :
        file_list_txt =[file for file in file_list if file.endswith(".zip")]
        return file_list_txt

def getList_txt(jdbc, hive, java, conf, batch_date, cnnc_manage_no, table_eng_nm, opaque):

    getFilename_txt(opaque)
    datelist = []

    # 디렉터리 내에 있는 파일에서 날짜만 list에 append
    for txt in file_list_txt:
        datelist.append(txt[0:8])
        date = txt[0:8]
        sDate = date[0:4] + "-" + date[4:6] + "-" + date[6:8]

        # 해당 날짜로 파티션 생성
        stdDate = datetime.strptime(sDate, "%Y-%m-%d")
        partitionDate = stdDate.strftime("%Y%m%d")
        print("partitionDate: {0}".format(partitionDate), flush=True)
        partitionName = "{0}000000".format(partitionDate)
        partitions = openapi.get_partitions(hive, cnnc_manage_no, table_eng_nm)
        
        # 파티션 유무에 따른 수집 여부 결정
        if partitionName in partitions:
            print("파티션이 있음: {0}".format(partitionName),
                  file=sys.stderr, flush=True)
            continue

        if txt.startswith(date):

            # 입출력 파일 준비
            filename = os.path.join(opaque, "{0}".format(txt))

            print("파일有: {0}".format(filename), flush=True)

            file = open(filename, "rt", encoding="euc-kr")
            reader = csv.reader(file)
            header = next(reader)
            writer = openapi.hiveopen(jdbc, hive, java, conf, partitionName,
                                       cnnc_manage_no, table_eng_nm)

            # 파일 데이터 가공 및 수집
            for record in reader:
                element = []
                for column in table_eng_nm["COLUMNS"]:
                    if column["DB_TABLE_ATRB_SN"] is not None:
                        value = record.pop(0)
                        if isinstance(value, str):
                            value = value.strip()
                        if column["HIVE_ATRB_TY_NM"] == "DECIMAL":
                            value = float(value)
                        if column["HIVE_ATRB_TY_NM"] == "DATE":
                            value = datetime.datetime.strptime(
                                value, "%Y-%m-%d").date()
                        element.append(value)
                writer.writerecord(element)

            # 출력 파일 정리
            writer.close()
            file.close()
    #적재 후 작업 디렉터리 삭제
    #shutil.rmtree("/home/hadoop/datalake-collector/test/test5")
    #print("작업 디렉터리 삭제 완료 : {0}".format(opaque))

def getList_shp(jdbc, hive, java, conf, batch_date, cnnc_manage_no, table_eng_nm, opaque):

    getFilename_shp(opaque)
    datelist = []
        
    # 디렉터리 내에 있는 파일에서 날짜만 list에 append
    for txt in file_list_txt:
        datelist.append(txt[0:8])
        date = txt[0:8]

        sDate = date[0:4] + "-" + date[4:6] + "-" + date[6:8]

        # 해당 날짜로 파티션 생성
        stdDate = datetime.strptime(sDate, "%Y-%m-%d")
        partitionDate = stdDate.strftime("%Y%m%d")
        print("partitionDate: {0}".format(partitionDate), flush=True)
        partitionName = "{0}000000".format(partitionDate)
        batch_date = partitionName

        partitions = openapi.get_partitions(hive, cnnc_manage_no, table_eng_nm)
        
        # 파티션 유무에 따른 수집 여부 결정
        if partitionName in partitions:
            print("파티션이 있음: {0}".format(partitionName),
                  file=sys.stderr, flush=True)
            continue

        # 압축해제
        with zipfile.ZipFile(opaque + "/" + txt, 'r') as zip_ref:
            zip_ref.extractall(opaque)

        if txt.startswith(date):

            file_list = os.listdir(opaque)
            shpfile = [file for file in file_list if file.endswith(".shp")]

            shpFnm = opaque + "/{0}".format(shpfile[0])
            sf = shapefile.Reader(shpFnm, encoding="euc-kr")

            shapeRecords = sf.shapeRecords()
            contents = []

            for i in shapeRecords:
                new_items = []

                recDict = i.record.as_dict()
                geoData = json.loads(json.dumps(i.shape.__geo_interface__))
                recDict['geom'] = shape(geoData).wkt   

                new_items.append(recDict)

                # 레코드별로 가져오기
                for item in new_items:
                    #print("START INSERT TO LIST GID : {0}".format(item['gid']))
                    element = []
                    # 컬럼별로 가져오기
                    for column in table_eng_nm["COLUMNS"]:
                        if column["DB_TABLE_ATRB_SN"] is not None:
                            if column["TABLE_ATRB_ENG_NM"] in item:
                                value = item[column["TABLE_ATRB_ENG_NM"]]
                                if isinstance(value, str):
                                    value = value.strip()
                                element.append(value)
                            else:
                                element.append(None)
                    contents.append(element)
            print(".", end="", file=sys.stdout, flush=True)
            print("", file=sys.stdout, flush=True)

            # Hive에 연도별 파티션 생성 후 레코드 적재
            if openapi.hivestore(jdbc, hive, java, conf, batch_date, cnnc_manage_no, contents, table_eng_nm, partition=partitionName) != 0:
                print("파티션 저장 실패: {0}".format(partitionName),
                  file=sys.stderr, flush=True)

            #적재 된 파일 삭제 (중복 파일 적재 피함)
            file_list = os.listdir(opaque)
            remove_file_list_txt =[file for file in file_list if file.startswith("wgis")]
            for remove_file in remove_file_list_txt:
                os.remove(opaque + "/{0}".format(remove_file))
            #openapi.hiveopen(jdbc, hive, java, conf, partitionName,
            #                           cnnc_manage_no, table_eng_nm)
            #writer.close()
            #return contents

            # 적재 후 파일 옮기기
            #shutil.move(filename, os.path.join(os.environ["DATALAKE_COLLECTOR"], "var/sftp/files")) #로컬위치
            #shutil.move(filename, "/data/mogaha/file/recieve_old/CG119202403066/KHSUGP") #로컬위치
    # 적재 후 작업 디렉터리 삭제
    #shutil.rmtree("/home/hadoop/datalake-collector/test/test5")
    #print("작업 디렉터리 삭제 완료 : {0}".format(opaque))

handlers = {
    "napi_mof_014": {
        "wgis_mei_memost": {"handler": getList_shp, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGA"},
        "wgis_mei_wemo_cd": {"handler": getList_shp, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGB"},
        "wgis_mei_memoofs": {"handler": getList_shp, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGE"},
        "wgis_mei_wemo_recd": {"handler": getList_shp, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGC"},
        "omo_wthr": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUNH"},
        "tb_mei_atmcrm_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGK"},
        "tb_mei_memocoast_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGG"},
        "tb_mei_memoemsa_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGH"},
        "tb_mei_memohbr_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGF"},
        "tb_mei_memoofsst_bas": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGM"},
        "tb_mei_memorm_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGI"},
        "tb_mei_oobatmc_inf": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGJ"},
        "fnd_vhfdsc_20201020_m": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGN"},
        "fnd_vhfdsc_dsast_m": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGP"}
        #"fnd_vhfdsc_info_m": {"handler": getList_txt, "opaque": "/home/hadoop/datalake-collector/doc/dat/mogaha/file/recieve/CG119202403066/KHSUGO"}, #한글깨짐(utf-8)
        #"tm_vessel_info": {"handler": getList, "opaque": "doc/dat/tm_vessel_info.csv"},
        #"th_ais_yyyymmdd": {"handler": getList, "opaque": "doc/dat/th_ais_20200109.csv"},
        #"th_vpass_info": {"handler": getList, "opaque": "doc/dat/th_vpass_info.csv"},
        #"th_vpass_yyyymmdd": {"handler": getList, "opaque": "doc/dat/th_vpass_20200109.csv"},    
        #"tb_daly_ecdt_bas": {"handler": getList, "opaque": "doc/dat/tb_daly_ecdt_bas.csv"},     
        #"tb_mei_atmcship_inf": {"handler": getList, "opaque": "doc/dat/tb_mei_atmcship_inf.csv"},
        #"tb_psgs_qy_daly_bas": {"handler": getList, "opaque": "doc/dat/tb_psgs_qy_daly_bas.csv"},

    }
}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("사용법: {0} {1} {2}".format(
            os.path.basename(sys.argv[0]), "{배치기준시간}", "{시스템명}"), flush=True)
        sys.exit(1)
    openapi.main(handlers)
