# APIcollector_sample/fishsell_api.py 

## 라이브러리 및 사용 예시



1. *시작 날짜와 종료날짜까지 매일의 날짜 생성*  			***date_range***()

   *시간을 문자열로 출력*													***strftime()***

   

   `import pandas`

   

   `date_index = pandas.date_range(' start = '20210601' ,  end = '20210615' )`
   
   `date_list = date_index.strftime("%Y%m%d").tolist()`

### ![image-20210615170642325](C:\Users\User\AppData\Roaming\Typora\typora-user-images\image-20210615170642325.png)



------



2. *문자열을 시간으로 출력*		***strptime()***

   *지정한 날짜대로 날짜 생성*  	***timedelta()***

   

   `import datetime`

   `from datetime import timedelta`

   

   `end_dt_str = '20210102'` <!--string타입-->

   

   `while(datetime.strptime(end_dt_str, "%Y%m%d") <= now):`

     `end_dt = datetime.strptime(start_dt_str, "%Y%m%d") + timedelta(days=6)`
   
     `end_dt_str = end_dt.strftime("%Y%m%d")`



------



3. *문자열 포맷팅*		***format()***

   

   ![image-20210615180342878](C:\Users\User\AppData\Roaming\Typora\typora-user-images\image-20210615180342878.png)



------



4. *데이터베이스 연결* 				***PostgreSQL***

   

   `\#Try to connect DB`

   `try:`

     `conn = psycopg2.connect(host="127.0.0.1", dbname="postgres", user="postgres", password="1234")`

   

     `cur=conn.cursor()`

     `cur.execute("SELECT * FROM fishsell;")`

     `rows = cur.fetchall()`

   

   `except:`

     `print("Not Connected!")`

   

   ------

   

5.  URL을 열기 위한 확장 라이브러리         ***urllib.request()***

   

    `\#request url 정의`

   ​    `url = "http://apis.data.go.kr/1192000/openapi/service/ManageAcst0110Service/getAcst0110List?ServiceKey={0}&pageNo={1}&numOfRows=100&fromDt={2}&toDt={3}&type=json".format(encodingkey, pageNo, start_dt_str , end_dt_str)`

   ​    `request = urllib.request.Request(url)`

   

   ​    `\#request 보내기 (header 정보를 포함한 request 객체를 전달)`

   ​    `response = urllib.request.urlopen(request)`

   

   ​    `\#결과 코드 정의 (getcode 메소드를 통해서 response의 HTTP status code를 확인)`

   ​    `rescode = response.getcode()`

   

   ​    `\#정상 호출인 경우 utf-8 형식으로 디코딩`

   ​    `if(rescode==200):`

   ​      `response_body = response.read().decode('utf-8')`

   

   ​    `else:`

   ​      `print("Error Code:" + rescode)`

   

------



