#parse a JSON strin using json.loads() method : reutrns a dictionary
import json
import urllib
import pandas as pd

#API request to the URL
import sys

if sys.version_info[0] == 3:
    from urllib.request import urlopen # for Python 3.x
else:
    from urllib import urlopen #for Python 2.x

with urlopen("http://api.nobelprize.org/v1/prize.json") as url:
    novel_prize_json_file = url.read()


#decoding to python object
novel_prize_json = json.loads(novel_prize_json_file.decode('utf-8'))


#print(novel_prize_json.keys())

#print(novel_prize_json['prizes'][0])

#print(novel_prize_json['prizes'][0].keys())

#print(json.dumps(novel_prize_json['prizes'][0], indent=4))

#print(json.dumps(novel_prize_json['prizes'][0], indent=4, sort_keys=True))

#novel_prize_physics = pd.DataFrame(novel_prize_json['prizes'][0]["laureates"])

#칼럼 순서를 columns로 지정
novel_prize_physics= pd.DataFrame(novel_prize_json['prizes'][0]["laureates"], columns=['motivation', 'share', 'surname', 'id', 'firstname'])

print(novel_prize_physics)

