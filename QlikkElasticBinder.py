from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import math
import pytz

# from matplotlib import pyplot as plt
# import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
moscow_tz = pytz.timezone("Europe/Moscow")
elastic_url = os.getenv("ELASTIC_URL")
elastic_user = os.getenv("ELASTIC_USER")
elastic_pass = os.getenv("ELASTIC_PASSWORD")

es = Elasticsearch([elastic_url], basic_auth=(elastic_user, elastic_pass))


def get_elasticsearchdata():
    # elasticsearch query
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "dissect.Checksum": "UsageDenied"
                        }
                    },
                    {
                        "match": {
                            "dissect.Checksum": "srv_dboardusr"
                        }
                    }
                ],
                "minimum_should_match": 2
            }
        }
    }

    part_of_index = scan(client=es,
                         query=query,
                         index=('dboard-{}'.format(datetime.now().strftime('%Y.%m.%d'))),
                         # index=('dboard-2022.11.02'),
                         raise_on_error=True,
                         preserve_order=False,
                         clear_scroll=True
                         )

    result = list(part_of_index)

    temp = []

    # We need only '_source', which has all the fields required.
    # This elimantes the elasticsearch metdata like _id, _type, _index.
    for hit in result:
        temp.append(hit['_source'])

    df = pd.DataFrame(temp)

    return df


df = get_elasticsearchdata()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)
df.columns.values.tolist()
# print(df.head(5))
# print(df['servername'])
print(df['@timestamp'])
df['@timestamp'] = pd.to_datetime(df['@timestamp'], format='%Y-%m-%dT%H:%M:%S.%fZ')
print(df['@timestamp'])
# delete seconds from timestamp
df['@timestamp'] = df['@timestamp'].dt.strftime('%Y-%m-%d %H:%M')
# print(df['@timestamp'])
print(df['@timestamp'])
now = datetime.now()
current_time = now.strftime(f"%Y-%m-%d %H:%M")
five = now - pd.Timedelta(minutes=5)
five = five.strftime('%Y-%m-%d %H:%M')
print(current_time)
print(five)
# print df['@timestamp'] between 2 dates
condition = (df['@timestamp'] > five) & (df['@timestamp'] < current_time)  # son 5 dk olacak şekilde uyarla
# print(condition)
df['@timestamp'] = df['@timestamp'].loc[condition]
# print(df['@timestamp'])
# print df['@timestamp'] if not null
# df['@timestamp'].remove(np.nan)
print(df['@timestamp'])
# print(df['@timestamp'])
print(len(df['@timestamp']))
if len(df['@timestamp']) < 1:
    exit()
# else:
# print("capacity okey")
try:
    # standarts = df['@timestamp'].loc[condition].dropna()
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Qlik Elastic Binder"
    msg['From'] = os.getenv("FROM_PART")
    msg['To'] = os.getenv("TO_PART")
    smtp = smtplib.SMTP(os.getenv("IP_SMTP"), os.getenv("PORT_SMTP"))

    Content_Title = "Error Loglari asagida belirtilmistir, <br> <hr>"
    Content_End = "<br><br> Bu mail otomatik olarak <b>{}</b> tarihinde gönderilmiştir. <br><br>".format(
        datetime.now().date())

    Mail_Content = Content_Title + """<table><tr><th style="border-style: solid; border-color: black;">Server Name</th><th style="border-style: solid; border-color: black;">Error Log</th></tr>{}</table>"""
    tableItem = ""

    for obj in range(len(df['@timestamp'])):
        tsV = df['@timestamp'][obj]
        if isinstance(tsV, float):
            if math.isnan(df['@timestamp'][obj]):
                continue
        tableItem += "<tr><td  style='border-style: solid; border-color: gray;'>{}</td><td  style='border-style: solid; border-color: gray;'>{}</td></tr>".format(
            df['@timestamp'][obj], df['message'][obj])
    print(len(df['@timestamp']))

    Mail_Content = Mail_Content.format(tableItem)
    Mail_Content += Content_End
    Mail_Content_Part = MIMEText(Mail_Content, 'html')
    msg.attach(Mail_Content_Part)
    smtp.sendmail(os.getenv("FROM_PART"), [os.getenv("TO_PART")], msg.as_string())
    smtp.quit()

    print("Email sent successfully!")
    # lines = ["Responsiblity Checker",
    #         'E-Mail: Sent Successfuly to {}:_{}'.format(os.getenv("PLATFORM_EKIP"), datetime.now().date())]
    # with open('Responsiblity Sender Info_{}.txt'.format(datetime.now().date()), 'w') as f:
    #    for line in lines:
    #        f.write(line)
    #        f.write('\n')
except Exception as ex:
    errorlines = ["Responsiblity Checker",
                  'E-Mail: Sent Failed to {}:_{}'.format(os.getenv("PLATFORM_EKIP"), datetime.now().date())]
    print("Something went wrong....", ex)
    # with open('Responsiblity Sender Info_{}.txt'.format(datetime.now().date()), 'w') as f:
    #    for line in errorlines:
    #        f.write(line)
    #        f.write('\n')