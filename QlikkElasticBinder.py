from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# from matplotlib import pyplot as plt
# import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()

elastic_url = os.getenv("ELASTIC_URL")
elastic_user = os.getenv("ELASTIC_USER")
elastic_pass = os.getenv("ELASTIC_PASSWORD")

es = Elasticsearch([elastic_url], basic_auth=(elastic_user, elastic_pass))


def get_elasticsearchdata():
    # elasticsearch query
    query = {
        "query": {
            "wildcard": {
                "pageurl.keyword": "*/account/login*"
            }
        }
    }

    part_of_index = scan(client=es,
                         query=query,
                         index='kpi_metrics_logs_2022.10.20',
                         raise_on_error=True,
                         preserve_order=False,
                         clear_scroll=True
                         )

    result = list(part_of_index)

    temp = []

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
try:
    standarts = df.head(5)
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

    for obj in range(len(df.head(5))):
        tableItem += "<tr><td  style='border-style: solid; border-color: gray;'>{}</td><td  style='border-style: solid; border-color: gray;'>{}</td></tr>".format(
            df['servername'][obj], df['sitename'][obj])

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