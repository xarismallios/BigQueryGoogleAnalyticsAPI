#!flask/bin/python
from flask import Flask
from flask import request, jsonify
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.transport import requests
import json
import csv
from werkzeug.wrappers import Response
from datetime import datetime,timedelta
from io import StringIO
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"


client = bigquery.Client(project = 'myassign-301517')

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello, efooder!"

@app.route('/big-query/order-convertion-rate/<int:days>', methods=['GET'])
def GetConvertionRate(days):
    print('days ', days)
    sql = """
                    SELECT
                    SUM( totals.transactions ) as total_transactions,
                    SUM( totals.visits )  as total_visits
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                    WHERE
                    _TABLE_SUFFIX BETWEEN
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']

    print('---------------- ',data['total_transactions']['0'])

    response = jsonify({
        'status': 'success',
        'convertion_rate': float(total_transactions)/float(total_visits)
        })
    response.status_code = 201
    return response

@app.route('/big-query/order-convertion-rate-previous-period/<int:days>', methods=['GET'])
def GetComparisonConvertionRate(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')
    sql = """
                SELECT
                SUM( totals.transactions ) as total_transactions,
                SUM( totals.visits )  as total_visits
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                _TABLE_SUFFIX BETWEEN
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']

    days2 = int(days)*2
    sql2 = """
        SELECT
        SUM( totals.transactions ) as total_transactions,
        SUM( totals.visits )  as total_visits
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE
        _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY));
    """.format(days2, days)

    df2 = client.query(sql2).to_dataframe()
    data2 = df2.to_json()
    data2 = json.loads(data2)
    total_transactions2 = data2['total_transactions']['0']
    total_visits2 = data2['total_visits']['0']

    print('---------------- ',data['total_transactions']['0'])

    response = jsonify({
        'status': 'success',
        'convertion_rate': float(total_transactions)/float(total_visits),
        'convertion_rate_period_before': float(total_transactions2)/float(total_visits2)
        })
    response.status_code = 201
    return response


@app.route('/big-query/order-convertion-rate/<int:days>/csv', methods=['GET'])
def GetConvertionRateCSV(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')

    sql = """
                SELECT
                SUM( totals.transactions ) as total_transactions,
                SUM( totals.visits )  as total_visits
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                _TABLE_SUFFIX BETWEEN
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']
    convertion_rate = float(total_transactions)/float(total_visits)

    log = [
        ('1', convertion_rate)
    ]

    response = Response(generate(log), mimetype='text/csv')
    # add a filename
    response.headers.set("Content-Disposition", "attachment", filename="convertion_rate.csv")
    return response

@app.route('/big-query/order-convertion-rate-previous-period/device/usertype/<int:days>', methods=['GET'])
def GetComparisonConvertionRateDeviceUserType(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')
    
    sql = """SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate, device.deviceCategory AS device,IF(totals.newVisits IS NOT NULL, "New Visitor", "Returning Visitor") userType,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB("2017-08-01", INTERVAL {} DAY)) AND
        FORMAT_DATE("%Y%m%d", "2017-08-01")
        group by  device,userType
        order by device""".format(days)
    
    given = '01-08-2017'
    date_object = datetime.strptime(given, '%d-%m-%Y').date()
    n_days_ago = date_object - timedelta(days=days)
    datep = n_days_ago.strftime('%d-%m-%Y')
    final = datep + "||"+given
    df = client.query(sql).to_dataframe()
    df['Period']=final
    
    new_date = date_object - timedelta(days=int(days+1))
    print(new_date)
    sql2 = """
        SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate, device.deviceCategory AS device,IF(totals.newVisits IS NOT NULL, "New Visitor", "Returning Visitor") userType,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB("{date}", INTERVAL {D} DAY)) AND
        FORMAT_DATE("%Y%m%d", "{date}")
        group by  device,userType
        order by device
    """.format(**{"D": days, "date": new_date})
    
    date_object2 = datetime.strptime(str(new_date), '%Y-%m-%d').date()
    n_days_ago2 = date_object2 - timedelta(days=days)
    datep2 = n_days_ago2.strftime('%Y-%m-%d')
    final2 = str(new_date)+ "||"+datep2
    df2 = client.query(sql2).to_dataframe()
    df2['Period']=final2
    
    df3 = df.append(df2, ignore_index=True)
    
    return Response(df3.to_json(orient="records"), mimetype='application/json')


@app.route('/big-query/get-user-profile/<int:id>', methods=['GET'])
def GetUserDetails(id):
    print('profile id ', id)
    """List of query browser"""
    print('List all query details')
    # 2248281639583218707
    sql = """
                SELECT *
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE fullVisitorId = "{}";
    """.format(id)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    print(data)
    if data['hits']['0'][0]['transaction']['transactionCoupon'] is None:
        coupon=0
    else :
        coupon=1
    if data['totals']['0']['transactions'] is None:
        hasPurchases=0
    else :
        hasPurchases=1



    product_list = data['hits']['0'][0]['product']
    product_list_arr = []
    for product in product_list:
        product_list_arr.append({
            "productSku": product['productSKU'],
            "productName": product['v2ProductName'],
            "itemRevenue": product['productRevenue'],
            "productQuantity": product['productQuantity']
        })



    response = jsonify({
        'status': 'success',
        'data' : {"pseudoUserId": str(id),
                    "deviceCategory": data['device']['0']['deviceCategory'],
                    "platform": data['device']['0']['operatingSystem'],
                    "dataSource": data['hits']['0'][0]['page']['pagePath'],
                    "hasPurchase": str(hasPurchases),
                    "usedCoupon": str(coupon),
                    "userRevenue": data['totals']['0']['totalTransactionRevenue'],
                    "acquisitionChannelGroup": data['channelGrouping']['0'],
                    "acquisitionSource": data['trafficSource']['0']['source'],
                    "acquisitionMedium": data['trafficSource']['0']['medium'],
                    "acquisitionCampaign": data['trafficSource']['0']['campaign'],
                    "userType": data['socialEngagementType']['0'],
                    "firstSeen": data['visitStartTime']['0'],
                    "lastSeen": data['visitStartTime']['0'],
                    "numberVisits": data['totals']['0']['visits'],
                    "numberPurchases": data['totals']['0']['transactions'],
                    "purchaseActivities": [{
                                "activityTime": data['totals']['0']['timeOnSite'],
                                "channelGrouping": data['channelGrouping']['0'],
                                "source": data['trafficSource']['0']['source'],
                                "medium": data['trafficSource']['0']['medium'],
                                "campaign": data['trafficSource']['0']['campaign'],
                                "landingPagePath": data['hits']['0'][0]['appInfo']['landingScreenName'],
                                "ecommerce": {
                                                "transaction": {
                                                        "transactionId": data['hits']['0'][0]['transaction']['transactionId'],
                                                        "transactionRevenue": data['hits']['0'][0]['transaction']['transactionRevenue'],
                                                        "transactionCoupon": data['hits']['0'][0]['transaction']['transactionCoupon'],
                                                },
                                                "products": product_list_arr
                                            },
                                }],
                }
        })
    response.status_code = 201
    return response

def generate(log):
    data = StringIO()
    w = csv.writer(data)

    # write header
    w.writerow(('No', 'Convertion_Rate'))
    yield data.getvalue()
    data.seek(0)
    data.truncate(0)

    # write each log item
    for item in log:
        w.writerow((
            item[0],
            item[1]  # format datetime as string
        ))
        yield data.getvalue()
        data.seek(0)
        data.truncate(0)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port= 8080, debug=True)
