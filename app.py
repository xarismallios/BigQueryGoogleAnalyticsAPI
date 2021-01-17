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


@app.route('/report/cvr/<int:days>', methods=['GET'])
def GetConversionRate(days):
    sql = """
                    SELECT
                    SUM( totals.transactions )/SUM( totals.visits ) as cvr
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                    WHERE
                    _TABLE_SUFFIX BETWEEN
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL {} DAY)) AND
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL 0 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    cvr = data['cvr']['0']

    given = '02-08-2017'
    date_object = datetime.strptime(given, '%d-%m-%Y').date()
    n_days_ago = date_object - timedelta(days=days)
    n_days_ago2 = date_object - timedelta(days=1)
    dates= n_days_ago2.strftime('%d-%m-%Y')
    datep = n_days_ago.strftime('%d-%m-%Y')
    final = datep + "||" + dates

    response = jsonify({
        'status': 'success',
        'conversion_rate': float(cvr),
        'period': str(final)
    })
    response.status_code = 201
    return response

@app.route('/report/cvr/compare-periods/<int:days>', methods=['GET'])
def GetComparisonConvertionRatePeriod(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')

    sql = """SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL {} DAY)) AND
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL 1 DAY));""".format(days)

    given = '02-08-2017'
    date_object = datetime.strptime(given, '%d-%m-%Y').date()
    n_days_ago = date_object - timedelta(days=days)
    n_days_ago2 = date_object - timedelta(days=1)
    dates= n_days_ago2.strftime('%d-%m-%Y')
    datep = n_days_ago.strftime('%d-%m-%Y')
    final = datep + "||" + dates

    df = client.query(sql).to_dataframe()
    df['Period'] = final

    new_date = n_days_ago
    print(new_date)
    sql2 = """
        SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB("{date}", INTERVAL {D} DAY)) AND
        FORMAT_DATE("%Y%m%d",DATE_SUB("{date}", INTERVAL 1 DAY))""".format(**{"D": days, "date": new_date})

    dates2 = new_date - timedelta(days=days)
    datep2 =new_date - timedelta(days=1)
    final2 = str(datep2) + "||" + str(dates2)


    df2 = client.query(sql2).to_dataframe()
    df2['Period'] = final2

    df3 = df.append(df2, ignore_index=True)

    return Response(df3.to_json(orient="records"), mimetype='application/json')


@app.route('/report/cvr/compare-periods/device/usertype/<int:days>', methods=['GET'])
def GetComparisonConvertionRateDeviceUserType(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')

    sql = """SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate,device.deviceCategory AS device,IF(totals.newVisits IS NOT NULL, "New Visitor", "Returning Visitor") userType,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL {} DAY)) AND
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL 1 DAY))
        group by  device,userType
        order by device;""".format(days)

    given = '02-08-2017'
    date_object = datetime.strptime(given, '%d-%m-%Y').date()
    n_days_ago = date_object - timedelta(days=days)
    n_days_ago2 = date_object - timedelta(days=1)
    dates= n_days_ago2.strftime('%d-%m-%Y')
    datep = n_days_ago.strftime('%d-%m-%Y')
    final = datep + "||" + dates

    df = client.query(sql).to_dataframe()
    df['Period'] = final

    new_date = n_days_ago
    print(new_date)
    sql2 = """
        SELECT SUM(totals.transactions)/SUM(totals.visits) AS conversion_rate,device.deviceCategory AS device,IF(totals.newVisits IS NOT NULL, "New Visitor", "Returning Visitor") userType,
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE _TABLE_SUFFIX BETWEEN
        FORMAT_DATE("%Y%m%d", DATE_SUB("{date}", INTERVAL {D} DAY)) AND
        FORMAT_DATE("%Y%m%d",DATE_SUB("{date}", INTERVAL 1 DAY))
        group by  device,userType
        order by device;""".format(**{"D": days, "date": new_date})

    dates2 = new_date - timedelta(days=days)
    datep2 =new_date - timedelta(days=1)
    final2 = str(datep2) + "||" + str(dates2)


    df2 = client.query(sql2).to_dataframe()
    df2['Period'] = final2

    df3 = df.append(df2, ignore_index=True)

    return Response(df3.to_json(orient="records"), mimetype='application/json')

@app.route('/report/cvr/<int:days>/csv', methods=['GET'])
def GetConvertionRateCSV(days):
    print('days ', days)
    sql = """
                    SELECT
                    SUM( totals.transactions )/SUM( totals.visits ) as cvr
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                    WHERE
                    _TABLE_SUFFIX BETWEEN
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL {} DAY)) AND
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-02'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    cvr = data['cvr']['0']
    
    given = '02-08-2017'
    date_object = datetime.strptime(given, '%d-%m-%Y').date()
    n_days_ago = date_object - timedelta(days=days)
    n_days_ago2 = date_object - timedelta(days=1)
    dates = n_days_ago2.strftime('%d-%m-%Y')
    datep = n_days_ago.strftime('%d-%m-%Y')
    final = datep + "||" + dates


    log = [
        (str(final), cvr)
    ]

    response = Response(generate(log), mimetype='text/csv')
    response.headers.set("Content-Disposition", "attachment", filename="conversion_rate.csv")
    return response



@app.route('/report/userprofile/<int:id>', methods=['GET'])
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
    try:
        if data:
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
        else:
            response = jsonify({
                    'status': 'Fail',
                    'message': 'User ID is wrong'
                    })
            response.status_code = 201
            return response
    except:
        response = jsonify({
            'status': 'Fail',
            'message': 'User ID is wrong'
            })
        response.status_code = 201
        return response

def generate(log):
    data = StringIO()
    w = csv.writer(data)

    # write header
    w.writerow(('Period', 'Conversion_Rate'))
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
    app.run(host='0.0.0.0',port= 8080)
