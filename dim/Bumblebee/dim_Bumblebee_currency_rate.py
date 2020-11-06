import json
import time
import sys
from urllib.request import Request, urlopen
from clickhouse_driver import Client

if __name__ == '__main__':

    #网站获取数据Api
    url = "http://api.currencylayer.com/live?access_key=fce6855bc3e33b3bd9d950864b3faf77&format=1"
    #
    #构建请求
    request = Request(url)
    html = urlopen(request)
    # 获取数据
    data = html.read()

    json_data = json.loads(data)['quotes']

    json_data = json.dumps(json_data)

    json_data = json.loads(json_data)

    import_time = time.strftime("%Y-%m-%d",time.localtime())

    if( len(sys.argv) > 1 ):
        import_time = sys.argv[1]

    client = Client(host='127.0.0.1',database='dim',user='redtea',password='redtea@clickhouse86868')

    # update表中数据保证幂等性
    updateSQL = '''alter table dim_Bumblebee_currency_rate delete where import_time = '{0}' '''.format(import_time)

    client.execute(updateSQL)

    # 获取表中的最大id
    resultSQL = "select max(id) as id from dim_Bumblebee_currency_rate"

    resultID = client.execute(resultSQL)
    # 对结果处理
    resultID = resultID[0]
    resultID = str(resultID)
    resultID = resultID[1:]
    resultID = resultID[:-2]
    resultID = int(resultID)

    values = ""
    USDCNY = json_data["USDCNY"]
    for key in json_data:
        resultID += 1
        values = values + '''({0},'{1}',{2},{3},'{4}'),'''.format(resultID,key[3:],json_data[key],json_data[key]/USDCNY,import_time)

    values = values[:-1]

    sql = '''INSERT INTO TABLE dim_Bumblebee_currency_rate values''' + values
    client = Client(host='127.0.0.1',database='dim',user='redtea',password='redtea@clickhouse86868')
    ans = client.execute(sql)
    client.disconnect()