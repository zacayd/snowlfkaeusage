import json
from flask import jsonify
from sqlalchemy import create_engine, text
import requests

def TestDBConnection(connectionString):
    try:
        server = connectionString.split(';')[0].split('=')[1]
        database = connectionString.split(';')[1].split('=')[1]
        usr = connectionString.split(';')[2].split('=')[1]
        pwd = connectionString.split(';')[3].split('=')[1]
        pwd = pwd.replace('@', '%40')
        port = "1433"
        engine = create_engine(f"mssql+pyodbc://{usr}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes")
        cnxn = engine.connect()
        cnxn.close()
        engine.dispose()
        return jsonify({"status": "success", "message": "success"}), 200
    except Exception as e:
        return jsonify({"status": "failed", "message": str(e)}), 400

def TestArangoDB(arangoDBUrl):
    try:
        url = f"http://{arangoDBUrl}/_api/version"
        response = requests.get(url)
        if response.status_code == 200:
            return jsonify({"status": "success", "message": response.text}), response.status_code
        else:
            return jsonify({"status": "success", "message": response.text}), response.status_code
    except Exception as e:
        return jsonify({"status": "failed", "message": str(e)}), 400

def UploadConfig():
    try:

        with open('Config.json', 'r') as f:
            try:
                    config = json.load(f)
                    connectionString = config['OumDBConnectionString']
                    server = connectionString.split(';')[0].split('=')[1]
                    database = connectionString.split(';')[1].split('=')[1]
                    usr = connectionString.split(';')[2].split('=')[1]
                    pwd = connectionString.split(';')[3].split('=')[1]
                    pwd = pwd.replace('@', '%40')
                    port = "1433"
                    sqlConnString = f"mssql+pyodbc://{usr}:{pwd}@{server}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
                    engine = create_engine(sqlConnString)
                    cnxn = engine.connect()
                    result = cnxn.execute(text(config['EnvConfigQuery']))
                    dataconfig = result.fetchall()
                    configJson = dataconfig[0][0]
                    configJsonDict = json.loads(configJson)
                    cnxn.close()
                    engine.dispose()
                    return configJsonDict,config
            except Exception as e:
                    print(e)
        return {}
    except Exception as e:
        print(e)

