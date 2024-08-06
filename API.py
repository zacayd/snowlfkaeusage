import json

from flask import Flask, jsonify, request
from ConsumerExecution import ConsumerExecution
from Utils import UploadConfig, TestDBConnection, TestArangoDB

app = Flask(__name__)

@app.route('/runUsageConsumer', methods=['GET'])
def run_api():

    try:
            configJsonDict,config = UploadConfig()


            data={"kafka_broker": configJsonDict['kafka_broker'],
                    "OumDBConnectionString":configJsonDict['OumDBConnectionString'],
                    "dbvVendor":configJsonDict['dbvVendor'],
                    "GspUrl":configJsonDict['GspUrl'],
                    "hive_server":configJsonDict['hive_server'],
                    "hive_port":configJsonDict['hive_port'],
                    "hdfs_host": configJsonDict['hdfs_host'],
                    "hdfs_port": configJsonDict['hdfs_port'],
                    "arangoDBUrl":configJsonDict['arangoDBUrl'],
                    "list_of_tables":configJsonDict["list_of_tables"]

                    }
    except Exception as e:
            print(e)
            return jsonify({"status": "failed", "message": {e}}), 200

    consumerEx = ConsumerExecution(data,config)
    consumerEx.run()
    return jsonify({"status": "success", "message": "Run completed"}), 200

@app.route('/HealthUsageConsumer', methods=['GET'])
def Health():

        try:

            data=UploadConfig()
            envConnString=data["OumDBConnectionString"]
            arangoDBUrlServer=data["arangoDBUrl"]
            UploadConfig()
            resultSql = TestDBConnection(envConnString)
            resultArangoDB = TestArangoDB(arangoDBUrlServer)
            sqlMessage = json.loads(resultSql[0].response[0])['message']
            arangoDBMessage = json.loads(resultArangoDB[0].response[0])['message']
            if resultSql[1] == 200 and resultArangoDB[1] == 200:
                return jsonify({"status": "success", "message": "success"}), 200
            else:
                return jsonify({"status": "failed", "message": f'{sqlMessage} {arangoDBMessage}'}), 500
        except Exception as e:
            print(e)
            return jsonify({"status": "failed", "message": {e}}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=7002)
