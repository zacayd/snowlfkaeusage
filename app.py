from flask import Flask, jsonify
from utils.config_loader import load_config
from consumers.consumer_execution import ConsumerExecution
from database.sql_server import SQLServer
from database.arango_db import ArangoDB

app = Flask(__name__)

@app.route('/runUsageConsumer', methods=['GET'])
def run_api():
    try:
        config = load_config()
        consumerEx = ConsumerExecution(config)
        consumerEx.run()
        return jsonify({"status": "success", "message": "Run completed"}), 200
    except Exception as e:
        return jsonify({"status": "failed", "message": str(e)}), 500

@app.route('/HealthUsageConsumer', methods=['GET'])
def health_check():
    try:
        config = load_config()
        sql_server = SQLServer(config['OumDBConnectionString'])
        arango_db = ArangoDB(config['arangoDBUrl'])

        result_sql = sql_server.test_connection()
        result_arango = arango_db.test_connection()

        sql_message = result_sql[0].get_json()['message']
        arango_message = result_arango[0].get_json()['message']

        if result_sql[1] == 200 and result_arango[1] == 200:
            return jsonify({"status": "success", "message": "success"}), 200
        else:
            return jsonify({"status": "failed", "message": f'{sql_message} {arango_message}'}), 500
    except Exception as e:
        return jsonify({"status": "failed", "message": str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=7002)
