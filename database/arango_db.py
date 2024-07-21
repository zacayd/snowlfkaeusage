import requests
from flask import jsonify

class ArangoDB:
    def __init__(self, arango_db_url):
        self.arango_db_url = arango_db_url

    def test_connection(self):
        try:
            url = f"http://{self.arango_db_url}/_api/version"
            response = requests.get(url)
            if response.status_code == 200:
                return jsonify({"status": "success", "message": response.text}), response.status_code
            else:
                return jsonify({"status": "success", "message": response.text}), response.status_code
        except Exception as e:
            return jsonify({"status": "failed", "message": str(e)}), 400
