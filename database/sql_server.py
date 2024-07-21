from sqlalchemy import create_engine
from flask import jsonify

class SQLServer:
    def __init__(self, connection_string):
        self.connection_string = connection_string

    def test_connection(self):
        try:
            engine = create_engine(self.connection_string)
            cnxn = engine.connect()
            cnxn.close()
            engine.dispose()
            return jsonify({"status": "success", "message": "success"}), 200
        except Exception as e:
            return jsonify({"status": "failed", "message": str(e)}), 400
