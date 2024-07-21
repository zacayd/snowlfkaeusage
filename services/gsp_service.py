import json
import requests

class GspService:
    def __init__(self, dbv_vendor, gsp_url):
        self.dbv_vendor = dbv_vendor
        self.gsp_url = gsp_url

    def gsp_parsing(self, query):
        payload = json.dumps({
            "query": query,
            "vendor": self.dbv_vendor
        })
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.post(self.gsp_url, headers=headers, data=payload)
        return response.text
