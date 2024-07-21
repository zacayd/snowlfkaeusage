from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from langchain.chat_models import AzureChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from sqlalchemy import create_engine
from langchain_community.utilities.sql_database import SQLDatabase

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

def stream_response(questionTxt, chain):
    buffer = ""
    for response in chain.run(questionTxt):
        buffer += response
        if len(buffer) > 50 or '\n' in buffer:
            yield f"data: {buffer}\n\n"
            buffer = ""
    if buffer:
        yield f"data: {buffer}\n\n"

@app.route('/query', methods=['POST'])
@app.route('/query', methods=['POST'])
def query():
    data = request.json
    question = data.get('question')
    BASE_URL = data.get('BASE_URL')
    API_KEY = data.get('API_KEY')
    DEPLOYMENT_NAME = data.get('DEPLOYMENT_NAME')
    temperature = data.get('temperature')
    db_url = data.get('db_url')
    role = data.get('role')

    azure_chat_model = AzureChatOpenAI(
        openai_api_base=BASE_URL,
        openai_api_version="2023-05-15",
        deployment_name=DEPLOYMENT_NAME,
        openai_api_key=API_KEY,
        openai_api_type="azure",
        temperature=temperature
    )

    try:
        engine = create_engine(db_url)
        db = SQLDatabase.from_uri(db_url)
    except Exception as e:
        return jsonify({'error': str(e)})

    chain = SQLDatabaseChain.from_llm(azure_chat_model, db=db, verbose=True)

    try:
        with open('instructions', 'r') as f:
            instructions = f.read()
    except Exception as e:
        return jsonify({'error': str(e)})

    try:
        question = question + '\n' + instructions
        questionTxt = [{"role": role, "content": question}]
        return Response(stream_response(questionTxt, chain), content_type='text/event-stream')
    except Exception as e:
        return jsonify({'error': str(e), 'details': repr(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
