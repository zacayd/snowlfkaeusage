import logging
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from langchain.chat_models import AzureChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from sqlalchemy import create_engine
from langchain_community.utilities.sql_database import SQLDatabase


import time
from Logger import Logger

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
log = Logger('AppLogger')  # Custom Logger instance

# Global variable to maintain context
conversation_context = []
message_text = []


def stream_response(questionTxt, chain, max_retries=10, delay=2):
    buffer = ""
    attempt = 0
    while attempt < max_retries:
        try:
            resp=chain.run(questionTxt)
            log.info(resp)
            for response in resp:
                buffer += response
                if len(buffer) > 50 or '\n' in buffer:
                    yield f"data: {buffer}\n\n"
                    buffer = ""
                # Append response to conversation context for context management
                conversation_context.append({"role": "assistant", "content": response})
            if buffer:
                yield f"data: {buffer}\n\n"
            break  # If successful, exit the retry loop
        except Exception as e:
            attempt += 1
            log.error(f"Attempt {attempt} failed with error: {e}")
            if attempt >= max_retries:

                yield f"data: Error processing query after {max_retries} attempts.\n\n"

                break
            time.sleep(delay)

def retry(func, delay=2):
    attempt = 0
    while True:
        try:
            attempt += 1
            return func()
        except Exception as e:
            log.error(f"Attempt {attempt} failed with error: {e}")
            time.sleep(delay)

@app.route('/query', methods=['POST'])
def query():
    global conversation_context  # Declare the conversation context as global
    try:
        data = request.json
        question = data.get('question')
        BASE_URL = data.get('BASE_URL')
        API_KEY = data.get('API_KEY')
        DEPLOYMENT_NAME = data.get('DEPLOYMENT_NAME')
        temperature = data.get('temperature')
        db_url = data.get('db_url')
        role = data.get('role')

        log.debug(f"Received request with question: {question}, db_url: {db_url}")
        log.info(f"Received request with question: {question}, db_url: {db_url}")

        azure_chat_model = AzureChatOpenAI(
            openai_api_base=BASE_URL,
            openai_api_version="2023-05-15",
            deployment_name=DEPLOYMENT_NAME,
            openai_api_key=API_KEY,
            openai_api_type="azure",
            temperature=temperature
        )



        try:

            engine = retry(lambda: create_engine(db_url))
            db = retry(lambda: SQLDatabase.from_uri(db_url))
            log.debug("Database connection established successfully.")
            log.info("Database connection established successfully.")
        except Exception as e:
            log.error(f"Database connection failed: {e}")
            log.error(f"Database connection failed: {e}")
            return jsonify({'error': 'Database connection failed', 'details': str(e)}), 500

        chain = SQLDatabaseChain.from_llm(azure_chat_model, db=db, verbose=True)

        try:
            with open('instructions', 'r') as f:
                instructions = f.read()
            log.debug("Instructions read successfully.")
            log.info("Instructions read successfully.")
        except Exception as e:
            log.error(f"Failed to read instructions: {e}")
            log.error(f"Failed to read instructions: {e}")
            return jsonify({'error': 'Failed to read instructions', 'details': str(e)}), 500

        try:
            # Append the new question to the conversation context
            conversation_context.append({"role": role, "content": question + '\n' + instructions})
            log.debug(f"Formatted question: {conversation_context}")
            log.info(f"Formatted question: {conversation_context}")

            return Response(stream_response(conversation_context, chain), content_type='text/event-stream')
        except Exception as e:
            log.error(f"Query processing failed: {e}")
            log.error(f"Query processing failed: {e}")
            return jsonify({'error': 'Query processing failed', 'details': str(e)}), 500

    except Exception as e:
        log.error(f"Unhandled exception: {e}")
        log.error(f"Unhandled exception: {e}")
        return jsonify({'error': 'Unhandled exception', 'details': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
