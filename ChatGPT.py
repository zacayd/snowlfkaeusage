import json
import uuid
import hashlib
import redis
from langchain_community.callbacks import get_openai_callback
from pyhive import hive
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from langchain.chat_models import AzureChatOpenAI
from langchain_community.utilities.sql_database import SQLDatabase
from langchain import OpenAI, SQLDatabase
from langchain_community.chat_models import AzureChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain


import time
from Logger import Logger

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
log = Logger('AppLogger')  # Custom Logger instance

# Constants for token cost

# Load configuration from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

BASE_URL = config.get('BASE_URL')
API_KEY = config.get('API_KEY')
DEPLOYMENT_NAME = config.get('DEPLOYMENT_NAME')
temperature = config.get('temperature')
db_url = config.get('db_url')
role = config.get('role')


COST_PER_1000_INPUT_TOKENS = config.get('COST_PER_1000_INPUT_TOKENS')
COST_PER_1000_OUTPUT_TOKENS = config.get('COST_PER_1000_OUTPUT_TOKENS')

# Initialize Redis connection
redis_client = redis.StrictRedis(host=config.get('REDIS_HOST', 'localhost'), port=config.get('REDIS_PORT', 6379),
                                 decode_responses=True)

# Generate MD5 hash of db_url
db_url_hash = hashlib.md5(db_url.encode()).hexdigest()

def get_redis_key(key):
    return f"{db_url_hash}:{key}"

def calculate_cost(input_token_count, output_token_count):
    input_cost = (input_token_count / 1000) * COST_PER_1000_INPUT_TOKENS
    output_cost = (output_token_count / 1000) * COST_PER_1000_OUTPUT_TOKENS
    rounded_cost = round(input_cost + output_cost, 2)
    return rounded_cost

def count_tokens(text):
    # Simple token count based on number of words
    return len(text.split())

def stream_response(question, questionTxt, chain, db_url, max_retries=3, delay=2):
    buffer = ""
    attempt = 0
    questionText = question
    question_hash = hashlib.md5(questionText.encode()).hexdigest()
    redis_key = get_redis_key(db_url)
    conversation_context = json.loads(redis_client.get(redis_key) or '[]')
    input_tokens = 0
    total_tokens = 0
    cost = 0
    # If not in Redis, generate the response using LangChain
    while attempt < max_retries:
        with get_openai_callback() as cb:
            try:
                # Check if the answer is already in Redis
                cached_response = redis_client.get(get_redis_key(question_hash))
                if cached_response:
                    log.info("Answer retrieved from Redis.")
                    resp = cached_response
                    output_tokens = 0
                    input_tokens = 0
                    total_tokens = 0
                    cost = 0
                else:

                    prompt_query = """
                                        Given an input question, first create a syntactically correct  query to run, then look at the results of the query and return the answer.
                                        Here is the format to follow:
                                        Question: Question here
                                        SQLQuery: SQL Query to run
                                        SQLResult: Result of the SQLQuery
                                        Answer: Final answer here

                                        {question}
                                  """

                    query = prompt_query.format(question=questionTxt)
                    resp = chain.run(query)
                    output_tokens = cb.completion_tokens
                    input_tokens = cb.prompt_tokens
                    total_tokens = cb.total_tokens
                    cost=cb.total_cost

                log.info(resp)
                redis_client.set(get_redis_key(question_hash), resp)
                conversation_context.append({"role": "assistant", "content": resp})

                # Append response to conversation context
                redis_client.set(redis_key, json.dumps(conversation_context))  # Save updated context to Redis

                resp = f'{resp}\n Tokens: {total_tokens}, Total cost: {cost} $'
                save_to_hive(db_url, questionText, input_tokens, output_tokens, total_tokens, cost)

                for response in resp:
                    buffer += response
                    if len(buffer) > 500 or '\n' in buffer:
                        yield f"data: {buffer}\n\n"
                        buffer = ""
                if buffer:
                    yield f"data: {buffer}\n\n"
                break  # If successful, exit the retry loop
            except Exception as e:
                attempt += 1
                save_to_hive(db_url, questionTxt, input_tokens, str(e), total_tokens, cost)

                log.error(f"Attempt {attempt} failed with error: {e}")
                if attempt >= max_retries:
                    yield f"data: Error processing query after {max_retries} attempts.\n\n"
                    break
                time.sleep(delay)

def save_to_hive(db_url, question, input_tokens, output_tokens, total_tokens, cost):
    try:
        unique_id = str(uuid.uuid4())  # Generate a unique string ID
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Get current timestamp

        # Escape single quotes in the question
        escaped_question = question.replace("'", "''").replace("\n", " ").replace("\r", " ")

        query = f"""
            INSERT INTO usage_log (id, question, input_tokens, output_tokens, total_tokens, cost, request_date)
            VALUES ('{unique_id}', '{escaped_question}', {input_tokens}, {output_tokens}, {total_tokens}, {cost}, '{current_timestamp}')
        """

        # Extract database connection details
        dbname = db_url.split('/')[3]
        hive_server = db_url.split('/')[2].split(':')[0]
        hive_port = int(db_url.split('/')[2].split(':')[1])

        # Establish connection to Hive
        conn = hive.Connection(host=hive_server, port=hive_port, username='hive')
        cursor = conn.cursor()

        try:
            cursor.execute(f"USE {dbname}")
            cursor.execute(query)
            log.info("Usage data saved to Hive successfully.")
        except Exception as e:
            log.error(f"Failed to execute query: {e}")
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        log.error(f"Failed to save usage data to Hive: {e}")

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
    try:
        data = request.json
        question = data.get('question')

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
            return jsonify({'error': 'Database connection failed', 'details': str(e)}), 500

        chain = SQLDatabaseChain.from_llm(azure_chat_model, db=db, verbose=True)

        try:
            with open('instructions', 'r') as f:
                instructions = f.read()
            log.debug("Instructions read successfully.")
            log.info("Instructions read successfully.")
        except Exception as e:
            log.error(f"Failed to read instructions: {e}")
            return jsonify({'error': 'Failed to read instructions', 'details': str(e)}), 500

        try:
            # Retrieve the conversation context from Redis using the hashed key
            redis_key = get_redis_key(db_url)
            conversation_context = json.loads(redis_client.get(redis_key) or '[]')

            # Check if instructions are already in the conversation context
            if not any(instructions in message['content'] for message in conversation_context):
                # Append the new question along with instructions only if not already present
                conversation_context.append({"role": role, "content": question + '\n' + instructions})
            else:
                # Append the question only
                conversation_context.append({"role": role, "content": question})

            # Save the updated conversation context in Redis under the hashed db_url key
            redis_client.set(redis_key, json.dumps(conversation_context))

            log.debug(f"Formatted question: {conversation_context}")
            log.info(f"Formatted question: {conversation_context}")

            return Response(stream_response(question, conversation_context, chain, db_url), content_type='text/event-stream')
        except Exception as e:
            log.error(f"Query processing failed: {e}")
            return jsonify({'error': 'Query processing failed', 'details': str(e)}), 500

    except Exception as e:
        log.error(f"Unhandled exception: {e}")
        return jsonify({'error': 'Unhandled exception', 'details': str(e)}), 500

@app.route('/clear_cache', methods=['POST'])
def clear_cache():
    try:
        # Flush the entire Redis database
        redis_client.flushdb()
        log.info("Redis cache cleared.")
        return jsonify({'status': 'success', 'message': 'Cache cleared successfully'}), 200
    except Exception as e:
        log.error(f"Failed to clear cache: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to clear cache', 'details': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
