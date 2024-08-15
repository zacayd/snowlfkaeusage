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


class RedisManager:
    def __init__(self, redis_host, redis_port, db_url):
        self.client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.db_url_hash = hashlib.md5(db_url.encode()).hexdigest()

    def get_key(self, key):
        return f"{self.db_url_hash}:{key}"

    def get(self, key):
        return self.client.get(self.get_key(key))

    def set(self, key, value):
        self.client.set(self.get_key(key), value)

    def clear_cache(self):
        self.client.flushdb()

    def get_conversation_context(self):
        return json.loads(self.get(self.db_url_hash) or '[]')

    def save_conversation_context(self, context):
        self.set(self.db_url_hash, json.dumps(context))


class HiveManager:
    def __init__(self, db_url, logger):
        self.db_url = db_url
        self.log = logger

    def save_to_hive(self, question, input_tokens, output_tokens, total_tokens, cost):
        try:
            unique_id = str(uuid.uuid4())
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            escaped_question = question.replace("'", "''").replace("\n", " ").replace("\r", " ")

            query = f"""
                INSERT INTO usage_log (id, question, input_tokens, output_tokens, total_tokens, cost, request_date)
                VALUES ('{unique_id}', '{escaped_question}', {input_tokens}, {output_tokens}, {total_tokens}, {cost}, '{current_timestamp}')
            """

            dbname = self.db_url.split('/')[3]
            hive_server = self.db_url.split('/')[2].split(':')[0]
            hive_port = int(self.db_url.split('/')[2].split(':')[1])

            conn = hive.Connection(host=hive_server, port=hive_port, username='hive')
            cursor = conn.cursor()

            try:
                cursor.execute(f"USE {dbname}")
                cursor.execute(query)
                self.log.info("Usage data saved to Hive successfully.")
            except Exception as e:
                self.log.error(f"Failed to execute query: {e}")
            finally:
                cursor.close()
                conn.close()
        except Exception as e:
            self.log.error(f"Failed to save usage data to Hive: {e}")


class QueryProcessor:
    def __init__(self, config, redis_manager, hive_manager, logger):
        self.config = config
        self.redis_manager = redis_manager
        self.hive_manager = hive_manager
        self.log = logger

    def calculate_cost(self, input_token_count, output_token_count):
        input_cost = (input_token_count / 1000) * self.config['COST_PER_1000_INPUT_TOKENS']
        output_cost = (output_token_count / 1000) * self.config['COST_PER_1000_OUTPUT_TOKENS']
        return round(input_cost + output_cost, 2)

    def stream_response(self,question, questionTxt, chain, db_url, max_retries=3, delay=2):
        buffer = ""
        attempt = 0
        question_hash = hashlib.md5(question.encode()).hexdigest()
        redis_key = self.redis_manager.get_key(db_url)
        conversation_context = self.redis_manager.get_conversation_context()
        input_tokens, total_tokens, cost = 0, 0, 0

        while attempt < max_retries:
            with get_openai_callback() as cb:
                try:
                    cached_response = self.redis_manager.get(question_hash)
                    if cached_response:
                        self.log.info("Answer retrieved from Redis.")
                        resp = cached_response
                        output_tokens, input_tokens, total_tokens, cost = 0, 0, 0, 0
                    else:
                        prompt_query = """
                            Given an input question, first create a syntactically correct  query to run, 
                            then look at the results of the query and return the answer.
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
                        cost = cb.total_cost

                    self.log.info(resp)
                    self.redis_manager.set(question_hash, resp)
                    conversation_context.append({"role": "assistant", "content": resp})
                    self.redis_manager.save_conversation_context(conversation_context)

                    resp = f'{resp}\n Tokens: {total_tokens}, Total cost: {cost} $'
                    self.hive_manager.save_to_hive(question, input_tokens, output_tokens, total_tokens, cost)

                    for response in resp:
                        buffer += response
                        if len(buffer) > 500 or '\n' in buffer:
                            yield f"data: {buffer}\n\n"
                            buffer = ""
                    if buffer:
                        yield f"data: {buffer}\n\n"
                    break
                except Exception as e:
                    attempt += 1
                    self.hive_manager.save_to_hive(question, input_tokens, str(e), total_tokens, cost)
                    self.log.error(f"Attempt {attempt} failed with error: {e}")
                    if attempt >= max_retries:
                        yield f"data: Error processing query after {max_retries} attempts.\n\n"
                        break
                    time.sleep(delay)


class App:
    def __init__(self):
        self.app = Flask(__name__)
        CORS(self.app)
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
        self.log = Logger('AppLogger')

        with open('config.json', 'r') as f:
            self.config = json.load(f)

        self.redis_manager = RedisManager(self.config.get('REDIS_HOST', 'localhost'),
                                          self.config.get('REDIS_PORT', 6379), self.config.get('db_url'))
        self.hive_manager = HiveManager(self.config.get('db_url'), self.log)
        self.query_processor = QueryProcessor(self.config, self.redis_manager, self.hive_manager, self.log)

        self.setup_routes()

    def setup_routes(self):
        @self.app.route('/query', methods=['POST'])
        def query():
            try:
                data = request.json
                question = data.get('question')

                self.log.debug(f"Received request with question: {question}, db_url: {self.config['db_url']}")
                self.log.info(f"Received request with question: {question}, db_url: {self.config['db_url']}")

                azure_chat_model = AzureChatOpenAI(
                    openai_api_base=self.config['BASE_URL'],
                    openai_api_version="2023-05-15",
                    deployment_name=self.config['DEPLOYMENT_NAME'],
                    openai_api_key=self.config['API_KEY'],
                    openai_api_type="azure",
                    temperature=self.config['temperature']
                )

                try:
                    engine = self.retry(lambda: create_engine(self.config['db_url']))
                    db = self.retry(lambda: SQLDatabase.from_uri(self.config['db_url']))
                    self.log.debug("Database connection established successfully.")
                    self.log.info("Database connection established successfully.")
                except Exception as e:
                    self.log.error(f"Database connection failed: {e}")
                    return jsonify({'error': 'Database connection failed', 'details': str(e)}), 500

                chain = SQLDatabaseChain.from_llm(azure_chat_model, db=db, verbose=True)

                try:
                    with open('instructions', 'r') as f:
                        instructions = f.read()
                    self.log.debug("Instructions read successfully.")
                    self.log.info("Instructions read successfully.")
                except Exception as e:
                    self.log.error(f"Failed to read instructions: {e}")
                    return jsonify({'error': 'Failed to read instructions', 'details': str(e)}), 500

                try:
                    conversation_context = self.redis_manager.get_conversation_context()

                    if not any(instructions in message['content'] for message in conversation_context):
                        conversation_context.append(
                            {"role": self.config['role'], "content": question + '\n' + instructions})
                    else:
                        conversation_context.append({"role": self.config['role'], "content": question})

                    self.redis_manager.save_conversation_context(conversation_context)

                    self.log.debug(f"Formatted question: {conversation_context}")
                    self.log.info(f"Formatted question: {conversation_context}")

                    return Response(self.query_processor.stream_response(question, conversation_context,chain, self.config['db_url']),
                                    content_type='text/event-stream')
                except Exception as e:
                    self.log.error(f"Query processing failed: {e}")
                    return jsonify({'error': 'Query processing failed', 'details': str(e)}), 500

            except Exception as e:
                self.log.error(f"Unhandled exception: {e}")
                return jsonify({'error': 'Unhandled exception', 'details': str(e)}), 500

        @self.app.route('/clear_cache', methods=['POST'])
        def clear_cache():
            try:
                self.redis_manager.clear_cache()
                self.log.info("Redis cache cleared.")
                return jsonify({'status': 'success', 'message': 'Cache cleared successfully'}), 200
            except Exception as e:
                self.log.error(f"Failed to clear cache: {e}")
                return jsonify({'status': 'error', 'message': 'Failed to clear cache', 'details': str(e)}), 500

    def retry(self, func, delay=2):
        attempt = 0
        while True:
            try:
                attempt += 1
                return func()
            except Exception as e:
                self.log.error(f"Attempt {attempt} failed with error: {e}")
                time.sleep(delay)

    def run(self):
        self.app.run(host='0.0.0.0', port=5000)


if __name__ == '__main__':
    app = App()
    app.run()
