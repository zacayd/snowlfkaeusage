import json
import uuid
import hashlib
import redis
from langchain.agents import ZeroShotAgent, AgentExecutor, initialize_agent, AgentType
from langchain.chains.llm import LLMChain
from langchain.memory import ConversationBufferMemory, ReadOnlySharedMemory
from langchain_community.callbacks import get_openai_callback
from langchain_core.prompts import PromptTemplate
from langchain_core.tools import Tool
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
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
        self.log = Logger('RedisManager')

    def get_key(self, key):
        return f"{self.db_url_hash}:{key}"

    def get(self, key):
        return self.client.get(self.get_key(key))

    def set(self, key, value):
        try:
            if isinstance(value, dict) and 'conv' in value and isinstance(value['conv'], list):
                # Extract the 'content' value from each object in the list
                content_list = [item for item in value['conv']]
                # Convert the list of contents to a JSON string
                value['conv'] = content_list
            self.client.set(self.get_key(key), json.dumps(value))
        except Exception as e:
            self.log.error(e)
    def clear_cache(self):
        self.client.flushdb()

    def get_conversation_context_conversation(self,messagetype):
        return json.loads(self.get(self.get_key(messagetype)) or '{}')

    def save_conversation_context_conversation(self, context,messagetype):
        try:
            if isinstance(context, list):
                # Save list directly without json.dumps
                self.set(self.get_key(messagetype), {messagetype:context})
            else:
                # For other types, save as JSON string
                self.set(self.get_key(messagetype), json.dumps(context))
        except Exception as e:
            self.log.error(e)

class HiveManager:
    def __init__(self, db_url, logger):
        self.db_url = db_url
        self.log = logger
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
        self.log = Logger('HiveManager')

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
    def __init__(self, config, redis_manager, hive_manager, logger, memory):
        self.config = config
        self.redis_manager = redis_manager
        self.hive_manager = hive_manager
        self.log = logger
        self.memory = memory
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')
        self.log = Logger('QueryProcessor')

    def load_conversation_from_redis(self,agentChain):
        # Load conversation context from Redis and update the memory
        conversation_context = self.redis_manager.get_conversation_context_conversation("conversation")
        self.memory.chat_memory.messages = conversation_context
        agentChain.memory.chat_memory.messages=conversation_context

    def save_conversation_to_redis(self,chat_history):

        self.redis_manager.save_conversation_context_conversation(chat_history,"conversation")

    def stream_response(self, question,origquestion, chain, max_retries=3, delay=2):
        buffer = ""
        attempt = 0



        question_hash = hashlib.md5(origquestion.encode()).hexdigest()
        input_tokens, total_tokens, cost = 0, 0, 0

        while attempt < max_retries:
            with get_openai_callback() as cb:
                try:
                    # Load the latest conversation context from Redis before running the chain


                    cached_response = self.redis_manager.get(question_hash)
                    if cached_response:
                        self.log.info("Answer retrieved from Redis.")
                        resp = cached_response
                        output_tokens, input_tokens, total_tokens, cost = 0, 0, 0, 0
                    else:
                        # prompt_query = """
                        #     Given an input question, first create a syntactically correct query to run,
                        #     then look at the results of the query and return the answer.
                        #     Here is the format to follow:
                        #     Answer: Final answer here
                        #     SQLQuery: SQL Query to run
                        #     {question}
                        # """
                        # query = prompt_query.format(question=question)
                        # self.load_conversation_from_redis(chain) dont know why it fails on run when you add to the memory the values

                        try:
                            resp = chain.run(question)
                        except Exception as e:
                            resp=''
                            self.log.error(e)

                        # After running the chain, update the Redis with the latest messages
                        chat_hist=chain.memory.chat_memory.messages
                        content_list = [item.content for item in chat_hist]

                        self.save_conversation_to_redis(content_list)


                        output_tokens = cb.completion_tokens
                        input_tokens = cb.prompt_tokens
                        total_tokens = cb.total_tokens
                        cost = cb.total_cost

                    self.log.info(resp)
                    self.redis_manager.set(question_hash, resp)


                    resp = f'{resp}\n Tokens: {total_tokens}, Total cost: {cost} $'


                    for response in resp:
                        buffer += response
                        if len(buffer) > 500 or '\n' in buffer:
                            yield f"data: {buffer}\n\n"
                            buffer = ""
                    if buffer:
                        yield f"data: {buffer}\n\n"
                    break
                    self.log.info("save to table")
                    self.hive_manager.save_to_hive(question, input_tokens, output_tokens, total_tokens, cost)

                except Exception as e:
                    self.log.error(e)
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

        self.redis_manager = RedisManager(self.config.get('REDIS_HOST', 'localhost'), self.config.get('REDIS_PORT', 6379), self.config.get('db_url'))
        self.clear_cache_on_startup()
        self.hive_manager = HiveManager(self.config.get('db_url'), self.log)
        self.query_processor = QueryProcessor(self.config, self.redis_manager, self.hive_manager, self.log,ConversationBufferMemory(memory_key="chat_history"))

        # Initialize components only once
        self.azure_chat_model = AzureChatOpenAI(
            openai_api_base=self.config['BASE_URL'],
            openai_api_version="2023-05-15",
            deployment_name=self.config['DEPLOYMENT_NAME'],
            openai_api_key=self.config['API_KEY'],
            openai_api_type="azure",
            temperature=self.config['temperature']
        )

        self.memory = ConversationBufferMemory(memory_key="chat_history")
        self.query_processor = QueryProcessor(self.config, self.redis_manager, self.hive_manager, self.log, self.memory)

        try:
            self.engine = self.retry(lambda: create_engine(self.config['db_url']))
            self.db = self.retry(lambda: SQLDatabase.from_uri(self.config['db_url']))
            self.log.debug("Database connection established successfully.")
            self.log.info("Database connection established successfully.")
        except Exception as e:
            self.log.error(f"Database connection failed during initialization: {e}")
            raise

        self.chain = SQLDatabaseChain.from_llm(self.azure_chat_model, db=self.db, verbose=True)

        self.template = """This is a conversation between a human and a bot:

        {chat_history}

        Write a summary of the conversation for {input}:
        """

        self.prompt = PromptTemplate(input_variables=["input", "chat_history"], template=self.template)
        self.memory = ConversationBufferMemory(memory_key="chat_history")
        self.readonlymemory = ReadOnlySharedMemory(memory=self.memory)

        self.tools = [
            Tool(
                name="Query DB",
                func=self.chain.run,
                description="",
            ),
        ]

        self.prefix = """Have a conversation with a human, answering the following questions as best you can. You have access to the following tools:"""
        self.suffix = """Begin!"

        {chat_history}
        Question: {input}
        {agent_scratchpad}"""

        self.agent_prompt = ZeroShotAgent.create_prompt(
            self.tools,
            prefix=self.prefix,
            suffix=self.suffix,
            input_variables=["input", "chat_history", "agent_scratchpad"],
        )

        self.llm_chain = LLMChain(llm=self.azure_chat_model, prompt=self.agent_prompt)
        self.agent = ZeroShotAgent(llm_chain=self.llm_chain, tools=self.tools, verbose=True)
        self.agent_chain = AgentExecutor.from_agent_and_tools(
            agent=self.agent, tools=self.tools, verbose=True, memory=self.memory
        )
        self.agent_instance = initialize_agent(self.tools, self.azure_chat_model, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

        # self.query_processor.load_conversation_from_redis(self.agent_chain)
        self.setup_routes()

    def clear_cache_on_startup(self):
        try:
            self.redis_manager.clear_cache()
            self.log.info("Redis cache cleared on startup.")
        except Exception as e:
            self.log.error(f"Failed to clear cache on startup: {e}")
    def setup_routes(self):
        @self.app.route('/query', methods=['POST'])
        def query():
            try:
                data = request.json
                question = data.get('question')

                self.log.debug(f"Received request with question: {question}, db_url: {self.config['db_url']}")
                self.log.info(f"Received request with question: {question}, db_url: {self.config['db_url']}")

                try:
                    with open('instructions', 'r') as f:
                        instructions = f.read()
                    self.log.info("Instructions read successfully.")
                except Exception as e:
                    self.log.error(f"Failed to read instructions: {e}")
                    return jsonify({'error': 'Failed to read instructions', 'details': str(e)}), 500

                try:
                    conversation_context = self.redis_manager.get_conversation_context_conversation("messages")
                    convDict = json.loads(conversation_context) if isinstance(conversation_context, str) else conversation_context
                    convDict = json.loads(convDict) if isinstance(convDict,
                                                                              str) else convDict

                    if (len(convDict)>0):
                        try:
                            lst=[message['content'] for message in convDict["messages"]]
                        except Exception as e:
                            self.log.error(e)



                    if len(convDict)>0 :
                        exists = any(instructions in item for item in lst)
                        if exists:
                            convDict["messages"].append(
                                {"role": self.config['role'], "content": question})
                            origquestion=question

                    else:
                        convDict["messages"]=[]
                        convDict["messages"].append({"role": self.config['role'], "content": question + '\n' + instructions})
                        origquestion = question
                        question=question + '\n' + instructions




                    self.redis_manager.save_conversation_context_conversation(conversation_context,"messages")

                    self.log.info(f"Formatted question: {conversation_context}")

                    return Response(self.query_processor.stream_response(question,origquestion,self.agent_chain),
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
