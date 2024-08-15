import json
import os
from langchain.agents import load_tools
from langchain.agents import initialize_agent
from langchain.agents import AgentType
from langchain import OpenAI, SQLDatabase
from langchain.chat_models import ChatOpenAI
from langchain_community.callbacks import get_openai_callback
from langchain_community.chat_models import AzureChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
from langchain.memory import ConversationBufferMemory
from langchain.memory.chat_message_histories import RedisChatMessageHistory
from langchain import OpenAI, LLMChain
from langchain.utilities import GoogleSearchAPIWrapper
from langchain.prompts.prompt import PromptTemplate
from typing import Any, Dict, List
from langchain.schema import BaseMemory
from langchain_experimental.generative_agents.memory import GenerativeAgentMemory
from langchain.memory.buffer import ConversationBufferMemory
from langchain.chains.conversation.base import ConversationChain

from langchain.agents import ZeroShotAgent, Tool, AgentExecutor
from langchain.memory import ConversationBufferMemory, ReadOnlySharedMemory
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.utilities import GoogleSearchAPIWrapper
from langchain.utilities import SerpAPIWrapper

import warnings
warnings.filterwarnings("ignore")

with open('config.json', 'r') as f:
    config = json.load(f)
db_url = config.get('db_url')

BASE_URL = config.get('BASE_URL')
API_KEY = config.get('API_KEY')
DEPLOYMENT_NAME = config.get('DEPLOYMENT_NAME')
temperature = config.get('temperature')
db_url = config.get('db_url')
role = config.get('role')

db = SQLDatabase.from_uri(db_url)

# setup llm
llm = AzureChatOpenAI(
            openai_api_base=BASE_URL,
            openai_api_version="2023-05-15",
            deployment_name=DEPLOYMENT_NAME,
            openai_api_key=API_KEY,
            openai_api_type="azure",
            temperature=temperature
        )
# Create db chain
prompt_query = """
            Given an input question, first create a syntactically correct postgresql query to run, then look at the results of the query and return the answer.
            Here is the format to follow:
             SQLQuery: SQL Query to run
             Answer: Final answer here
            {question}
      """
# Setup the database chain


with open('instructions', 'r') as f:
    instructions = f.read()
question = f"what is the query that run the most time? \n {instructions}"

template = """This is a conversation between a human and a bot:

{chat_history}

Write a summary of the conversation for {input}:
"""

prompt = PromptTemplate(input_variables=["input", "chat_history"], template=template)
memory = ConversationBufferMemory(memory_key="chat_history")
readonlymemory = ReadOnlySharedMemory(memory=memory)




db_chain = SQLDatabaseChain.from_llm(llm, db=db, verbose=True)

tools = [
    Tool(
        name="Query DB",
        func=db_chain.run,
        description="",
    ),
]

prefix = """Have a conversation with a human, answering the following questions as best you can. You have access to the following tools:"""
suffix = """Begin!"

{chat_history}
Question: {input}
{agent_scratchpad}"""

prompt = ZeroShotAgent.create_prompt(
    tools,
    prefix=prefix,
    suffix=suffix,
    input_variables=["input", "chat_history", "agent_scratchpad"],
)

llm_chain = LLMChain(llm= llm, prompt=prompt)
agent = ZeroShotAgent(llm_chain=llm_chain, tools=tools, verbose=True)
agent_chain = AgentExecutor.from_agent_and_tools(
    agent=agent, tools=tools, verbose=True, memory=memory
)
agent = initialize_agent(tools, llm, agent=AgentType.ZERO_SHOT_REACT_DESCRIPTION, verbose=True)

txt=agent_chain.run("Hi my name is jihwan")
print(txt)
txt=agent_chain.run("Do you remember my name?")
print(txt)

try:
    with get_openai_callback() as cb:
        query = prompt_query.format(question=question)
        print(f"zzz:{question}")
        print(cb.prompt_tokens)

        txt=agent_chain.run(question)
        print(txt)
        print(cb.prompt_tokens)
        print(f"zzz:how much in minutes?")
        txt = agent_chain.run("how much in minutes?")
        print(cb.prompt_tokens)
        print(txt)

except Exception as e:
    print(e)
