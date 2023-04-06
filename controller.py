import json
from chatgpt import ChatGPT
from google_sql_connector import GoogleCloudSQL
import configparser
import logging

def read_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    return config

config = read_config()

# Access the config values
server = config.get("database", "server")
database = config.get("database", "database")
user = config.get("database", "user")
password = config.get("database", "password")
openai_api_key = config.get("openai", "api_key")
openai_org = config.get("openai", "org")
openai_model = config.get("openai", "model")
max_tokens = config.get("openai", "max_tokens")


class Controller:

    def __init__(self):
        # initialise all the things
        self.google_sql = GoogleCloudSQL(server, database, user, password)
        self.google_sql.connect()
        self.chatModel = ChatGPT(openai_api_key, openai_org, openai_model, max_tokens)

    def run(self, message, sender, counter=0):
        if counter > 4:
            return 'error: too many requests'
        response_string = self.chatModel.message(message, sender)
        try:
            response = json.loads(response_string[:-1] if response_string.endswith('.') else response_string)
        except json.JSONDecodeError:
            return self.run("Please repeat that answer but use valid JSON only.", "SYSTEM", counter + 1)
        match response["recipient"]:
            case "USER": 
                return response["message"]
            case "SERVER":
                match response["action"]:
                    case "QUERY":
                        result = self.google_sql.execute_query(response["message"])
                        return self.run(result, None, counter + 1)
                    case "SCHEMA":
                        result = self.google_sql.execute_schema(response["message"])
                        return self.run(result, None, counter + 1)
                    case _:
                        logging.error('Invalid action')
                        logging.error(response)
            case _:
                logging.error('Invalid recipient')
                logging.error(response)
