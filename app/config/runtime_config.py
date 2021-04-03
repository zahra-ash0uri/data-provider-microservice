from dotenv import load_dotenv
import os


class RuntimeConfig:
    load_dotenv()
    APP_HOST = os.getenv('APP_HOST', '127.0.0.1')
    APP_PORT = os.getenv('APP_PORT', '8888')
    DEBUG_MODE = os.getenv('DEBUG_MODE', True)

    MONGODB_HOST = os.getenv('MONGODB_HOST', '127.0.0.1')
    MONGODB_PORT = os.getenv('MONGODB_PORT', '27017')

    REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
    REDIS_PORT = os.getenv('REDIS_PORT', '6379')
    REDIS_DATABASE = os.getenv('REDIS_DATABASE', '0')
    REDIS_USERNAME = os.getenv('REDIS_USERNAME', 'root')
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'root')
    CACHING_KEY_NAME = 'ad_id:{}'

    CACHING_INTERVAL = 60
