import ast
import os

from dotenv import load_dotenv

from utils.Logger import logger

load_dotenv(encoding="ascii")


def is_true(x):
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.lower() in ['true', '1', 't', 'y', 'yes']
    elif isinstance(x, int):
        return x == 1
    else:
        return False


api_prefix = os.getenv('API_PREFIX', None)
authorization = os.getenv('AUTHORIZATION', '').replace(' ', '')
chatgpt_base_url = os.getenv('CHATGPT_BASE_URL', 'https://chatgpt.com').replace(' ', '')
auth_key = os.getenv('AUTH_KEY', None)
x_sign = os.getenv('X_SIGN', None)

ark0se_token_url = os.getenv('ARK' + 'OSE_TOKEN_URL', '').replace(' ', '')
if not ark0se_token_url:
    ark0se_token_url = os.getenv('ARK0SE_TOKEN_URL', None)
proxy_url = os.getenv('PROXY_URL', '').replace(' ', '')
sentinel_proxy_url = os.getenv('SENTINEL_PROXY_URL', None)
export_proxy_url = os.getenv('EXPORT_PROXY_URL', None)
file_host = os.getenv('FILE_HOST', None)
voice_host = os.getenv('VOICE_HOST', None)
impersonate_list_str = os.getenv('IMPERSONATE', '[]')
user_agents_list_str = os.getenv('USER_AGENTS', '[]')
device_tuple_str = os.getenv('DEVICE_TUPLE', '()')
browser_tuple_str = os.getenv('BROWSER_TUPLE', '()')
platform_tuple_str = os.getenv('PLATFORM_TUPLE', '()')

cf_file_url = os.getenv('CF_FILE_URL', None)
turnstile_solver_url = os.getenv('TURNSTILE_SOLVER_URL', None)

history_disabled = is_true(os.getenv('HISTORY_DISABLED', True))
pow_difficulty = os.getenv('POW_DIFFICULTY', '000032')
retry_times = int(os.getenv('RETRY_TIMES', 3))
conversation_only = is_true(os.getenv('CONVERSATION_ONLY', False))
enable_limit = is_true(os.getenv('ENABLE_LIMIT', True))
upload_by_url = is_true(os.getenv('UPLOAD_BY_URL', False))
check_model = is_true(os.getenv('CHECK_MODEL', False))
scheduled_refresh = is_true(os.getenv('SCHEDULED_REFRESH', False))
random_token = is_true(os.getenv('RANDOM_TOKEN', True))
oai_language = os.getenv('OAI_LANGUAGE', 'zh-CN')

authorization_list = authorization.split(',') if authorization else []
chatgpt_base_url_list = chatgpt_base_url.split(',') if chatgpt_base_url else []
ark0se_token_url_list = ark0se_token_url.split(',') if ark0se_token_url else []
proxy_url_list = proxy_url.split(',') if proxy_url else []
sentinel_proxy_url_list = sentinel_proxy_url.split(',') if sentinel_proxy_url else []
impersonate_list = ast.literal_eval(impersonate_list_str)
user_agents_list = ast.literal_eval(user_agents_list_str)
device_tuple = ast.literal_eval(device_tuple_str)
browser_tuple = ast.literal_eval(browser_tuple_str)
platform_tuple = ast.literal_eval(platform_tuple_str)

enable_gateway = is_true(os.getenv('ENABLE_GATEWAY', False))
auto_seed = is_true(os.getenv('AUTO_SEED', True))
force_no_history = is_true(os.getenv('FORCE_NO_HISTORY', False))
no_sentinel = is_true(os.getenv('NO_SENTINEL', False))

# ---- Browser-Driver mode ----
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
worker_dispatch_stream = os.getenv('WORKER_DISPATCH_STREAM', 'jobs.dispatch')
worker_control_stream = os.getenv('WORKER_CONTROL_STREAM', 'jobs.control')
worker_dlq_stream = os.getenv('WORKER_DLQ_STREAM', 'jobs.dlq')
worker_dispatch_group = os.getenv('WORKER_DISPATCH_GROUP', 'workers')
task_ttl_seconds = int(os.getenv('TASK_TTL_SECONDS', 10800))
lease_ttl_seconds = int(os.getenv('LEASE_TTL_SECONDS', 14400))
heartbeat_interval = float(os.getenv('HEARTBEAT_INTERVAL', 25))
async_models_str = os.getenv('ASYNC_MODELS', 'gpt-5.5-pro,o1-pro,deep-research')
async_models = [m.strip() for m in async_models_str.split(',') if m.strip()]
cookie_encryption_key = os.getenv('COOKIE_ENCRYPTION_KEY', None)
gateway_id = os.getenv('GATEWAY_ID', os.getenv('HOSTNAME', 'gw-default'))
driver_mode = os.getenv('DRIVER_MODE', 'redis')  # redis | legacy
instance_pool_backend = os.getenv('INSTANCE_POOL_BACKEND', 'redis')  # redis | file

_deprecated_in_driver_mode = {
    'ARK0SE_TOKEN_URL': ark0se_token_url,
    'SENTINEL_PROXY_URL': sentinel_proxy_url,
    'TURNSTILE_SOLVER_URL': turnstile_solver_url,
    'POW_DIFFICULTY': pow_difficulty if pow_difficulty != '000032' else None,
}
if driver_mode == 'redis':
    for env_name, env_val in _deprecated_in_driver_mode.items():
        if env_val:
            logger.warning(f"{env_name} is set but unused in DRIVER_MODE=redis (deprecated, will be removed)")

with open('version.txt') as f:
    version = f.read().strip()

logger.info("-" * 60)
logger.info(f"Chat2Api {version} | https://github.com/lanqian528/chat2api")
logger.info("-" * 60)
logger.info("Environment variables:")
logger.info("------------------------- Security -------------------------")
logger.info("API_PREFIX:        " + str(api_prefix))
logger.info("AUTHORIZATION:     " + str(authorization_list))
logger.info("AUTH_KEY:          " + str(auth_key))
logger.info("------------------------- Request --------------------------")
logger.info("CHATGPT_BASE_URL:  " + str(chatgpt_base_url_list))
logger.info("PROXY_URL:         " + str(proxy_url_list))
logger.info("EXPORT_PROXY_URL:  " + str(export_proxy_url))
logger.info("FILE_HOST:     " + str(file_host))
logger.info("VOICE_HOST:    " + str(voice_host))
logger.info("IMPERSONATE:       " + str(impersonate_list))
logger.info("USER_AGENTS:       " + str(user_agents_list))
logger.info("---------------------- Functionality -----------------------")
logger.info("HISTORY_DISABLED:  " + str(history_disabled))
logger.info("POW_DIFFICULTY:    " + str(pow_difficulty))
logger.info("RETRY_TIMES:       " + str(retry_times))
logger.info("CONVERSATION_ONLY: " + str(conversation_only))
logger.info("ENABLE_LIMIT:      " + str(enable_limit))
logger.info("UPLOAD_BY_URL:     " + str(upload_by_url))
logger.info("CHECK_MODEL:       " + str(check_model))
logger.info("SCHEDULED_REFRESH: " + str(scheduled_refresh))
logger.info("RANDOM_TOKEN:      " + str(random_token))
logger.info("OAI_LANGUAGE:      " + str(oai_language))
logger.info("------------------------- Gateway --------------------------")
logger.info("ENABLE_GATEWAY:    " + str(enable_gateway))
logger.info("AUTO_SEED:         " + str(auto_seed))
logger.info("FORCE_NO_HISTORY: " + str(force_no_history))
logger.info("------------------------- Driver ---------------------------")
logger.info("DRIVER_MODE:       " + str(driver_mode))
logger.info("REDIS_URL:         " + str(redis_url))
logger.info("ASYNC_MODELS:      " + str(async_models))
logger.info("TASK_TTL_SECONDS:  " + str(task_ttl_seconds))
logger.info("LEASE_TTL_SECONDS: " + str(lease_ttl_seconds))
logger.info("GATEWAY_ID:        " + str(gateway_id))
logger.info("-" * 60)
