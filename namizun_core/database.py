import logging
import socket
from random import uniform, choices, randint
from threading import Thread
from time import sleep, time
from namizun_core import database
from namizun_core.log import store_new_upload_agent_log, store_new_tcp_uploader_log
from namizun_core.time import get_now_time
from redis import Redis
from os import system, path
import psutil

# Setup logging
logging.basicConfig(filename='/var/log/namizun.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

buffer_ranges = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 55000, 60000, 65000]
total_upload_size_for_each_ip = 0
uploader_count = 0
real_ips = []

parameters = [
    'fake_udp_uploader_running', 'fake_tcp_uploader_running', 
    'coefficient_buffer_size', 'coefficient_uploader_threads_count', 'coefficient_buffer_sending_speed',
    'range_ips', 'in_submenu', 'coefficient_limitation',
    'total_upload_cache', 'total_download_cache', 'download_amount_synchronizer', 'upload_amount_synchronizer'
]
namizun_db = None
prefix = 'namizun_'
ip_prefix = f'{prefix}ip_'
cache_parameters = {}
buffers_weight = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

def singleton():
    global namizun_db
    if namizun_db is None:
        namizun_db = Redis()
    return namizun_db

def get_default(key):
    if key == 'range_ips':
        if path.isfile('/var/www/namizun/range_ips'):
            return open('/var/www/namizun/range_ips').read()
        else:
            system('cp /var/www/namizun/else/range_ips /var/www/namizun/')
            return open('/var/www/namizun/range_ips').read()
    elif key == 'fake_udp_uploader_running' or key == 'fake_tcp_uploader_running':
        return True
    elif key == 'coefficient_buffer_size':
        return 1
    elif key == 'coefficient_uploader_threads_count':
        return 3
    elif key == 'coefficient_buffer_sending_speed':
        return 1
    elif key == 'coefficient_limitation':
        return 6
    elif key == 'total_upload_cache':
        return 0
    elif key == 'total_download_cache':
        return 0
    elif key == 'upload_amount_synchronizer':
        return 0
    elif key == 'download_amount_synchronizer':
        return 0
    elif key == 'in_submenu':
        return False

def check_datatype(value):
    if isinstance(value, bytes):
        value = value.decode('UTF-8')
    if value == 'False':
        return False
    elif value == 'True':
        return True
    elif value == 'None':
        return None
    else:
        try:
            return int(value)
        except ValueError:
            return value

def get_parameter(key):
    if key in parameters:
        my_db = singleton()
        data = my_db.get(prefix + key)
        if data is None:
            data = get_default(key)
            my_db.set(prefix + key, str(data))
        return check_datatype(data)
    else:
        return None

def set_parameter(key, value):
    if key in parameters:
        logging.debug(f"Setting parameter {key} to {value}")
        singleton().set(prefix + key, str(value))
        return value
    else:
        return None

def get_cache_parameter(key):
    if key in parameters:
        value = cache_parameters.get(key, None)
        logging.debug(f"Retrieving parameter {key}: {value}")
        return value
    else:
        return None

def get_buffers_weight():
    global buffers_weight
    selected_buffer_size = 2 * get_cache_parameter('coefficient_buffer_size') - 1
    buffers_weight = [
        1 / 2 ** abs(buffer_size - selected_buffer_size)
        for buffer_size in range(1, 14)
    ]

def set_parameters_to_cache():
    for key in parameters:
        cache_parameters[key] = get_parameter(key)
    get_buffers_weight()
    logging.debug(f"Setting parameters to cache: {cache_parameters}")

def set_ip_port_to_database(target_ip, target_port):
    my_db = singleton()
    my_db.set(ip_prefix + target_ip, str(target_port), ex=randint(600, 6000))

def get_ip_ports_from_database():
    my_db = singleton()
    result = {}
    keys = my_db.keys(f"{ip_prefix}*")
    if len(keys) > 0:
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode('UTF-8')
            ip = key.split('_')[-1]
            result[ip] = check_datatype(my_db.get(ip_prefix + ip))
    return result

def load_ips_from_file(file_path):
    global real_ips
    with open(file_path, 'r') as file:
        real_ips = [line.strip() for line in file if line.strip()]

def get_ip_with_fixed_port():
    target_ip = choices(real_ips, k=1)[0]
    game_port = 443  # Use port 443 for all uploads
    return target_ip, game_port

def start_tcp_uploader():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        target_ip, game_port = get_ip_with_fixed_port()
        sock.connect((target_ip, game_port))
        remain_upload_size = upload_size = int(uniform(total_upload_size_for_each_ip * 0.7, total_upload_size_for_each_ip * 1.2))
        started_time = get_now_time()

        while remain_upload_size >= 0:
            selected_buffer_range = choices(buffer_ranges, buffers_weight, k=1)[0]
            buf = int(uniform(selected_buffer_range - 5000, selected_buffer_range))
            try:
                if sock.send(bytes(buf)):
                    remain_upload_size -= buf
                    sleep(0.001 * int(uniform(5, 26)) / get_cache_parameter('coefficient_buffer_sending_speed'))
            except (BrokenPipeError, ConnectionResetError) as e:
                logging.error(f"{type(e).__name__}: Failed to send data to {target_ip}:{game_port}")
                break

        sock.close()
        store_new_tcp_uploader_log(started_time, target_ip, game_port, upload_size, get_now_time())

    except Exception as e:
        logging.error(f"Exception in start_tcp_uploader: {e}")

def adjustment_of_upload_size_and_uploader_count(total_upload_size):
    global total_upload_size_for_each_ip, uploader_count
    uploader_count -= int(0.2 * uploader_count)
    total_upload_size_for_each_ip -= int(0.05 * total_upload_size_for_each_ip)
    if total_upload_size_for_each_ip * uploader_count > total_upload_size:
        adjustment_of_upload_size_and_uploader_count(total_upload_size)

def set_upload_size_and_uploader_count(total_upload_size, total_uploader_count):
    global total_upload_size_for_each_ip, uploader_count
    uploader_count = int(uniform(total_uploader_count * 0.05, total_uploader_count * 0.2))
    coefficient_of_upload = int((get_cache_parameter('coefficient_buffer_size') + 1) / 2)
    upload_size_max_range = choices([50, 100, 150], [1, 2, 3], k=1)[0]
    total_upload_size_for_each_ip = int(uniform((upload_size_max_range - 50) * coefficient_of_upload,
                                                upload_size_max_range * coefficient_of_upload)) * 1024 * 1024
    if total_upload_size_for_each_ip * uploader_count > total_upload_size:
        adjustment_of_upload_size_and_uploader_count(total_upload_size)

def multi_tcp_uploader(total_upload_size, total_uploader_count, ip_file_path):
    load_ips_from_file(ip_file_path)
    set_upload_size_and_uploader_count(total_upload_size, total_uploader_count)
    threads = []
    store_new_upload_agent_log(uploader_count, total_upload_size_for_each_ip)
    for sender_agent in range(uploader_count):
        agent = Thread(target=start_tcp_uploader)
        agent.start()
        threads.append(agent)
    for sender_agent in threads:
        sender_agent.join()
    sleep(randint(1, 5))
    return uploader_count, total_upload_size_for_each_ip

def log_resource_usage():
    cpu_usage = psutil.cpu_percent()
    memory_info = psutil.virtual_memory()
    logging.info(f"CPU Usage: {cpu_usage}%, Memory Usage: {memory_info.percent}%")

def run_uploader(total_upload_size, total_uploader_count, ip_file_path):
    try:
        while True:
            logging.info("Starting a new upload cycle")
            multi_tcp_uploader(total_upload_size, total_uploader_count, ip_file_path)
            log_resource_usage()
            sleep_duration = randint(30, 120)  # Randomized sleep duration between 30 and 120 seconds
            logging.info(f"Sleeping for {sleep_duration} seconds...")
            sleep(sleep_duration)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

# Example usage
if __name__ == "__main__":
    set_parameters_to_cache()
    run_uploader(total_upload_size=500 * 1024 * 1024, total_uploader_count=50, ip_file_path='/var/www/namizun/else/ips.txt')
