import logging
import sys
from namizun_core import database
from random import uniform, randint, choice
from time import sleep, time
from namizun_core.network import get_network_io, get_system_network_io
from namizun_core.udp import multi_tcp_uploader
from namizun_core.ip import cache_ip_ports_from_database
from namizun_core.time import get_now_hour
from namizun_core.log import store_restart_namizun_uploader_log, store_new_upload_loop_log
import psutil

# Setup logging
logging.basicConfig(filename='/var/log/namizun.log', level=logging.WARNING, format='%(asctime)s %(levelname)s:%(message)s')

logging.warning("Starting uploader.py...")

def ensure_fake_tcp_uploader_running():
    logging.warning("Setting fake_tcp_uploader_running to True...")
    try:
        database.set_parameter('fake_tcp_uploader_running', True)
        value = database.get_cache_parameter('fake_tcp_uploader_running')
        logging.warning(f"fake_tcp_uploader_running after setting: {value}")
    except Exception as e:
        logging.error(f"Error in ensure_fake_tcp_uploader_running: {e}")
        sys.exit(1)

ensure_fake_tcp_uploader_running()

def reboot_finder():
    logging.warning("Running reboot_finder...")
    retries = 3
    while retries > 0:
        try:
            new_upload_amount, new_download_amount = get_network_io()
            cached_download_amount = database.get_cache_parameter('total_download_cache')
            cached_upload_amount = database.get_cache_parameter('total_upload_cache')
            logging.warning(f"new_upload_amount: {new_upload_amount}, new_download_amount: {new_download_amount}")
            logging.warning(f"cached_download_amount: {cached_download_amount}, cached_upload_amount: {cached_upload_amount}")
            if new_upload_amount >= cached_upload_amount and new_download_amount >= cached_download_amount:
                database.set_parameter('total_download_cache', new_download_amount)
                database.set_parameter('total_upload_cache', new_upload_amount)
            else:
                logging.error("Network IO check failed, possible reboot detected.")
                sys.exit(1)
        except Exception as e:
            logging.error(f"Error in reboot_finder: {e}")
            retries -= 1
            sleep(5)
        else:
            break

reboot_finder()

def simulate_natural_traffic():
    duration = randint(1, 10)
    speed_factor = uniform(0.9, 1.1)
    return duration, speed_factor

def log_resource_usage():
    cpu_usage = psutil.cpu_percent(interval=1)
    mem_usage = psutil.virtual_memory().percent
    logging.warning(f"CPU usage: {cpu_usage}%, Memory usage: {mem_usage}%")

try:
    total_uploaded = 0
    total_upload_size = database.get_cache_parameter('total_upload_size')
    remain_upload_size = total_upload_size
    remain_uploader = randint(1, 5)
    last_upload_check_time = time()
    upload_check_interval = randint(300, 600)
    minimum_upload_activity = randint(100, 200)

    while remain_uploader > 0 and remain_upload_size > 0:
        logging.warning(f"Remaining uploaders: {remain_uploader}, Remaining upload size: {remain_upload_size}")
        uploader_count, upload_size_for_each_ip = multi_tcp_uploader(0.3 * total_upload_size, remain_uploader, '/var/www/namizun/else/ips.txt')
        logging.warning(f"uploader_count: {uploader_count}, upload_size_for_each_ip: {upload_size_for_each_ip}")
        if uploader_count == 0:
            remain_uploader -= 1
        else:
            remain_uploader -= uploader_count
        remain_upload_size -= uploader_count * upload_size_for_each_ip
        logging.warning(f"remain_uploader: {remain_uploader}, remain_upload_size: {remain_upload_size}")

        total_uploaded += uploader_count * upload_size_for_each_ip

        current_time = time()
        if current_time - last_upload_check_time > upload_check_interval:
            new_upload_amount = get_network_io()[0]
            if new_upload_amount - database.get_cache_parameter('total_upload_cache') < minimum_upload_activity:
                logging.error("Upload has stopped, exiting...")
                sys.exit(1)
            last_upload_check_time = current_time

        duration, speed_factor = simulate_natural_traffic()
        sleep(duration)
        remain_upload_size *= speed_factor

        sleep_duration = randint(3, 60)
        logging.warning(f"Sleeping for {sleep_duration} seconds...")
        sleep(sleep_duration)

        logging.warning(f"Total uploaded so far: {total_uploaded / (1024 * 1024)} MB")

        log_resource_usage()

except Exception as e:
    logging.error(f"An error occurred: {e}")
    sys.exit(1)
