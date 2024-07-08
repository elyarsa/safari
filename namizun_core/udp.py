from namizun_core import database
from threading import Thread
from random import uniform, randint
from time import sleep
from random import choices
import socket
from namizun_core.log import store_new_upload_agent_log, store_new_tcp_uploader_log
from namizun_core.time import get_now_time
import logging

buffer_ranges = [5000, 10000, 15000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 55000, 60000, 65000]
total_upload_size_for_each_ip = 0
uploader_count = 0
real_ips = []

def load_ips_from_file(file_path):
    global real_ips
    with open(file_path, 'r') as file:
        real_ips = [line.strip() for line in file if line.strip()]

def get_ip_with_fixed_port():
    target_ip = choices(real_ips, k=1)[0]
    game_port = 443  # Use port 443 for all uploads
    return target_ip, game_port

def start_tcp_uploader():
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_ip, game_port = get_ip_with_fixed_port()
            sock.connect((target_ip, game_port))
            remain_upload_size = upload_size = int(uniform(total_upload_size_for_each_ip * 0.7, total_upload_size_for_each_ip * 1.2))
            started_time = get_now_time()
            while remain_upload_size >= 0:
                selected_buffer_range = choices(buffer_ranges, database.buffers_weight, k=1)[0]
                buf = int(uniform(selected_buffer_range - 5000, selected_buffer_range))
                try:
                    if sock.send(bytes(buf)):
                        remain_upload_size -= buf
                        sleep(0.001 * int(uniform(5, 26)) / database.get_cache_parameter('coefficient_buffer_sending_speed'))
                except BrokenPipeError:
                    logging.error(f"BrokenPipeError: Failed to send data to {target_ip}:{game_port}")
                    retries += 1
                    sleep(2 ** retries)  # Exponential backoff
                    break
                except Exception as e:
                    logging.error(f"Exception in start_tcp_uploader: {e}")
                    retries += 1
                    sleep(2 ** retries)  # Exponential backoff
                    break
            sock.close()
            store_new_tcp_uploader_log(started_time, target_ip, game_port, upload_size, get_now_time())
            return  # Exit the function after successful upload
        except Exception as e:
            logging.error(f"Exception in start_tcp_uploader: {e}")
            retries += 1
            sleep(2 ** retries)  # Exponential backoff

    logging.error("Max retries reached, exiting start_tcp_uploader")

def adjustment_of_upload_size_and_uploader_count(total_upload_size):
    global total_upload_size_for_each_ip, uploader_count
    uploader_count -= int(0.2 * uploader_count)
    total_upload_size_for_each_ip -= int(0.05 * total_upload_size_for_each_ip)
    if total_upload_size_for_each_ip * uploader_count > total_upload_size:
        adjustment_of_upload_size_and_uploader_count(total_upload_size)

def set_upload_size_and_uploader_count(total_upload_size, total_uploader_count):
    global total_upload_size_for_each_ip, uploader_count
    uploader_count = int(uniform(total_uploader_count * 0.05, total_uploader_count * 0.2))
    coefficient_of_upload = int((database.get_cache_parameter('coefficient_buffer_size') + 1) / 2)
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
