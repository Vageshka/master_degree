import argparse
import json
import logging
import os
import sys

_self_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(_self_dir)
logging.basicConfig(format='[%(name)s %(levelname)5.5s] %(message)s', level=logging.INFO)
logger = logging.getLogger(os.path.basename(sys.argv[0]))

from redis_wrapper import RedisWrapper


def create_parser():
    arg_parser = argparse.ArgumentParser()
    main_subparsers = arg_parser.add_subparsers(required=True, dest='entity')
    create_server_subparser(main_subparsers)
    create_db_subparser(main_subparsers)
    create_table_subparser(main_subparsers)

    return arg_parser


def create_server_subparser(main_subparsers):
    server_parser = main_subparsers.add_parser("server", help="Управление серверами в конфигурации")
    server_subparsers = server_parser.add_subparsers(required=True, dest='action')

    set_parser = server_subparsers.add_parser("add", help="Добавить информацию о сервере")
    set_parser.add_argument("--hash", type=str, required=True, help="Хеш сервера")
    set_parser.add_argument("data", nargs='+', help="Данные о сервере в формате <ключ>=<значение>")

    get_parser = server_subparsers.add_parser("get", help="Получить информацию о сервере")
    get_parser.add_argument("--hash", type=str, required=True, help="Хеш сервера")

    hash_list_parser = server_subparsers.add_parser("hash-list", help="Получить список хешей серверов")


def create_db_subparser(main_subparsers):
    db_parser = main_subparsers.add_parser("db", help="Управление базами данных в конфигурации")
    db_parser.add_argument("--name", type=str, required=True, help="Имя базы данных")
    db_subparsers = db_parser.add_subparsers(required=True, dest='action')

    set_subparser = db_subparsers.add_parser("set", help="Добавить базу данных")
    set_subparser.add_argument("--master", type=str, required=True, help="Хеш основного сервера")
    set_subparser.add_argument("--replicas", nargs='*', required=False, help="Список хешей реплик базы данных")


def create_table_subparser(main_subparsers):
    table_parser = main_subparsers.add_parser("table", help="Управление таблицами в конфигурации")
    table_parser.add_argument("--db", type=str, required=True, help="Имя базы данных")
    table_parser.add_argument("--name", type=str, required=True, help="Имя таблицы")
    table_subparsers = table_parser.add_subparsers(required=True, dest='action')

    enable_subparser = table_subparsers.add_parser("enable-sharding", help="Разрешить шардирование для таблицы")
    shard_list_parser = table_subparsers.add_parser("get-shard-list", help="Определить список шардов для таблицы")

    shard_parser = table_subparsers.add_parser("get-shard", help="Определить сервера шарда по ключу записи")
    shard_parser.add_argument("--record-key", type=str, required=True, help="Ключ записи")

    split_parser = table_subparsers.add_parser("split-shard", help="Разделить шард")
    split_parser.add_argument("--master", type=str, required=True, help="Хеш нового основного сервера шарда")
    split_parser.add_argument("--replicas", nargs='*', required=False, help="Список хешей реплик нового шарда")
    split_parser.add_argument("--shard-start", type=int, required=True, help="Начало диапазона шарда")
    split_parser.add_argument("--shard-end", type=int, required=True, help="Конец диапазона шарда")


def process_sever_actions(opts, redis_wrapper):
    if opts.action == 'add':
        data = {}
        for key_val in opts.data:
            if '=' not in key_val:
                logger.error("Данные должны иметь вид <ключ>=<значение>")
                sys.exit(1)
            key, val = key_val.split('=', maxsplit=1)
            data[key] = val
        redis_wrapper.add_server(opts.hash, data)
        logger.info("Данные добавлены")
    elif opts.action == 'get':
        data = redis_wrapper.get_server_info(opts.hash)
        if not data:
            logger.info(f"Данные сервера с хешем {opts.hash} не найдены")
        else:
            logger.info(f"Данные сервера {opts.hash}:")
            for key, val in data.items():
                logger.info(f"{key} = {val}")
    elif opts.action == 'hash-list':
        servers = redis_wrapper.get_all_servers_hash()
        logger.info(f"Список добавленных в конфигурацию серверов:")
        for index, shash in enumerate(servers):
            logger.info(f"{index}. {shash}")


def process_db_actions(opts, redis_wrapper):
    if opts.action == 'set':
        redis_wrapper.add_db(opts.name, opts.master, opts.replicas)
        logger.info("Данные добавлены")


def process_table_actions(opts, redis_wrapper):
    if opts.action == 'enable-sharding':
        redis_wrapper.enable_sharding(opts.db, opts.name)
        logger.info(f"Для таблицы {opts.db}.{opts.name} было разрешено шардирование")
    elif opts.action == 'get-shard-list':
        shard_map = redis_wrapper.get_shard_map(opts.db, opts.name)
        logger.info(f"Список шардов таблицы {opts.db}.{opts.name}:")
        for index in range(len(shard_map) - 1):
            logger.info(f"{index+1}. [{shard_map[index]}, {shard_map[index+1]})")
    elif opts.action == 'get-shard':
        shard_servers = redis_wrapper.get_shard_servers_hash_by_record(opts.db, opts.name, opts.record_key)
        logger.info(f"Основной сервер для чтения: {shard_servers['read']['master']}")
        logger.info(f"Реплики сервера для чтения: {json.dumps(shard_servers['read']['replicas'])}")
        logger.info(f"Основной сервер для записи: {shard_servers['write']['master']}")
        logger.info(f"Реплики сервера для записи: {json.dumps(shard_servers['write']['replicas'])}")
    elif opts.action == 'split-shard':
        if not redis_wrapper.is_shard_exists(opts.name, opts.shard_start, opts.shard_end, opts.master):
            logger.error(f"Не существует шард относящийся к диапазону [{opts.shard_start}, {opts.shard_end})")
            sys.exit(1)
        redis_wrapper.split_shard(opts.db, opts.name, opts.shard_start, opts.shard_end, opts.master, opts.replicas)


def main(args=None):

    arg_parser = create_parser()
    opts = arg_parser.parse_args(args=args)

    redis_wrapper = RedisWrapper()
    if opts.entity == 'server':
        process_sever_actions(opts, redis_wrapper)
    if opts.entity == 'db':
        process_db_actions(opts, redis_wrapper)
    if opts.entity == 'table':
        process_table_actions(opts, redis_wrapper)

    return 0


if __name__ == '__main__':
    sys.exit(main())