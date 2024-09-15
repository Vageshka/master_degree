import argparse
import logging
import os
import redis
import sys
import xxhash

REDIS_HOST = "127.0.0.1"
REDIS_PORT = "6380"
REDIS_USER = os.getenv("REDIS_USER")
REDIS_PASSWORD = os.getenv("REDIS_USER_PASSWORD")

logging.basicConfig(format='[%(name)s %(levelname)5.5s] %(message)s', level=logging.INFO)
logger = logging.getLogger(os.path.basename(sys.argv[0]))


class RedisWrapper:
    CONST_MODULO = 65536

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT,
                 user=REDIS_USER, password=REDIS_PASSWORD):
        
        self._redis = redis.Redis(host=host, port=port, username=user, password=password)
        self._version = self._redis.info('server')['redis_version']
        # logging.info(f"Redis version: {self._version}")

    @property
    def client(self) -> redis.Redis:
        return self._redis

    @staticmethod
    def get_key(*subpaths: str):
        path = '/'.join(subpaths)
        if path.startswith('/'):
            return path
        return f"/{path}"

    @staticmethod
    def get_server_key(server_hash: str):
        return RedisWrapper.get_key('servers', server_hash)

    @staticmethod
    def get_shard_hash(start: int, end: int):
        return f"{start}-{end}"

    @staticmethod
    def get_shard_rw_keys(shards_prefix, start: int, end: int, master=True):
        shard_key = RedisWrapper.get_shard_hash(start, end)
        server_type = 'master' if master else 'replicas'
        for_read = RedisWrapper.get_key(shards_prefix, shard_key, server_type, 'read')
        for_write = RedisWrapper.get_key(shards_prefix, shard_key, server_type, 'write')
        return for_read, for_write

    def add_server(self, server_hash, server_data):
        key = self.get_server_key(server_hash)
        self.client.hset(key, mapping=server_data)

    def assign_server_hash_to_key(self, key, server_hash):
        server_key = self.get_server_key(server_hash)
        if not self.client.exists(server_key):
            raise ValueError(f"Server data does not exists: {server_key}")
        self.client.set(key, server_hash)

    def assign_server_set_to_key(self, key, servers):
        self.client.delete(key)
        for server_hash in servers:
            server_key = self.get_server_key(server_hash)
            if not self.client.exists(server_key):
                raise ValueError(f"Server data does not exists: {server_key}")
            self.client.sadd(key, server_hash)

    def add_db(self, db_name: str, master_server_hash: str, replicas: list | set = None):
        db_key = self.get_key(db_name)
        master_key = self.get_key(db_key, 'master')
        self.assign_server_hash_to_key(master_key, master_server_hash)
        if replicas:
            replicas_key = self.get_key(db_key, 'replicas')
            self.assign_server_set_to_key(replicas_key, replicas)

    def is_sharding_enabled(self, db_name: str, table_name: str) -> bool:
        map_key = self.get_key(db_name, table_name, 'shards')
        if not self.client.exists(map_key):
            return False
        if self.client.llen(map_key) < 2:
            logger.warning(f"Length of list {map_key} is less than 2. Clearing...")
            self.client.delete(map_key)
            return False
        return True

    def enable_sharding(self, db_name: str, table_name: str) -> None:
        if self.is_sharding_enabled(db_name, table_name):
            return

        map_key = self.get_key(db_name, table_name, 'shards')
        db_master_key = self.get_key(db_name, 'master')
        db_replicas_key = self.get_key(db_name, 'replicas')
        if not self.client.exists(db_master_key):
            raise ValueError(f"No info about db {db_name}")

        # Copy info of main db servers into shard data
        r_master, w_master = self.get_shard_rw_keys(map_key, 0, self.CONST_MODULO)
        for shard_master_key in [r_master, w_master]:
            self.client.copy(db_master_key, shard_master_key, replace=True)
        r_replicas, w_replicas = self.get_shard_rw_keys(map_key, 0, self.CONST_MODULO, master=False)
        for shard_replicas_key in [r_replicas, w_replicas]:
            self.client.copy(db_replicas_key, shard_replicas_key, replace=True)

        # Write map
        self.client.rpush(map_key, 0, self.CONST_MODULO)

    def is_shard_ready_for_reshard(self, db_name: str, table_name: str, shard_start: int, shard_end: int) -> bool:
        if not self.is_sharding_enabled(db_name, table_name):
            return False
        shard_prefix = self.get_key(db_name, table_name, 'shards')
        r_master_key, w_master_key = self.get_shard_rw_keys(shard_prefix, shard_start, shard_end)
        r_master = self.client.get(r_master_key)
        w_master = self.client.get(w_master_key)
        if r_master is None or w_master is None:
            raise ValueError(f"One of keys was not set: {r_master_key} or {w_master_key}")

        return r_master_key == w_master_key

    def is_shard_exists(self, db_name: str, table_name: str, shard_start: int, shard_end: int) -> bool:
        shard_map = self.get_shard_map(db_name, table_name)
        for index in range(len(shard_map) - 1):
            if shard_map[index] == shard_start and shard_end == shard_map[index+1]:
                return True
        return False

    def split_shard(self, db_name: str, table_name: str, shard_start: int, shard_end: int,
                    new_server: str, new_replicas: list | set = None):
        pivot = shard_start + (shard_end - shard_start) >> 1
        shards_key = self.get_key(db_name, table_name, 'shards')

        old_shard_hash = self.get_shard_hash(shard_start, shard_end)
        first_shard_hash = self.get_shard_hash(shard_start, pivot)
        second_shard_hash = self.get_shard_hash(pivot, shard_end)

        old_shard_keys = []
        old_shard_keys.extend(self.get_shard_rw_keys(shards_key, shard_start, shard_end))
        old_shard_keys.extend(self.get_shard_rw_keys(shards_key, shard_start, shard_end, master=False))

        # [shard_start, pivot]
        for key in old_shard_keys:
            self.client.copy(key, key.replace(old_shard_hash, first_shard_hash), replace=True)

        # [pivot, shard_end]
        for index, key in enumerate(old_shard_keys):
            if index % 2 != 0:  # only read nodes
                continue
            self.client.copy(key, key.replace(old_shard_hash, second_shard_hash), replace=True)
        self.assign_server_hash_to_key(old_shard_keys[1].replace(old_shard_hash, second_shard_hash), new_server)
        if new_replicas:
            replicas_key = old_shard_keys[3].replace(old_shard_hash, second_shard_hash)
            self.assign_server_set_to_key(replicas_key, new_replicas)

        # change map
        self.client.linsert(shards_key, 'after', shard_start, pivot)

    def get_shard_map(self, db_name: str, table_name: str):
        shards_key = self.get_key(db_name, table_name, 'shards')
        return [int(val) for val in self.client.lrange(shards_key, 0, -1)]

    def get_shard_servers_hash_by_record(self, db_name: str, table_name: str, record_key: int) -> dict:
        remainder = abs(record_key % self.CONST_MODULO)
        shards_key = self.get_key(db_name, table_name, 'shards')
        shard_map = self.get_shard_map(db_name, table_name)

        right_index = 1
        while shard_map[right_index] < remainder:
            right_index += 1
        left, right = shard_map[right_index - 1], shard_map[right_index]

        r_master_key, w_master_key = self.get_shard_rw_keys(shards_key, left, right)
        r_replicas_key, w_replicas_key = self.get_shard_rw_keys(shards_key, left, right, master=False)

        return {
            'read': {
                'master': self.client.get(r_master_key).decode('utf-8'),
                'replicas': [val.decode('utf-8') for val in self.client.smembers(r_replicas_key)]
            },
            'write': {
                'master': self.client.get(w_master_key).decode('utf-8'),
                'replicas': [val.decode('utf-8') for val in self.client.smembers(w_replicas_key)]
            },
        }

    def get_server_info(self, server_hash: str) -> dict:
        key = self.get_server_key(server_hash)
        info = {}
        for key, val in self.client.hgetall(key).items():
            info[key.decode('utf-8')] = val.decode('utf-8')
        return info

    def get_all_servers_hash(self) -> list:
        key_prefix = self.get_server_key('')
        all_server_keys = self.client.keys(f"{key_prefix}*")
        return [server_key.decode('utf-8').replace(key_prefix, '') for server_key in all_server_keys]

    @staticmethod
    def get_str_hash(value: str):
        x = xxhash.xxh128()
        x.update(value)
        return x.intdigest()
