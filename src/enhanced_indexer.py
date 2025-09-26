"""
Enhanced on-chain indexer with:
- compressed postings lists (delta + simple varint)
- boolean queries: AND (intersection) and OR (merge)
"""

import sqlite3, json, hashlib, struct, os
from collections import defaultdict
from typing import List, Iterator

def varint_encode(n: int) -> bytes:
    out = bytearray()
    while True:
        towrite = n & 0x7F
        n >>= 7
        if n:
            out.append(towrite | 0x80)
        else:
            out.append(towrite)
            break
    return bytes(out)

def varint_decode_stream(b: bytes) -> Iterator[int]:
    i = 0
    L = len(b)
    while i < L:
        shift = 0
        val = 0
        while True:
            byte = b[i]
            i += 1
            val |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        yield val

def compress_postings(block_numbers: List[int]) -> bytes:
    if not block_numbers:
        return b""
    out = bytearray()
    prev = 0
    for b in block_numbers:
        delta = b - prev
        out.extend(varint_encode(delta))
        prev = b
    return bytes(out)

def decompress_postings(b: bytes) -> List[int]:
    res = []
    prev = 0
    for delta in varint_decode_stream(b):
        val = prev + delta
        res.append(val)
        prev = val
    return res

class BloomFilter:
    def __init__(self, m_bits=8192, k=6):
        import hashlib, struct
        self.m = m_bits
        self.k = k
        self.bitarr = 0

    def _hashes(self, key: bytes):
        import hashlib, struct
        h = hashlib.sha256(key).digest()
        for i in range(self.k):
            start = (i*8) % (len(h)-7)
            val = struct.unpack_from(">Q", h, start)[0]
            yield val % self.m

    def add(self, key: str):
        for pos in self._hashes(key.encode('utf-8')):
            self.bitarr |= (1 << pos)

    def __contains__(self, key: str):
        for pos in self._hashes(key.encode('utf-8')):
            if not (self.bitarr >> pos) & 1:
                return False
        return True

    def to_bytes(self):
        num_bytes = (self.m + 7)//8
        return self.bitarr.to_bytes(num_bytes, 'big')

    @classmethod
    def from_bytes(cls, b: bytes, m_bits=8192, k=6):
        bf = cls(m_bits=m_bits, k=k)
        bf.bitarr = int.from_bytes(b, 'big')
        return bf

class EnhancedIndexer:
    def __init__(self, db_path="enhanced_index.db", shard_size=100, bloom_m=8192, bloom_k=6):
        self.db_path = db_path
        self.shard_size = shard_size
        self.bloom_m = bloom_m
        self.bloom_k = bloom_k

    def build_index(self, chain):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("CREATE TABLE shards(shard_id INTEGER PRIMARY KEY, start_block INTEGER, end_block INTEGER, bloom BLOB)")
        cur.execute("CREATE TABLE postings(address TEXT, shard_id INTEGER, postings BLOB, PRIMARY KEY(address, shard_id))")
        conn.commit()
        shard_postings = defaultdict(lambda: defaultdict(list))
        shard_blooms = {}
        for blk in chain:
            blknum = blk["block_number"]
            shard_id = blknum // self.shard_size
            if shard_id not in shard_blooms:
                shard_blooms[shard_id] = BloomFilter(m_bits=self.bloom_m, k=self.bloom_k)
            for tx in blk["transactions"]:
                for addr in (tx["from"], tx["to"]):
                    shard_postings[shard_id][addr].append(blknum)
                    shard_blooms[shard_id].add(addr)
                for topic in tx.get("topics", []):
                    tkey = f"topic:{topic}"
                    shard_postings[shard_id][tkey].append(blknum)
                    shard_blooms[shard_id].add(tkey)
        for shard_id, postings in shard_postings.items():
            start_block = shard_id * self.shard_size
            end_block = start_block + self.shard_size - 1
            bloom_bytes = shard_blooms[shard_id].to_bytes()
            cur.execute("INSERT INTO shards(shard_id, start_block, end_block, bloom) VALUES (?, ?, ?, ?)", 
                        (shard_id, start_block, end_block, bloom_bytes))
            for addr, blocks in postings.items():
                blocks_sorted = sorted(set(blocks))
                comp = compress_postings(blocks_sorted)
                cur.execute("INSERT INTO postings(address, shard_id, postings) VALUES (?, ?, ?)", 
                            (addr, shard_id, comp))
        conn.commit()
        conn.close()

    def postings_for(self, key: str):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT shard_id, bloom FROM shards")
        candidates = []
        for shard_id, bloom_blob in cur.fetchall():
            bf = BloomFilter.from_bytes(bloom_blob, m_bits=self.bloom_m, k=self.bloom_k)
            if key in bf:
                candidates.append(shard_id)
        res = []
        for sid in candidates:
            cur.execute("SELECT postings FROM postings WHERE address = ? AND shard_id = ?", (key, sid))
            r = cur.fetchone()
            if r:
                res.extend(decompress_postings(r[0]))
        conn.close()
        return sorted(res)

    @staticmethod
    def intersect_sorted(a: List[int], b: List[int]) -> List[int]:
        i = j = 0
        out = []
        while i < len(a) and j < len(b):
            if a[i] == b[j]:
                out.append(a[i]); i += 1; j += 1
            elif a[i] < b[j]:
                i += 1
            else:
                j += 1
        return out

    @staticmethod
    def merge_sorted(a: List[int], b: List[int]) -> List[int]:
        i = j = 0; out = []
        while i < len(a) and j < len(b):
            if a[i] == b[j]:
                out.append(a[i]); i += 1; j += 1
            elif a[i] < b[j]:
                out.append(a[i]); i += 1
            else:
                out.append(b[j]); j += 1
        while i < len(a):
            out.append(a[i]); i += 1
        while j < len(b):
            out.append(b[j]); j += 1
        res = []
        prev = None
        for x in out:
            if x != prev:
                res.append(x); prev = x
        return res

    def boolean_query(self, must_have: List[str]=[], any_of: List[str]=[]):
        lists = []
        for k in must_have:
            lists.append(self.postings_for(k))
        if lists:
            cur = lists[0]
            for lst in lists[1:]:
                cur = self.intersect_sorted(cur, lst)
            required = cur
        else:
            required = None
        any_list = []
        for k in any_of:
            any_list = self.merge_sorted(any_list, self.postings_for(k))
        if required is None and any_list:
            return any_list
        if required is None and not any_list:
            return []
        if required is not None and not any_list:
            return required
        return self.intersect_sorted(required, any_list)
