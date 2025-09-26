"""
demo_boolean.py - builds index with enhanced_indexer and runs boolean query experiments
Produces charts showing query times for AND (intersection) vs OR (merge).
"""
import time, json
from pathlib import Path
import matplotlib.pyplot as plt
from enhanced_indexer import EnhancedIndexer
import random, hashlib

def generate_synthetic_chain(num_blocks=2000, avg_txs_per_block=15, unique_addresses=5000, seed=42):
    random.seed(seed)
    chain = []
    addresses = [f"0x{hashlib.sha1(str(i).encode()).hexdigest()[:40]}" for i in range(unique_addresses)]
    tx_counter = 0
    for blk in range(num_blocks):
        txs = []
        num_txs = max(1, int(random.gauss(avg_txs_per_block, avg_txs_per_block*0.3)))
        for _ in range(num_txs):
            sender = random.choice(addresses)
            receiver = random.choice(addresses)
            tx_hash = hashlib.sha256(f"{blk}-{tx_counter}-{sender}-{receiver}".encode()).hexdigest()
            topics = [f"topic:{random.randint(0,100)}" for _ in range(random.randint(0,3))]
            txs.append({"hash": tx_hash, "from": sender, "to": receiver, "topics": topics, "block": blk})
            tx_counter += 1
        chain.append({"block_number": blk, "transactions": txs})
    return chain

OUT = Path(__file__).parent.parent / "outputs"
CHARTS = Path(__file__).parent.parent / "charts"
OUT.mkdir(parents=True, exist_ok=True)
CHARTS.mkdir(parents=True, exist_ok=True)

print("Generating chain...")
chain = generate_synthetic_chain(num_blocks=2000, avg_txs_per_block=15, unique_addresses=5000, seed=123)
idx = EnhancedIndexer(db_path=str(OUT/"enhanced_index.db"), shard_size=100, bloom_m=8192, bloom_k=6)
t0 = time.perf_counter()
idx.build_index(chain)
t1 = time.perf_counter()
print(f"Index built in {t1-t0:.2f}s")

a = chain[0]["transactions"][0]["from"]
b = chain[10]["transactions"][0]["to"]
c = "topic:5"

t0 = time.perf_counter(); pa = idx.postings_for(a); t1 = time.perf_counter(); ta = t1-t0
t0 = time.perf_counter(); pb = idx.postings_for(b); t1 = time.perf_counter(); tb = t1-t0
t0 = time.perf_counter(); pc = idx.postings_for(c); t1 = time.perf_counter(); tc = t1-t0
print(f"Postings sizes: a={len(pa)} ({ta:.6f}s), b={len(pb)} ({tb:.6f}s), c={len(pc)} ({tc:.6f}s)")

t0 = time.perf_counter(); res_and = idx.boolean_query(must_have=[a,b], any_of=[]); t1 = time.perf_counter(); tand = t1-t0
t0 = time.perf_counter(); res_or = idx.boolean_query(must_have=[], any_of=[a,b,c]); t1 = time.perf_counter(); tor = t1-t0
print(f"Boolean AND result size={len(res_and)} time={tand:.6f}s")
print(f"Boolean OR  result size={len(res_or)} time={tor:.6f}s")

def naive_boolean(chain, must_have=[], any_of=[]):
    res = []
    for blk in chain:
        present = set()
        for tx in blk["transactions"]:
            for key in (tx["from"], tx["to"]):
                if key in must_have or key in any_of:
                    present.add(key)
            for topic in tx.get("topics", []):
                if f"topic:{topic}" in any_of or f"topic:{topic}" in must_have:
                    present.add(f"topic:{topic}")
        if must_have and not all(k in present for k in must_have):
            continue
        if any_of and not any(k in present for k in any_of):
            continue
        if (not must_have) and (not any_of):
            continue
        res.append(blk["block_number"])
    return res

t0 = time.perf_counter(); naive_and = naive_boolean(chain, must_have=[a,b]); t1 = time.perf_counter(); naive_and_t = t1-t0
t0 = time.perf_counter(); naive_or = naive_boolean(chain, any_of=[a,b,c]); t1 = time.perf_counter(); naive_or_t = t1-t0
print(f"Naive AND size={len(naive_and)} time={naive_and_t:.6f}s")
print(f"Naive OR  size={len(naive_or)} time={naive_or_t:.6f}s")

import matplotlib.pyplot as plt
plt.figure(); plt.bar(["post_pa","post_pb","post_pc"], [ta,tb,tc]); plt.title("Postings retrieval times"); plt.tight_layout(); plt.savefig(CHARTS/"postings_times.png"); plt.close()
plt.figure(); plt.bar(["boolean_AND","boolean_OR","naive_AND","naive_OR"], [tand,tor,naive_and_t,naive_or_t]); plt.title("Boolean query times"); plt.tight_layout(); plt.savefig(CHARTS/"boolean_query_times.png"); plt.close()

with open(OUT/"summary_boolean.json","w") as f:
    json.dump({"pa":len(pa),"pb":len(pb),"pc":len(pc),"res_and":len(res_and),"res_or":len(res_or)}, f, indent=2)
print("Demo boolean complete. Charts saved.")
