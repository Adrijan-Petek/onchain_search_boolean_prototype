from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enhanced_indexer import EnhancedIndexer
app = FastAPI(title="Onchain Search Prototype")

class QueryRequest(BaseModel):
    must_have: list[str] = []
    any_of: list[str] = []

INDEX = EnhancedIndexer(db_path="outputs/enhanced_index.db", shard_size=100, bloom_m=8192, bloom_k=6)

@app.post("/query")
def query(req: QueryRequest):
    try:
        res = INDEX.boolean_query(must_have=req.must_have, any_of=req.any_of)
        return {"count": len(res), "blocks": res[:200]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
