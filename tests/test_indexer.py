import pytest
from src.enhanced_indexer import EnhancedIndexer

def test_indexing_and_search():
    indexer = EnhancedIndexer()
    docs = [
        {"id": 1, "text": "Alice sends 10 tokens to Bob"},
        {"id": 2, "text": "Charlie sends 5 tokens to Dave"},
        {"id": 3, "text": "Alice sends 2 tokens to Dave"}
    ]
    indexer.index_documents(docs)

    # Simple boolean search
    results = indexer.search("Alice AND Dave")
    result_ids = [doc["id"] for doc in results]
    assert 3 in result_ids
    assert 1 not in result_ids

    results_or = indexer.search("Alice OR Charlie")
    result_ids_or = [doc["id"] for doc in results_or]
    assert set(result_ids_or) == {1, 2, 3}

def test_empty_index_search():
    indexer = EnhancedIndexer()
    results = indexer.search("Bob AND Alice")
    assert results == []
