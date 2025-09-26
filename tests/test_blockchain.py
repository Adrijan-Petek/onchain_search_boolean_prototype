import time
import pytest
from src.blockchain import Blockchain, Block

def test_genesis_block():
    bc = Blockchain(difficulty=1)
    genesis_block = bc.chain[0]
    assert isinstance(genesis_block, Block)
    assert genesis_block.transactions[0]["msg"] == "Genesis Block"

def test_mine_block():
    bc = Blockchain(difficulty=1)
    bc.add_transaction("Alice", "Bob", 10)
    new_block = bc.mine_block()
    assert new_block is not None
    assert new_block.transactions[0]["sender"] == "Alice"
    assert bc.chain[-1] == new_block

def test_chain_validation():
    bc = Blockchain(difficulty=1)
    bc.add_transaction("Alice", "Bob", 10)
    bc.mine_block()
    assert bc.is_chain_valid() is True

    # Tamper with the chain
    bc.chain[1].transactions[0]["amount"] = 999
    assert bc.is_chain_valid() is False
