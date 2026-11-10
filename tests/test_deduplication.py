from src.deduplication import stable_record_id

def test_record_id_is_deterministic():
    assert stable_record_id("a.log", 1, "hello") == stable_record_id("a.log", 1, "hello")
    assert stable_record_id("a.log", 1, "hello") != stable_record_id("a.log", 2, "hello")
