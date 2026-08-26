import csv
import io

from run_benchmark import parse_int_list, write_records
from agent import ActionRecord


def test_parse_int_list_splits_and_converts():
    assert parse_int_list("1,2,5,10") == [1, 2, 5, 10]
    assert parse_int_list("7") == [7]


def test_write_records_produces_one_csv_row_per_record():
    records = [
        ActionRecord("forkyard", 20_000_000, 2, 0, "transfer", 12.5, True),
        ActionRecord("forkyard", 20_000_000, 2, 0, "discard", 3.1, True),
    ]
    buf = io.StringIO()
    write_records(buf, records)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    assert len(rows) == 2
    assert rows[0]["action"] == "transfer"
    assert rows[0]["ok"] == "True"
    assert rows[1]["backend"] == "forkyard"
