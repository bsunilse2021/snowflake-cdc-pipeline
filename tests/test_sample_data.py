import random

from sf_pipeline.sample_data import generate_orders


def test_generate_orders_shape_and_ids():
    rows = list(generate_orders(100, 50, random.Random(1)))
    assert len(rows) == 50
    assert rows[0]["ORDER_ID"] == 100 and rows[-1]["ORDER_ID"] == 149
    assert all(r["ORDER_AMOUNT"] >= 5 for r in rows)
