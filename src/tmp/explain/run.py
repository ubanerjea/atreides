"""
Run EXPLAIN variants on the sample tenk1/tenk2 query and write output to /out/.
"""

from runner import ExplainMode, explain_and_save

SAMPLE_QUERY = """
SELECT *
FROM tenk1 t1, tenk2 t2
WHERE t1.unique1 < 10 AND t2.unique2 < 10 AND t1.hundred < t2.hundred
"""

if __name__ == "__main__":
    for mode in ExplainMode:
        out_path = explain_and_save(SAMPLE_QUERY, mode)
        print(f"{mode.name} -> {out_path}")
