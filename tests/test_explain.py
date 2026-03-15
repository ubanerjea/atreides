# Run with: pytest tests/test_explain.py -v
#           pytest tests/test_explain.py -v [--keep-output]

from src.util.explain_util import ExplainMode, explain_and_save

SAMPLE_QUERY = """
SELECT *
FROM tenk1 t1, tenk2 t2
WHERE t1.unique1 < 10 AND t2.unique2 < 10 AND t1.hundred < t2.hundred
"""


def test_explain_and_save_all_modes(request):
    keep_output = request.config.getoption("--keep-output")
    generated = []
    try:
        for mode in ExplainMode:
            out_path = explain_and_save(SAMPLE_QUERY, mode)
            generated.append(out_path)
            assert out_path.exists(), f"Output file not created: {out_path}"
    finally:
        if not keep_output:
            for path in generated:
                if path.exists():
                    path.unlink()
