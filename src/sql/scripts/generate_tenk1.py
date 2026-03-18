# Run with: python src/sql/scripts/generate_tenk1.py
#           python src/sql/scripts/generate_tenk1.py [--rows N] [--seed N]
#
# NOTE: This is a data-generation utility — it performs TRUNCATE + COPY on tenk1.

import sys
import io
import argparse

import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from util.db import DBConnection

N_DEFAULT = 1_000_000
_STRING4 = np.array(['AAAAxx', 'HHHHxx', 'OOOOxx', 'VVVVxx'])
_COLUMNS = [
    'unique1', 'unique2', 'two', 'four', 'ten', 'twenty',
    'hundred', 'thousand', 'twothousand', 'fivethous', 'tenthous',
    'odd', 'even', 'stringu1', 'stringu2', 'string4',
]


def encode_as_name(values: np.ndarray, length: int = 6) -> list[str]:
    """Encode an integer array as base-26 uppercase strings (LSB first), padded to `length` chars."""
    n = len(values)
    v = values.astype(np.int64).copy()
    chars = np.empty((n, length), dtype=np.uint8)
    for j in range(length):
        chars[:, j] = (v % 26).astype(np.uint8) + ord('A')
        v //= 26
    flat = chars.tobytes()
    return [flat[i * length:(i + 1) * length].decode('ascii') for i in range(n)]


def generate_dataframe(n: int = N_DEFAULT, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    u1 = rng.permutation(n).astype(np.int32)
    u2 = rng.permutation(n).astype(np.int32)
    h = (u1 % 100).astype(np.int32)

    return pd.DataFrame({
        'unique1':     u1,
        'unique2':     u2,
        'two':         (u1 % 2).astype(np.int32),
        'four':        (u1 % 4).astype(np.int32),
        'ten':         (u1 % 10).astype(np.int32),
        'twenty':      (u1 % 20).astype(np.int32),
        'hundred':     h,
        'thousand':    (u1 % 1000).astype(np.int32),
        'twothousand': (u1 % 2000).astype(np.int32),
        'fivethous':   (u1 % 5000).astype(np.int32),
        'tenthous':    (u1 % 10000).astype(np.int32),
        'odd':         (h * 2).astype(np.int32),
        'even':        (h * 2 + 1).astype(np.int32),
        'stringu1':    encode_as_name(u1),
        'stringu2':    encode_as_name(u2),
        'string4':     _STRING4[u2 % 4],
    }, columns=_COLUMNS)


def load_via_copy(df: pd.DataFrame, db: DBConnection) -> None:
    buf = io.StringIO()
    df.to_csv(buf, sep='\t', header=False, index=False)
    buf.seek(0)

    col_list = ', '.join(_COLUMNS)
    with db._engine.raw_connection() as raw_conn:
        with raw_conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE public.tenk1")
            cur.copy_expert(
                f"COPY public.tenk1 ({col_list}) FROM STDIN"
                " WITH (FORMAT CSV, DELIMITER E'\\t')",
                buf,
            )
        raw_conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed tenk1 with generated data.")
    parser.add_argument('--rows', type=int, default=N_DEFAULT, metavar='N',
                        help=f"Rows to generate (default: {N_DEFAULT:,})")
    parser.add_argument('--seed', type=int, default=42,
                        help="RNG seed for reproducibility (default: 42)")
    args = parser.parse_args()

    print(f"Generating {args.rows:,} rows (seed={args.seed})...")
    df = generate_dataframe(args.rows, args.seed)

    print("Loading via COPY...")
    db = DBConnection()
    load_via_copy(df, db)
    print(f"Done — {args.rows:,} rows loaded into tenk1.")


if __name__ == '__main__':
    main()
