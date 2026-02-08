import struct
import re

MAGIC = 0x27AA

def read_stringbag(path: str):
    data = open(path, "rb").read()
    off = 0

    magic, unk, count = struct.unpack_from("<HHI", data, off)
    off += 8
    if magic != MAGIC:
        raise ValueError(f"Bad magic: {magic:#x} (expected {MAGIC:#x})")

    rows = []
    for _ in range(count):
        klen, _ = struct.unpack_from("<HH", data, off); off += 4
        key = data[off:off + klen * 2].decode("utf-16le"); off += klen * 2

        vlen, _ = struct.unpack_from("<HH", data, off); off += 4
        val = data[off:off + vlen * 2].decode("utf-16le"); off += vlen * 2

        rows.append((key, val))

    if off != len(data):
        raise ValueError(f"Trailing bytes: {len(data) - off}")

    return rows

def escape_nvarchar(s: str) -> str:
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\\n\n{2,}", r"\\n\n", s)
    s = re.sub(r"\n[^\S\n]*\n+", "\n", s)
    s = s.replace("'", "''")
    return "N'" + s + "'"

def dump_sql(
    rows,
    out_sql_path: str,
    table: str = "[dbo].[StringBagList]",
    stringbag: str = "[StringBag]",
    Text: str = "[Text]",
    include_create_table: bool = True,
    use_go: bool = True,
):
    with open(out_sql_path, "w", encoding="utf-8-sig") as f:
        if include_create_table:
            f.write(f"""IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'{table}') AND type IN ('U'))
    DROP TABLE {table}
GO

CREATE TABLE {table} (
  {stringbag} nvarchar(3000) COLLATE Thai_CI_AS NULL,
  {Text} nvarchar(3000) COLLATE Thai_CI_AS NULL
)
GO

ALTER TABLE {table} SET (LOCK_ESCALATION = TABLE)
GO

""")

        for code, unc in rows:
            code_sql = "N'" + code.replace("'", "''") + "'"
            unc_sql = escape_nvarchar(unc)

            f.write(f"INSERT INTO {table} ({stringbag}, {Text}) VALUES ({code_sql}, {unc_sql})\n")
            if use_go:
                f.write("GO\n")

if __name__ == "__main__":
    rows = read_stringbag("data09.bin")
    dump_sql(rows, "StringBagList.sql")
