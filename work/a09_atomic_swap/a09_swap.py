# work/a09_atomic_swap/a09_swap.py -- ghi lại partition kiểu ĐÚNG: dựng ở chỗ khuất, publish bằng rename
#
# usage: python work\a09_atomic_swap\a09_swap.py <scale> [--nap 2] [--crash] [--recover]
#   python work\a09_atomic_swap\a09_swap.py small           # dựng staging rồi swap
#   python work\a09_atomic_swap\a09_swap.py small --nap 2   # nới khoảng trống để nhìn thấy
#   python work\a09_atomic_swap\a09_swap.py small --crash   # chết đúng giữa hai lần rename
#   python work\a09_atomic_swap\a09_swap.py small --recover # dọn hậu quả của --crash
#
# Ý tưởng duy nhất của cả bài: đừng sửa thứ người đọc đang nhìn. Dựng bản mới ở
# nơi họ không thấy, rồi publish bằng một thao tác rẻ và gần như tức thời.
#
# rename chỉ đổi metadata thư mục. Nó tốn vài micro giây bất kể thư mục nặng bao
# nhiêu, và trên cùng một ổ đĩa thì hoặc xong hẳn hoặc không xảy ra.

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # repo root, để import lib
import duckdb
from lib.labpaths import lake_dir
from a09_common import DAY, rebuild_sql

scale = sys.argv[1]
lake_root = lake_dir(scale)
live = lake_root / "orders" / f"order_date={DAY}"

# .staging là ANH EM của orders/, không nằm trong orders/. Đây là điểm mấu chốt
# mà Task 4 sẽ chứng minh bằng cách làm sai: glob của người đọc là
# orders/*/*.parquet, nên bất cứ thứ gì đặt bên trong orders/ đều bị vơ vào.
staging = lake_root / ".staging" / f"order_date={DAY}"

# .trash mang dấu thời gian: mỗi lần swap để lại một thế hệ cũ riêng. Đó chính
# là bản rollback. Đừng xoá ngay -- đợi bản mới được nghiệm thu đã.
trash = lake_root / ".trash" / f"order_date={DAY}.{int(time.time())}"


def build_staging():
    """Phần CHẬM. Vài giây ở small, hàng chục giây ở full. Người đọc không biết
    gì cả, vì thư mục này nằm ngoài tầm glob của họ."""
    if staging.exists():                 # rác từ một lần chạy chết giữa chừng: vứt đi
        for f in staging.glob("*"):
            f.unlink()
        staging.rmdir()
    staging.mkdir(parents=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET threads=8;")
    con.execute(f"COPY ({rebuild_sql(scale)}) "
                f"TO '{(staging / 'data_0.parquet').as_posix()}' (FORMAT parquet)")
    print("staging ready (readers never saw a thing)", flush=True)


def rename_with_retry(src: Path, dst: Path, attempts=20, wait=0.25):
    """Trên Windows, rename thất bại với PermissionError khi BẤT KỲ process nào
    đang mở một file bên trong src: người đọc đang quét dở, Windows Search
    Indexer, phần mềm diệt virus.

    Người đọc chỉ giữ file trong vài mili giây, nên một vòng retry ngắn là đủ.
    Không có vòng này thì lỗi PermissionError đầu tiên sẽ giết script đúng vào
    thời điểm tệ nhất -- sau khi rename 1 đã chạy, trước khi rename 2 kịp chạy.
    """
    for i in range(attempts):
        try:
            os.rename(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(wait)


def swap():
    """Phần NHANH. Hai lần rename, tổng cỡ 4 mili giây ở máy chuẩn.

    Đây là chỗ phải thành thật: thay một thư mục đang sống cần HAI lần rename,
    và hai lần rename không phải một bước atomic. Trong vài mili giây giữa
    chúng, partition không tồn tại. Task 6 sẽ nới quãng đó ra cho bạn nhìn thấy.
    """
    trash.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rename_with_retry(live, trash)            # rename 1: live -> trash
    if "--crash" in sys.argv:
        print("simulated crash between the renames", flush=True)
        os._exit(1)                           # os._exit: chết ngay, không dọn dẹp gì
    if "--nap" in sys.argv:                   # cố tình nới rộng khoảng trống (Task 6)
        time.sleep(float(sys.argv[sys.argv.index("--nap") + 1]))
    rename_with_retry(staging, live)          # rename 2: staging -> live
    print(f"published in {time.time() - t0:.3f}s (old kept in .trash/{trash.name})",
          flush=True)


def recover():
    """An toàn khi crash vì bản mới (staging) và bản cũ (trash) KHÔNG BAO GIỜ
    mất cùng lúc. Ở mọi thời điểm, ít nhất một trong hai đang giữ một bản đầy đủ.
    Cái nào còn sống sẽ quyết định cách sửa.

    Thứ tự ưu tiên: staging trước (đi tiếp), trash sau (lùi lại). Đi tiếp tốt
    hơn vì staging là dữ liệu mới hơn.
    """
    if live.exists():
        print("live partition present - nothing to recover")
        return
    if staging.exists() and any(staging.glob("*.parquet")):
        os.rename(staging, live)
        print("finished the swap: staging -> live")
        return
    trashes = sorted((lake_root / ".trash").glob(f"order_date={DAY}.*"))
    if trashes:
        os.rename(trashes[-1], live)
        print("rolled back: newest trash -> live")
        return
    raise RuntimeError("no live, no staging, no trash: rebuild from raw (A11 skill)")


if __name__ == "__main__":
    if "--recover" in sys.argv:
        recover()
    else:
        build_staging()
        swap()

# Số kỳ vọng ở Task 5 (small):
#   staging ready (readers never saw a thing)
#   published in 0.004s (old kept in .trash/order_date=2026-06-19.17...)
#
# Còn người đọc ở terminal 1 giữ nguyên MỘT dòng count=179294 và không nói gì
# thêm. Kết quả của bài này là thứ VẮNG MẶT: không có số 0, không có lỗi IO,
# không có count dở dang -- trong khi vẫn đúng phép rebuild đã xé nát Task 3.
#
# TODO (stretch 1 -- pointer file, mini-Iceberg):
#   Ghi một file _current.json cạnh partition, liệt kê chính xác các file
#   Parquet tạo nên bản hiện hành. Người đọc đọc con trỏ đó rồi chỉ quét đúng
#   những file được liệt kê, không dùng glob nữa. Publish khi ấy = os.replace
#   MỘT file nhỏ, mà os.replace thì atomic thật trên Windows -- khoảng trống
#   hai-lần-rename biến mất hoàn toàn.
#   Làm xong cái này là bạn đã dựng lại phần lõi snapshot của một table format.