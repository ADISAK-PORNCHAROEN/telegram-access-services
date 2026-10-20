"""Bounded parallel MTProto parts; engine owns shared request gates."""
import asyncio
import hashlib
import os
import time
from contextlib import asynccontextmanager

from app.application.concurrency import gather_owned

from telethon import custom, helpers
from telethon.errors import RpcCallFailError, ServerError
from telethon.tl import types
from telethon.tl.functions.upload import SaveFilePartRequest, SaveBigFilePartRequest
from app.infrastructure.telegram.retry import FLOOD_WAIT_ERRORS

CHUNK_SIZE = 512 * 1024  # เพดานขนาดขอนต่อคำขอของ Telegram (GetFile / Save*FilePart)
DEFAULT_STREAMS = 4  # ค่าเริ่มต้น: ยิงกี่ขอนพร้อมกันต่อ 1 ไฟล์
MAX_STREAMS = 8  # เพดานกัน FloodWait — ยิ่งเยอะยิ่งเสี่ยงโดนจำกัดความถี่จาก Telegram
FAST_THRESHOLD = 1 * 1024 * 1024  # ตั้งแต่ ~1MB ขึ้นไปคู่ขนานแล้วคุ้ม (รูป/วีดิโอ/เอกสารทุกชนิด)
API_FILE_SIZE_LIMIT = 10 * 1024 * 1024  # สัญญา Telegram: ≤10MB = SaveFilePart(+md5); >10MB = SaveBigFilePart
DOWNLOAD_STREAMS_TOTAL = 20  # เพดานคำขอโหลดค้างรวมทุกไฟล์ทุกงาน (อัลบั้มดาวน์โหลดข้ามไฟล์ก็ไม่ล้น)
UPLOAD_PART_STREAMS = 20  # เพดานคำขออัพพร้อมกัน "รวมทุกไฟล์ทุกงาน" — อัลบั้มเปิดหลายไฟล์ก็ไม่ล้น
STREAMABLE_EXTS = {".mp4", ".mkv", ".webm", ".mov"}
RETRYABLE_READ_ERRORS = (*FLOOD_WAIT_ERRORS, ServerError, RpcCallFailError)


@asynccontextmanager
async def _request_slot(gate, trace, retry=None, *, scope=None):
    if retry is not None:
        async with retry.request_slot(gate, trace, scope=scope):
            yield
        return
    slot = getattr(gate, "slot", None)
    if slot is None:
        async with gate:
            yield
    else:
        async with slot(trace):
            yield



def plan_ranges(size: int, streams: int) -> list[tuple[int, int]]:
    """แบ่งขนาดไฟล์ (byte) เป็นช่วง (เริ่ม, จบ): ครบทุกไบต์ ต่อกันไม่ซ้อน เริ่มทุกช่วงตรงขอบขอน.

    จำนวนช่วงไม่เกิน streams และไม่เกินจำนวนขอนที่มีจริง (ไฟล์เล็กกว่าหนึ่งขอน = ช่วงเดียว)
    """
    streams = max(1, min(streams, -(-size // CHUNK_SIZE)))
    base = (size // streams // CHUNK_SIZE) * CHUNK_SIZE
    if base == 0:
        return [(0, size)]
    ranges: list[tuple[int, int]] = []
    start = 0
    for _ in range(streams - 1):
        ranges.append((start, start + base))
        start += base
    ranges.append((start, size))  # ช่วงท้ายซับไบต์ที่เหลือทั้งหมด — นับรวมได้ครบทั้งไฟล์เป๊ะ
    return ranges


def is_streamable_video(name: str) -> bool:
    """นามสกุลวีดิโอที่ Telegram เล่นแบบสตรีมทันทีได้ เมื่อติดป้าย supports_streaming."""
    return os.path.splitext(name)[1].lower() in STREAMABLE_EXTS


def _ticker(callback, total: int):
    """สรุป byte ที่โหลดรวมกันทุกชิ้น → เรียก callback(ทำเสร็จ, รวม) แบบไม่สนใจจะพัง."""
    done = 0

    def touch(n: int) -> None:
        nonlocal done
        done += n
        if callback:
            try:
                callback(done, total)
            except Exception:
                pass  # progress พังได้เสมอ — ห้ามฉุดงานหลัก

    return touch


async def fast_download(client, message, dest_path: str, size: int,
                        *, streams: int = DEFAULT_STREAMS, progress=None, gate=None,
                        retry=None, trace=None) -> str:
    """ดาวน์โหลด media ของ message แบบคู่ขนานลงไฟล์ dest_path — คืนพาไฟล์ที่เขียนเสร็จ."""
    if size <= 0:
        raise ValueError("download size must be positive")
    gate = gate if gate is not None else asyncio.Semaphore(DOWNLOAD_STREAMS_TOTAL)
    ranges = plan_ranges(size, max(1, min(streams, MAX_STREAMS)))
    with open(dest_path, "wb") as prealloc:
        prealloc.truncate(size)  # จองที่ล่วงหน้า — แต่ละชิ้นเขียนลงย่อหน้าของตัวเอง

    show = _ticker(progress, size)
    handle = open(dest_path, "r+b")

    async def sweep(start: int, end: int) -> None:
        position = start
        def make_iterator(offset: int):
            return client.iter_download(
                message,
                offset=offset,
                limit=-(-(end - offset) // CHUNK_SIZE),
                request_size=CHUNK_SIZE,
                chunk_size=CHUNK_SIZE,
                file_size=size,
            )

        dl = make_iterator(position)

        async def next_chunk():
            nonlocal dl
            try:
                async with _request_slot(gate, trace, retry):
                    try:
                        return await dl.__anext__()
                    except FLOOD_WAIT_ERRORS as exc:
                        if retry is not None:
                            # Publish before releasing the permit so sibling ranges
                            # cannot dispatch while this iterator is being discarded.
                            await retry.publish_flood_wait(exc)
                        raise
            except RETRYABLE_READ_ERRORS as exc:
                # Discard only this iterator; position is the last verified write.
                try:
                    await dl.close()
                    dl = make_iterator(position)
                except BaseException:
                    if retry is not None and isinstance(exc, FLOOD_WAIT_ERRORS):
                        retry.forget_published_flood_wait(exc)
                    raise
                raise

        try:
            while position < end:
                expected = min(CHUNK_SIZE, end - position)
                try:
                    chunk = (await retry.call(next_chunk, trace, safe_retry=True)
                             if retry else await next_chunk())
                except StopAsyncIteration:
                    break
                if not chunk or len(chunk) != expected:
                    raise OSError("invalid download range length")
                handle.seek(position)
                written = handle.write(chunk)
                if written != expected:
                    raise OSError("incomplete write in download range")
                position += len(chunk)
                if trace:
                    trace.record_verified_download_bytes(len(chunk))
                show(len(chunk))
            if position != end:
                raise OSError(f"incomplete download: expected {end}, got {position}")
        finally:
            await dl.close()  # คืน sender ที่ยืมไปแชตอื่นด้วย (เมื่อไฟล์ไม่ได้อยู่ DC เดียวกัน)

    try:
        await gather_owned(*(sweep(start, end) for start, end in ranges))
    finally:
        handle.close()
    return dest_path


def _file_md5_hasher(path: str):
    """hasher md5 (อ่านรอบเดียว) — InputSizedFile ของ Telethon ต้องการหัว hasher ตรง ๆ (เรียก .hexdigest() เอง)."""
    hash_md5 = hashlib.md5()
    with open(path, "rb") as check:
        for block in iter(lambda: check.read(CHUNK_SIZE * 4), b""):
            hash_md5.update(block)
    return hash_md5


def _file_md5(path: str) -> str:
    return _file_md5_hasher(path).hexdigest()


async def fast_upload(client, src_path: str, size: int,
                      *, streams: int = DEFAULT_STREAMS, progress=None, gate=None,
                      retry=None, trace=None):
    """อัพโหลดไฟล์ตามพาขึ้น Telegram เป็นขอนคู่ขนาน — คืน handle ให้ send_file รับต่อทันที.

    ผู้เรียกใช้ต่อด้วย send_file(แชท, handle, ...) — ชื่อไฟล์พกไปด้วยเสมอ
    ทั้ง mime/attribute วีดิโอจึงเดาจากนามสกุลเหมือนตอนส่งไฟล์จากดิสก์ปกติ
    คืน InputFileBig (ไฟล์ >10MB) หรือ InputSizedFile (≤10MB, แนบ md5) — send_file รับทั้งคู่เป็นไฟล์ที่อัพแล้ว
    """
    if trace:
        trace.start_upload_diagnostics()
    outcome = "error"
    handle = None
    setup_started = time.perf_counter()
    setup_recorded = False
    try:
        if size <= 0:
            raise ValueError("ไฟล์ว่างเปล่า — อัพโหลดไม่ได้")
        if os.path.getsize(src_path) != size:
            raise ValueError("upload size differs from downloaded file")
        gate = gate if gate is not None else asyncio.Semaphore(UPLOAD_PART_STREAMS)
        part_count = -(-size // CHUNK_SIZE)
        file_id = helpers.generate_random_long()
        name = os.path.basename(src_path)
        big = size > API_FILE_SIZE_LIMIT
        hash_md5 = None if big else _file_md5_hasher(src_path)
        show = _ticker(progress, size)
        handle = open(src_path, "rb")
        if trace:
            trace.record_upload_phase("setup", time.perf_counter() - setup_started)
            setup_recorded = True
            trace.upload_diagnostics.record_setup_bytes(0 if big else size)
        state = {"next": 0}

        async def pump() -> None:
            while True:
                index = state["next"]
                state["next"] += 1
                if index >= part_count:
                    return
                prepare_started = time.perf_counter()
                try:
                    handle.seek(index * CHUNK_SIZE)
                    data = handle.read(CHUNK_SIZE)
                    request = (SaveBigFilePartRequest(
                        file_id=file_id, file_part=index, file_total_parts=part_count,
                        bytes=data
                    ) if big else SaveFilePartRequest(
                        file_id=file_id, file_part=index, bytes=data))
                finally:
                    if trace:
                        trace.record_upload_phase(
                            "part_prepare", time.perf_counter() - prepare_started)

                async def save_part():
                    request_slot = (retry.request_slot(
                        gate, trace, scope="upload_part") if retry else
                        _request_slot(gate, trace))
                    async with request_slot:
                        if trace:
                            trace.record_upload_attempt(len(data))
                        rpc_started = time.perf_counter()
                        try:
                            return await client(request)
                        finally:
                            if trace:
                                trace.record_upload_phase(
                                    "telegram_rpc_await",
                                    time.perf_counter() - rpc_started)

                saved = (await retry.call(
                    save_part, trace, safe_retry=True,
                    repeated_bytes=len(data), scope="upload_part") if retry else
                    await save_part())
                if not saved:
                    raise OSError("Telegram rejected upload part")
                if trace:
                    trace.record_uploaded_bytes(len(data))
                show(len(data))

        await gather_owned(*(pump() for _ in range(max(1, min(streams, MAX_STREAMS)))))
        outcome = "success"
        return (types.InputFileBig(file_id, part_count, name) if big
                else custom.InputSizedFile(
                    file_id, part_count, name, md5=hash_md5, size=size))
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    finally:
        try:
            if handle is not None:
                handle.close()
        finally:
            if trace:
                if not setup_recorded:
                    trace.record_upload_phase(
                        "setup", time.perf_counter() - setup_started)
                trace.finish_upload_diagnostics(outcome)
