from typing import Callable, Any, Literal
from random import choices
from string import ascii_letters
from subprocess import Popen
from pathlib import Path
import re
import ffmpeg
import magic

from lib.allowed_types import type_allowed, MimeTypeException

dir = Path(__file__).parent / "ffmpeg/bin"
FFMPEG_PATH = dir / "ffmpeg"
FFPROBE_PATH = dir / "ffprobe"

SUFFIXES = {
    "image": "png",
    "video": "mp4",
    "audio": "mp3"
}

OUTPUT_KWARGS = {
    "video": {"format": "matroska", "vcodec": "libx265", "crf": 28},
    "image": {"update": True}
}

PROGRESS_RE = re.compile(br"[\w\W]*progress=[a-z]+\n")


def parse_progress(progress_report: str | bytes) -> dict[str, str]:
    if not isinstance(progress_report, str):
        progress_report = progress_report.decode("utf-8")
        
    return {key_value[0]: key_value[1].strip() for key_value in [key_value.split("=") for key_value in progress_report.split("\n") if key_value]}

async def compress_file(file_path: Path, output_dir: Path, progress_callback: Callable[[int, dict[str, str]], Any]) -> Path:
    
    with file_path.open("rb") as file:
        mime_type = magic.from_buffer(file.read(2048), mime=True)
    
    if not type_allowed(mime_type):
        raise MimeTypeException(
            f"File {file_path.name} has unsupported MIME type {mime_type}"
        )
    
    file_type = mime_type.split("/")[0]
    
    info = ffmpeg.probe(file_path, cmd=str(FFPROBE_PATH), loglevel="error")
    total_frames = int(info["streams"][0].get("nb_frames", 1))
    
    suffix = SUFFIXES[file_type]
    output_kwargs = OUTPUT_KWARGS[file_type]
    
    stem = "".join(choices(ascii_letters, k=8))
    
    output_path = output_dir / Path(f"{stem}.{suffix}")
    
    process: Popen = (
        ffmpeg
            
            .input(str(file_path), progress="pipe:", loglevel="error")
            .output(str(output_path), **output_kwargs)
            .global_args('-nostats')
            .run_async(cmd=str(FFMPEG_PATH), pipe_stdout=True)
    )
    
    assert process.stdout
    
    bytes_ = bytearray()
    while True:
        in_byte: bytes = process.stdout.read(1)
        bytes_ += in_byte
        if PROGRESS_RE.match(bytes_):
            progress = parse_progress(bytes_)
            progress_callback(total_frames, progress)
            bytes_.clear()
        elif not in_byte:
            break
    
    process.wait()
    
    return output_path
    


class CompressionError(Exception): pass
