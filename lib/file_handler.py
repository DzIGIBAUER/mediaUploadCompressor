import asyncio
import json
import shutil
from tempfile import NamedTemporaryFile
from pathlib import Path
from multiprocessing import Queue, Manager, Process, cpu_count
from multiprocessing.managers import DictProxy

from fastapi import UploadFile
from ffmpeg import Error as FfmpegError

from lib.compression import compress_file
from lib.allowed_types import MimeTypeException
from lib.supabase import supabase


class FileHandler():
    def __init__(self) -> None:
 
        self.temp_dir = Path(".temp/")
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
        
        self.temp_dir.mkdir(parents=True, exist_ok=True)
 
        self.queue: Queue[tuple[str, str, Path]] = Queue()
        
        manager = Manager()
        self.upload_valid: DictProxy[str, bool] = manager.dict()
        self.upload_process_media_info: DictProxy[str, dict[str, dict]] = manager.dict()
        
        proc_list = [Process(target=self.start_worker, args=(i,)) for i in range(cpu_count()-1)]
        for p in proc_list: p.start()
    
    def generate_media_info(self, file_names: list[str] = []) -> dict[str, dict]:
        return {fn:{"valid": True, "progress": 0, "message": None} for fn in file_names}
    
    def start_worker(self, *args) -> None:
        asyncio.run(self._worker(*args))
    
    def save_upload_file_tmp(self, upload_file: UploadFile) -> Path:
        try:
            suffix = Path(upload_file.filename).suffix
            with NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                shutil.copyfileobj(upload_file.file, tmp)
                tmp_path = Path(tmp.name)
        finally:
            upload_file.file.close()
        
        return tmp_path
    
    def handle(self, files: list[UploadFile], user_id: str) -> str:
        
        file_paths: list[Path] = []
        file_names: list[str] = []
        for file in files:
            file_path = self.save_upload_file_tmp(file)
            file_paths.append(file_path)
            file_names.append(file.filename)
        
        mi = self.generate_media_info(file_names)
        
        response = (
            supabase
            .table("upload process")
            .insert({
                "user_id": user_id,
                "media_info": json.dumps(mi)
            })
            .execute()
        )
        
        
        data = response.data[0]
        id = data["id"]
        
        self.upload_process_media_info[id] = mi
        self.upload_valid[id] = True
        
        [self.queue.put((id, fn, fp)) for fn, fp in zip(file_names, file_paths)]
        
        return id
    
    async def _worker(self, i: int) -> None:
        print(f"Worker {i} started")
        try:
            while True:
                id, file_name, file_path = self.queue.get()
                
                print(f"Worker {i} handling file {file_path.name}")
                await self._process_file(id, file_name, file_path)
                
        except KeyboardInterrupt: pass
    
    async def _process_file(self, upload_id: str, file_name, file_path: Path) -> None:
        print(f"Processing file {file_path.name}...")
        
        handle_progress = lambda x, y: self.handle_progress(upload_id, file_name, x, y)
        
        try:
            compressed_file_path = await compress_file(file_path, self.temp_dir, handle_progress)
            
            print(f"{compressed_file_path} compressed")
        except Exception as e:
            if isinstance(e, MimeTypeException):
                self.invalidate_file(upload_id, file_name, str(e))
            elif isinstance(e, FfmpegError):
                self.invalidate_file(upload_id, file_name, "File compression failed.")
            else:
                raise e
    
    
    def handle_progress(self, upload_id: str, file_name: str, total_frames: int, progress: dict[str, str]) -> None:
        mi = self.upload_process_media_info[upload_id]
        mi[file_name]["progress"] = round(100/total_frames*int(progress["frame"]))
        
        if int(progress["frame"]) == total_frames:
            mi[file_name]["message"] = "File successfully compressed."
        
        self.upload_process_media_info[upload_id] = mi
        
        (
            supabase
            .table("upload process")
            .update({
                "media_info": json.dumps(mi)
            })
            .eq("id", upload_id)
            .execute()
        )
    
    def invalidate_file(self, upload_id: str, file_name: str, reason: str) -> None:
        mi = self.upload_process_media_info[upload_id]
        mi[file_name]["valid"] = False
        mi[file_name]["message"] = reason
        
        self.upload_process_media_info[upload_id] = mi
        
        self.upload_valid[upload_id] = False
        
        (
            supabase
            .table("upload process")
            .update({
                "valid": False,
                "message": "Upload failed",
                "media_info": json.dumps(mi)
            })
            .eq("id", upload_id)
            .execute()
        )

