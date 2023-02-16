import asyncio
import json
import shutil
import uuid
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
        
        # What made me do it like this? I. Don't. Know.
        self.upload_valid: DictProxy[str, bool] = manager.dict()
        self.upload_process_media_info: DictProxy[str, dict[str, dict]] = manager.dict()
        self.upload_process_compressed_files: DictProxy[str, dict[str, Path]] = manager.dict()
        self.upload_process_post_data: DictProxy[str, dict] = manager.dict()
        
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
    
    def handle(self, files: list[UploadFile], title: str, descriptions: list[str | None], user_id: str) -> str:
        
        file_paths: list[Path] = []
        file_names: list[str] = []
        post_data: dict[str, str | None] = {
            "author_id": user_id,
            "title": title,
        }
        
        for file, desc in zip(files, descriptions):
            file_path = self.save_upload_file_tmp(file)
            file_paths.append(file_path)
            file_names.append(file.filename)
            post_data[file.filename] = desc
        
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
        self.upload_process_compressed_files[id] = {}
        self.upload_process_post_data[id] = post_data
        
        [self.queue.put((id, fn, fp)) for fn, fp in zip(file_names, file_paths)]
        
        return id
    
    async def _worker(self, i: int) -> None:
        print(f"Worker {i} started", flush=True)
        
        while True:
            id, file_name, file_path = self.queue.get()
            
            print(f"Worker {i} handling file {file_path.name}", flush=True)
            await self._process_file(id, file_name, file_path)

    
    async def _process_file(self, upload_id: str, file_name: str, file_path: Path) -> None:
        print(f"Processing file {file_path.name}...", flush=True)
        
        handle_progress = lambda x, y: self.handle_progress(upload_id, file_name, x, y)
        
        try:
            compressed_file_path = await compress_file(file_path, self.temp_dir, handle_progress)
            
            cfp = self.upload_process_compressed_files[upload_id]
            cfp[file_name] = compressed_file_path
            self.upload_process_compressed_files[upload_id] = cfp
            
            
            print(f"{compressed_file_path} compressed", flush=True)
            print(self.upload_process_compressed_files[upload_id], flush=True)
        except Exception as e:
            if isinstance(e, MimeTypeException):
                self.invalidate_file(upload_id, file_name, str(e))
            elif isinstance(e, FfmpegError):
                self.invalidate_file(upload_id, file_name, "File compression failed.")
            else:
                raise e
        finally:
            file_path.unlink(missing_ok=True)
            
            # if all files have 100% progress are valid, compression has finished 
            for file_info in self.upload_process_media_info[upload_id].values():
                if file_info["progress"] != 100 and file_info["valid"]: break
            else:
                self.finished(upload_id)
    
    def finished(self, upload_id: str) -> None:
        print(f"Upload {upload_id} finished", flush=True)
        try:
            if not self.upload_valid[upload_id]: return
            
            insert_data = {
                "author_id": self.upload_process_post_data[upload_id]["author_id"],
                "title": self.upload_process_post_data[upload_id]["title"],
                "media": [],
                "description": []
            }
            
            for original_file_name, compressed_file in self.upload_process_compressed_files[upload_id].items():
                compressed_file = Path(compressed_file)
                file_name = Path(uuid.uuid4().hex).with_suffix(compressed_file.suffix)
                path = f"media/{upload_id}/{file_name}"
                response = (
                    supabase
                    .storage()
                    .from_("media")
                    .upload(path, compressed_file)
                )
                
                file_url = supabase.storage().from_("media").get_public_url(path)
                insert_data["media"].append(file_url)
                insert_data["description"].append(self.upload_process_post_data[upload_id][original_file_name])
            
            
            response = (
                supabase
                .table("posts")
                .insert(insert_data)
                .execute()
            )
            
            data = response.data[0]
            
            mi = self.upload_process_media_info[upload_id]
            
            self.upload_process_media_info[upload_id] = mi
            
            (
                supabase
                .table("upload process")
                .update({
                    "post_id": data["id"],
                    "message": "Upload complete",
                    "media_info": json.dumps(mi)
                })
                .eq("id", upload_id)
                .execute()
            )
                
        finally:
            del self.upload_valid[upload_id]
            del self.upload_process_media_info[upload_id]
            del self.upload_process_compressed_files[upload_id]
            del self.upload_process_post_data[upload_id]
        
    
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

