from fastapi import FastAPI, UploadFile, Form

from lib.file_handler import FileHandler


app = FastAPI()
file_handler = FileHandler()

@app.post("/handle_post/")
def handle_post(files: list[UploadFile], user_id: str = Form()):
    upload_id = file_handler.handle(files, user_id)
    
    return {"uploadId": upload_id}
