from fastapi import FastAPI, UploadFile, Form

from lib.file_handler import FileHandler


app = FastAPI()
file_handler = FileHandler()

@app.post("/handle_post/")
def handle_post(files: list[UploadFile], title: str = Form(), descriptions: list[str | None] = Form(), user_id: str = Form()):
    
    if len(files) != len(descriptions):
        return { "error": "Number of files and descriptions didn't match." }
    
    upload_id = file_handler.handle(files, title, descriptions, user_id)
    
    return {"uploadId": upload_id}
