from fastapi import UploadFile

allowed = {
    "image": ["jpeg"],
    "video": ["mp4", "x-matroska"],
}



def type_allowed(mime_type: str) -> bool:
    type, format = mime_type.split("/")
    
    return type in allowed and format in allowed[type]

class MimeTypeException(Exception): pass