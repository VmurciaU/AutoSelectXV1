from fastapi import FastAPI, Request # type: ignore
from fastapi.templating import Jinja2Templates # type: ignore


app = FastAPI()
templates = Jinja2Templates(directory="templates")

DOGS = [{"name": "Milo", "type": "Goldendoodle"}, {"name": "Jax", "type": "German Shepherd"}]

@app.get('/')
async def name(request: Request):
    return templates.TemplateResponse("home.html", {"request": request, "name": "Victor Murcia", "dogs": DOGS})