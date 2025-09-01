from fastapi import FastAPI

app = FastAPI()




@app.get('/antisemitic-with-weapon')
def get_antisemitic_with_weapon():
    pass


@app.get('/two-weapons')
def two_weapons():
    pass