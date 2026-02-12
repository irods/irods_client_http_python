import json

def process_response(r):
    if r.status_code / 100 == 2:
        rdict = r.json()
        return {"status_code": r.status_code, "data": rdict}
    else:
        rdict = None
        if r.text != "":
            rdict = r.json()
        return {"status_code": r.status_code, "data": rdict}
