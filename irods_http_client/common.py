import json


def process_response(r):
    rdict = r.json() if r.text != "" else None
    return {"status_code": r.status_code, "data": rdict}

def check_token(t):
    if t == None:
        raise RuntimeError(
            "No token set. Use setToken() to set the auth token to be used."
        )

def validate_instance(x, expected_type):
    if not isinstance(x, expected_type):
        raise TypeError

def validate_0_or_1(x):
    validate_instance(x, int)
    if x not in [0, 1]:
        raise ValueError(f"{x} must be 0 or 1")

def validate_gte_zero(x):
    validate_instance(x, int)
    if not x >= 0:
        raise ValueError(f"{x} must be >= 0")

def validate_gte_minus1(x):
    validate_instance(x, int)
    if not x >= -1:
        raise ValueError(f"{x} must be >= 0, or flag value of -1")
