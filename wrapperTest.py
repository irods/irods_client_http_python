import requests

url_base = ''
version_no = ''

def initialize(url, version):
    global url_base
    global version_no
    url_base = url
    version_no = version

def info():
    global url_base
    global version_no
    r = requests.get(url_base + version_no + '/info')
    print(r.json())

def authenticate(username, password):
    global url_base
    global version_no
    r = requests.post(url_base + version_no + '/authenticate', auth=(username, password))
    return(r.text)

def collections_create(token, lpath, intermediates):
    global url_base
    global version_no
    headers = {
        'Authorization': 'Bearer ' + token,
    }

    if (not isinstance(intermediates, int)):
        raise Exception('Usage:\n0 - Do not create intermediates\n1 - Create intermediates')
    if (intermediates > 1 or intermediates < 0):
        raise Exception('Usage:\n0 - Do not create intermediates\n1 - Create intermediates')

    params = {
        'op': 'create',
        'lpath': lpath,
        'create-intermediates': intermediates
    }

    r = requests.get(url_base + version_no + '/collections', params=params, headers=headers)

    if (r.status_code == 200):
        return(r.json())
    else:
        return('Error: [' + str(r.status_code) + ']')

def collections_stat(token, lpath):
    global url_base
    global version_no
    headers = {
        'Authorization': 'Bearer ' + token,
    }

    params = {
        'op': 'stat',
        'lpath': lpath,
    }

    r = requests.get(url_base + version_no + '/collections', params=params, headers=headers)

    if (r.status_code == 200):
        return(r.json())
    else:
        return('Error: [' + str(r.status_code) + ']')
    
def collections_rename(token, old_lpath, new_lpath):
    global url_base
    global version_no
    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    data = {
        'op': 'rename',
        'old-lpath': old_lpath,
        'new-lpath': new_lpath
    }

    r = requests.get(url_base + version_no + '/collections', data=data, headers=headers)

    if (r.status_code == 200):
        return(r.json())
    else:
        return('Error: [' + str(r.status_code) + ']')
