import requests
auth = requests.utils.get_netrc_auth("https://urs.earthdata.nasa.gov")
print(auth)