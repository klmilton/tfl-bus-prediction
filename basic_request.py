import requests, sys

BASE = "https://api.tfl.gov.uk"

url = f"{BASE}/line/mode/tube/status"

params = {"app_key": "0d995904e3a24b128c11eafe6e329ceb"}

r=requests.get(url, params=params)

r.raise_for_status()

for line in r.json():
    print(f"{line['name']}: {line['lineStatuses'][0]['statusSeverityDescription']}")


def find_stoppoint_id(query, max_hits=5, params=params):#, app_id=None, app_key=None):
    url = f"{BASE}/StopPoint/Search/{query}"
    #params={}
    #if app_key:
    #    params.update({"app_key": app_key})
    r = requests.get(url, params=params)
    r.raise_for_status()
    print(f"this is what a response object looks like: {r.json()}\n")
    results = r.json().get("matches", [])
    print(f"this is what a response object matches looks like: {results}\n")
    for i,m in enumerate(results[:max_hits], 1):
        print(f"{i}. {m['name']} [{m['id']}] - modes = {m.get('modes')}")
    if not results:
        raise SystemExit(f"No matches/StopPoints found for query: {query}")
    return results[0]["id"] 

def get_arrivals(stoppoint_id, params=params):
    url = f"{BASE}/StopPoint/{stoppoint_id}/Arrivals"
    r = requests.get(url, params=params)
    r.raise_for_status()
    arrivals = r.json()
    print(arrivals)
    arrivals.sort(key=lambda a: a.get("timeToStation", 1e9))
    return arrivals

if __name__ == "__main__":
    query = "Waterloo"
    params = {"app_key": "0d995904e3a24b128c11eafe6e329ceb"}
    print(f"Searching for StopPoint ID for query: {query} ...")
    stop_id = find_stoppoint_id(query, params=params)
    print(f"StopPoint ID for {stop_id}\n fetching arrivals ...")

    arrivals = get_arrivals(stop_id, params=params)
    for a in arrivals[:10]:
        print(a)
        line = a.get("lineName")
        dest= a.get("destinationName")
        mins = round(a.get("timeToStation", 0) / 60)
        platform = a.get("platformName")
        print(f"{line} -> {dest} in {mins} min {platform or ''}")