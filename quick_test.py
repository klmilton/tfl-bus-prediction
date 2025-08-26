from tfl_client import TflClient, normalise_arrivals_json

client= TflClient()

query = "Tottenham Court Road"
matches = client.search_stop_points(query=query, modes=["bus"], maz_results=5)

if not matches:
    print(f"No matches found for query: {query}")
    raise SystemExit(1)

print("matches:")
for i, m in enumerate(matches, 1):
    print(f"{i}. {m['name']} [{m['id']}] - modes = {m.get('modes')}")


chosen_id = matches [0]["id"]
print(f"`nFetching arrivals for stop/hub: {chosen_id} ...")
arrivals = client.get_bus_arrivals_for_hub_or_stop(chosen_id)

df = normalise_arrivals_json(arrivals)
if df.empty:
    print("No arrivals at the moment.")
else:
    print(df[["lineId","stationName","platformName","destinationName","eta_minutes"]].sort_values(["eta_minutes","lineId"]).head(15))