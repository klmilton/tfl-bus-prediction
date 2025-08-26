import os
import time
import json
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv
import pandas as pd
import sqlite3


load_dotenv()


TFL_BASE_URL="https://api.tfl.gov.uk"

class TflClient:
	"""A client for interacting with the Transport for London (TfL) API."""
	def __init__(self, app_key=None, user_agent="tfl-starter/0.1"):
		self.app_key= app_key or os.getenv("TFL_APP_KEY", "")
		self.session = requests.Session() # Create a session for persistent connections
		self.session.headers.update({
			"User-Agent": user_agent, # Set a default user agent - prevents 403 errors
			"Accept": "application/json", # Set the Accept header to request JSON responses
			"app_key": self.app_key
		})
		if self.app_key:
			self.session.headers.update({
				"app_key": self.app_key
			})
		else:
			# Raise an error if app_key is not provided or set in environment variables
			raise ValueError("TFL_APP_KEY must be set in environment variables or passed as an argument.")
		
	def _url(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
		"""Construct the full URL for a given path and parameters."""
		if params is None:
			params = {}
		query_string = urlencode(params)
		if query_string:
			return f"{TFL_BASE_URL}{path}?{query_string}"
		else:
			return f"{TFL_BASE_URL}{path}"
			# If no query parameters, just return the base URL with the path
			# This is useful for endpoints that do not require parameters
			# e.g. /Line/Mode/tube/Status
		#return f"{TFL_BASE_URL}{path}?{query_string}" if query_string else f"{TFL_BASE_URL}{path}"
		#return f"{TFL_BASE_URL}{path}"
	
	@retry(
		wait=wait_exponential(min=0.5, max=8),
		stop=stop_after_attempt(4),
		retry=retry_if_exception_type(requests.RequestException),
		reraise=True
	)
	def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
		"""Make a GET request to the TfL API."""
		url = self._url(path, params)
		response = self.session.get(url, timeout=20)
		if response.status_code >= 400:
			# Raise an HTTPError for bad responses
			response.raise_for_status()
		return response.json()
	
	def search_stop_points(self, query: str, modes: Optional[List[str]] = None, max_results:int = 10) -> List[Dict[str, Any]]:
		"""Use StopPoint Search to find IDs from a text query, can optionally filter by a mode e.g. 'bus','tube'."""
		params = {"query": query}
		if modes:
			params["modes"] = ",".join(modes)
		data= self._get("/StopPoint/Search", params=params)
		#data has shape {"query": "", "matches":[{...}, ...]}
		matches = data.get("matches", [])[:max_results]
		return matches
	
	def get_stop_point(self, stop_id: str) -> Dict[str, Any]:
		"""Get details of a specific stop point by its ID."""
		if not stop_id:
			raise ValueError("stop_id must be provided.")
		return self._get(f"/StopPoint/{stop_id}")
	
	def get_child_stop_ids(self, stop_or_hub_id: str) -> List[str]:
		"""For buses, arrivals are often at the 'child' stop level. If you pass a hub/bus station this returns its hild stop ID, otherwise it returns [] if None."""
		sp=self.get_stop_point(stop_or_hub_id)
		children = sp.get("children", [])
		return [c["id"] for c in children if c.get("id")]
	
	# -------- Arrivals ---------

	def get_arrivals_for_stop(self, stop_id: str) -> List[Dict[str, Any]]:
		"""Arrivals for a specific StopPoint (use child stop IDs for buses)."""
		return self._get(f"/StopPoint/{stop_id}/Arrivals")
	
	def get_bus_arrivals_for_hub_or_stop(self, stop_or_hub_id: str) -> List[Dict[str, Any]]:
		"""Convenience: if you pass a hub, fetch arrivals from all child stops, jsut that one."""
		arrivals = []
		child_ids = self.get_child_stop_ids(stop_or_hub_id)
		print('child ids',child_ids)
		print('stop_or_hub_id:', stop_or_hub_id)
		'''	
		if not child_ids:
			try:
				print('trying without for loop: ',self.get_arrivals_for_stop(child_ids))
				arrivals.extend(self.get_arrivals_for_stop(child_ids))
			except requests.HTTPError:
				print('error fetching ids')
				pass
				#skip problematic child IDs
		else:
			arrivals = self.get_arrivals_for_stop(stop_or_hub_id)
			#print('first one:',self.get_arrivals_for_stop(child_ids))'''
		if child_ids:
			for cid in child_ids:
				print(cid)
				#print(self.get_arrivals_for_stop(cid))
				try:
					stop_info = self.get_stop_point(cid)
					stop_name = stop_info.get("commonName", "Unknown Stop")
					arrivals.extend(self.get_arrivals_for_stop(cid))
					print('inner loop',len(arrivals), 'for stop name:', stop_name)
				except requests.HTTPError:
					print('error fetching arrivals for child ID:', cid)
					#skip problematic child IDs
					continue
		else:
			arrivals = self.get_arrivals_for_stop(stop_or_hub_id)
		# If no child stops, just fetch arrivals for the hub/stop ID directly
		if not arrivals:
			print('sorting out the use of child ids')
			arrivals = self.get_arrivals_for_stop(stop_or_hub_id)
		print(len(arrivals), 'arrivals fetched')
		# sort by soonest arrival
		arrivals.sort(key=lambda a: a.get("timeToStation", 1e9))
		return arrivals
	
	#---- Tube line status (example ) ----

	def get_tube_line_status(self, detail: bool = False) -> List[Dict[str, Any]]:
		"""Example: line status for tube modes. Other modes could be: 'dlr', 'overground', 'tflrail', 'tram'."""
		params = {"detail": str(detail).lower()}
		return self._get("/Line/Mode/tube/Status", params=params)
	

def normalise_arrivals_json(arrivals: List[Dict[str, Any]]) -> pd.DataFrame:
	"""Flatten common fields for analysis and saving."""
	if not arrivals:
		return pd.DataFrame()
	
	keep = ["id", "operationType","vechileId", "naptanID", "stationName", "lineID", "lineName", "platformName","destinationName", "towards", "timeToStation", "expectedArrival", "timetoLive", "modeName"]
	
	rows= []
	for a in arrivals:
		row = {k: a.get(k) for k in keep}
		rows.append(row)
	df = pd.DataFrame(rows)
	#convert seconds to minutes for readability
	print('df:', df.head(10))
	if "timeToStation" in df.columns:
		df["eta_minutes"] = (df["timeToStation"].astype(float) / 60.0).round(2)
	
	return df

def save_dataframe(df: pd.DataFrame, out_path: str) -> None:
	"""Save a DataFrame to a CSV file."""
	os.makedirs(os.path.dirname(out_path), exist_ok=True)
	if out_path.endswith(".csv"):
		df.to_csv(out_path, index=False)
	elif out_path.endswith(".json"):
		df.to_json(out_path, orient="records", lines=True)
	else:
		df.to_csv(out_path, index=False)  # Default to CSV if no extension matches




##################### SQL Database Setup #####################

#connect to SQlite database
conn = sqlite3.connect('bus_data.db')
cursor = conn.cursor()

#create a table for bus arrivals
cursor.execute('''
CREATE TABLE IF NOT EXISTS bus_arrivals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    stop_point_id TEXT,
    route TEXT,
    destination TEXT,
    expected_arrival TEXT,
    time_to_station INTEGER
)
               ''')

conn.commit()
conn.close()

def insert_into_db(data):
    conn = sqlite3.connect('bus_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO bus_arrivals (timestamp, stop_point_id, route, destination, expected_arrival, time_to_station)
    VALUES (?, ?, ?, ?, ?, ?)
    ''', (data['timestamp'], data['stop_point_id'], data['route'], data['destination'], data['expected_arrival'], data['time_to_station']))
    
    conn.commit()
    conn.close()


###################### Running the Client ######################

if __name__ == "__main__":
	""" Example usage:
	 1. Fina a stop (or hub) by name (e.g. 'Oxford Circus Station)
	  2. Fetch arrivals (handles hub -> children for buses)
	   3. save to csv
		4. also fetch tube line status"""
	client = TflClient()
	#query = os.getenv("TFL_QUERY", "Oxford Circus Station")
	query = "Green Park Station"  # Example query, can be replaced with any stop name
	matches = client.search_stop_points(query, modes=["bus", "tube"], max_results=5)
	print(f"Top matches for '{query}'")
	for m in matches:
		print(f"- {m.get('name')} [{m.get('id')}] (modes: {m.get('modes')})")

	if matches:
		#print(matches)
		if len(matches) == 1:
			chosen_id = matches[0]["id"]
			print(f"\nFetching arrivals for: {chosen_id}")
			arr= client.get_bus_arrivals_for_hub_or_stop(chosen_id)
			df = normalise_arrivals_json(arr)
			#print(df.head(10))

			ts = time.strftime("%Y-%m-%d_%H%M%S")
			out_path = f"data/raw/arrivals_{chosen_id}_{ts}.csv"
			save_dataframe(df, out_path)
			print(f"Saved {len(df)} rows to {out_path}")
		else:
			for i, m in enumerate(matches):
				chosen_id = matches[i]["id"]  # Default to first match
		#chosen_id = matches[0]["id"]
				print(f"\nFetching arrivals for: {chosen_id}, {m.get('name')}")
				arr= client.get_bus_arrivals_for_hub_or_stop(chosen_id)
				df = normalise_arrivals_json(arr)
				#print(df.head(10))

				ts = time.strftime("%Y-%m-%d_%H%M%S")
				out_path = f"data/raw/arrivals_{chosen_id}_{ts}.csv"
				save_dataframe(df, out_path)
				print(f"Saved {len(df)} rows to {out_path}")

	#Tube line status example
	try:
		status = client.get_tube_line_status(detail=False)
		print("\nTube line status (sample):")
		for s in status[:]:
			line = s.get("name")
			states = s.get("lineStatuses", [])
			summary = ",".join({st.get('statusSeverityDescription') for st in states if st.get('statusSeverityDescription')})
			print(f"- {line}: {summary}")
	except Exception as e:
		print(f"Error fetching tube line status: {e}")