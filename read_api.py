import requests

# 1. Define the API endpoint URL
url = "https://jsonplaceholder.typicode.com/posts"  # Example public API


try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    print("Data successfully retrieved!")
    print(data[:2]) 

except requests.exceptions.HTTPError as http_err:
     print(f"HTTP error occurred: {http_err}")
except requests.exceptions.ConnectionError as conn_err:
    print(f"Connection error occurred: {conn_err}")
except Exception as err:
    print(f"An error occurred: {err}")
