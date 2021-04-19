# this file contains OpenWhisk config

API_HOST = "http://localhost:3233"
NAMESPACE = "guest"
ACTIVATIONS_ENDPOINT = f"{API_HOST}/api/v1/namespaces/{NAMESPACE}/activations"
ACTIVATIONS_PARAMS = {"limit": 200, "docs": True}
INVOKE_ENDPOINT = f"{API_HOST}/api/v1/namespaces/{NAMESPACE}/actions/"
INVOKE_PARAMS = {"blocking": False, "result": False}
USER = "23bc46b1-71f6-4ed5-8c54-816aa4f8c502"
PASSWORD = "123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP"

DB_FILENAME = "./activations.db"
