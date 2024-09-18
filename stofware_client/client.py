import json
import requests
from typing import Optional, Union, List, Dict, Any

# Types for better readability
QueryOrder = Union["ASC", "DESC", "asc", "desc"]
QueryOperator = Union["EQ", "NE", "IS", "NOT", "GT", "GE", "LT", "LTE", "IN", "NOTIN", "ILIKE", "JSONB_CONTAINS"]
AggregateFunction = Union["mean", "median", "mode", "sum", "count", "countdistinct", "nunique", "std", "min", "max"]
QueryParametersFilterValue = Union[str, bool, int, List[int], List[str]]
BooleanOperator = Union["AND", "OR"]

class StofwareClient:
    def __init__(self, base_url: str, token: Optional[str] = None):
        self.base_url = base_url
        self.token = token

    def model(self, entity: str):
        return ApiModelQuery(self, entity)

    def view(self, view_name: str):
        return ApiViewQuery(self, view_name)

    def set_token(self, token: str):
        self.token = token


    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Union[Dict, str]] = None,
        data: Optional[Union[Dict, str]] = None) -> Dict:
        """
        Makes an HTTP request to the specified endpoint with given parameters and data.

        Args:
            method (str): HTTP method (GET, POST, etc.).
            endpoint (str): API endpoint (relative to base_url).
            params (Optional[Union[Dict, str]]): Query parameters as dict or JSON string.
            data (Optional[Union[Dict, str]]): Request body as dict or JSON string.

        Returns:
            Dict: Parsed JSON response from the API.

        Raises:
            ValueError: If params or data are invalid JSON strings.
            TypeError: If params or data are not dicts or JSON strings.
            Exception: For non-OK HTTP responses.
        """
        
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"  # Ensure single slash

        # Handle params
        processed_params = self._process_input(params, "params")

        # Handle data
        processed_data = self._process_input(data, "data")

        response = requests.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=processed_params,
            json=processed_data
        )

        if not response.ok:
            raise Exception(f"{response.status_code}: {response.text}")

        try:
            return response.json()
        except json.JSONDecodeError:
            raise ValueError("Response content is not valid JSON")
    
    def _process_input(self, input_data: Optional[Union[Dict, str]], name: str) -> Optional[Union[Dict, str]]:
        """
        Processes input data for params or data by ensuring it's a dict or a valid JSON string.

        Args:
            input_data (Optional[Union[Dict, str]]): The input data to process.
            name (str): Name of the parameter ('params' or 'data') for error messages.

        Returns:
            Optional[Union[Dict, str]]: Processed input data.

        Raises:
            ValueError: If input_data is a string but not valid JSON.
            TypeError: If input_data is neither a dict nor a string.
        """
        if input_data is None:
            return None

        if isinstance(input_data, str):
            try:
                parsed = json.loads(input_data)
                if not isinstance(parsed, dict):
                    raise ValueError(f"The JSON string for {name} must represent an object/dictionary.")
                return parsed
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string for {name}: {e}") from e

        elif isinstance(input_data, dict):
            return input_data

        else:
            raise TypeError(f"{name} must be a dict or a JSON-formatted string")


class ApiBaseQuery:
    def __init__(self, client: StofwareClient):
        self.client = client
        self.params: Dict[str, Any] = {}

    def filter(self, name: str, operator: QueryOperator, value: QueryParametersFilterValue):
        if 'filters' not in self.params:
            self.params['filters'] = []
        self.params['filters'].append({"name": name, "operator": operator, "value": value})
        return self

    def append_filter(self, name: str, operator: QueryOperator, value: QueryParametersFilterValue, boolean_operator: BooleanOperator = "AND"):
        if 'filter' not in self.params:
            self.params['filter'] = {"operator": boolean_operator, "items": [{"name": name, "operator": operator, "value": value}]}
        else:
            if self.params['filter']['operator'] == boolean_operator:
                self.params['filter']['items'].append({"name": name, "operator": operator, "value": value})
            else:
                self.params['filter'] = {"operator": boolean_operator, "items": [{"name": name, "operator": operator, "value": value}] + self.params['filter']['items']}
        return self

    def set_filter(self, filter_group: Union[Dict, str]):
        """
        Sets the 'filter' parameter by accepting a dictionary or a JSON string.

        Args:
            filter_group (Union[Dict, str]): Filter criteria as a dict or JSON string.

        Raises:
            ValueError: If filter_group is a string but not valid JSON.
            TypeError: If filter_group is neither a dict nor a string.
        """
        if isinstance(filter_group, dict):
            try:
                self.params['filter'] = json.dumps(filter_group)
            except (TypeError, ValueError) as e:
                raise ValueError(f"filter_group dictionary must be serializable to JSON: {e}") from e

        elif isinstance(filter_group, str):
            # Validate that it's a valid JSON string representing a dict
            try:
                parsed = json.loads(filter_group)
                if not isinstance(parsed, dict):
                    raise ValueError("filter_group JSON string must represent an object/dictionary.")
                self.params['filter'] = filter_group  # Store the string as is
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string for filter_group: {e}") from e

        else:
            raise TypeError("filter_group must be a dict or a JSON-formatted string")


    def order_by(self, name: str, direction: QueryOrder = "DESC"):
        self.params['order_by'] = {"name": name, "direction": direction}
        return self

    def page(self, num: int):
        self.params['page'] = num
        return self

    def page_limit(self, limit: int):
        self.params['page_limit'] = limit
        return self


class ApiModelQuery(ApiBaseQuery):
    def __init__(self, client: StofwareClient, model: str):
        super().__init__(client)
        self.model = model

    def select(self, fields: List[str]):
        self.params['select'] = fields
        return self

    def include(self, includes: List[str]):
        self.params['include'] = includes
        return self

    def get_single(self, id: Union[int, str]):
        return self.client._request("GET", f"models/{self.model}/{id}", self.params)

    def get_all(self):
        return self.client._request("GET", f"models/{self.model}", self.params)

    def aggregate(self, columns: List[Dict], extra_params: Optional[Dict] = None):
        self.params['columns'] = columns
        if extra_params:
            self.params.update(extra_params)
        return self.client._request("GET", f"aggregate/{self.model}", self.params)

    def post(self, data: Dict):
        return self.client._request("POST", f"models/{self.model}", None, data)

    def put(self, id: Union[int, str], data: Dict):
        return self.client._request("PUT", f"models/{self.model}/{id}", None, data)

    def bulk_put(self, data: Dict):
        return self.client._request("PUT", f"models/{self.model}", None, data)

    def delete(self, id: Union[int, str]):
        return self.client._request("DELETE", f"models/{self.model}/{id}")

    def bulk_delete(self, data: Dict):
        return self.client._request("DELETE", f"models/{self.model}", None, data)


class ApiViewQuery(ApiBaseQuery):
    def __init__(self, client: StofwareClient, view_name: str):
        super().__init__(client)
        self.view_name = view_name

    def get_all(self):
        return self.client._request("GET", f"views/{self.view_name}", self.params)

    def aggregate(self, columns: List[Dict], extra_params: Optional[Dict] = None):
        self.params['columns'] = columns
        if extra_params:
            self.params.update(extra_params)
        return self.client._request("GET", f"views/{self.view_name}/aggregate", self.params)
