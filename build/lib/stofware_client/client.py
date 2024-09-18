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

    def _request(self, method: str, endpoint: str, params: Optional[Dict] = None, data: Optional[Dict] = None) -> Dict:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        url = f"{self.base_url}/{endpoint}"

        if params:
            params = {k: (v if not isinstance(v, list) else str(v)) for k, v in params.items()}
        
        response = requests.request(method, url, headers=headers, params=params, json=data)

        if not response.ok:
            raise Exception(f"{response.status_code}: {response.text}")

        return response.json()


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

    def set_filter(self, filter_group: Dict):
        self.params['filter'] = filter_group
        return self

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
