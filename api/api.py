import json
import requests
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from config import AWS_REGION, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_ENDPOINT


class ApiClient:
    def __init__(self):
        self.endpoint = AWS_ENDPOINT
        self.region = AWS_REGION
        self.service = "execute-api"

        # Create AWS credentials session
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        self.credentials = session.get_credentials()

    def sign_request(self, method, path, body=None):
        """Create and sign AWS request"""
        url = f"{self.endpoint}{path}"

        aws_request = AWSRequest(
            method=method,
            url=url,
            data=json.dumps(body) if body else None,
            headers={"Content-Type": "application/json"}
        )
        aws_request.prepare()  # Prepare request for signing

        # Sign the request
        SigV4Auth(self.credentials, self.service, self.region).add_auth(aws_request)

        return aws_request

    def get(self, path):
        """Send GET request"""
        try:
            request = self.sign_request("GET", path)
            response = requests.get(request.url, headers=dict(request.headers))
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print("GET API Error:", e)
            raise e

    def post(self, path, data):
        """Send POST request"""
        try:
            request = self.sign_request("POST", path, data)
            response = requests.post(request.url, headers=dict(request.headers), data=request.data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print("POST API Error:", e)
            raise e
