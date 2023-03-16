
import json
import pandas as pd
from utils import utils as u
from dotenv import load_dotenv
import os 

load_dotenv()

myAPI_ENDPOINT_AUTH     = os.getenv("API_ENDPOINT_AUTH")
myAPI_HEATHERS_AUTH     = os.getenv("API_HEATHERS_AUTH")
myAPI_BODY_AUTH         = os.getenv("API_BODY_AUTH")

myAPI_ENDPOINT_RESPONSES= os.getenv("API_ENDPOINT_RESPONSES")
myAPI_API_HEATHERS      = os.getenv("API_HEATHERS")

class Extract():
    def __init__(self) -> None:
        self.process = 'Extract Process'

    def get_api_token(self):
        res_token = u.api_call_request('POST', myAPI_ENDPOINT_AUTH, json.loads(myAPI_HEATHERS_AUTH), myAPI_BODY_AUTH)
        return res_token

    def get_api_response(self, pSurvey):
        token = self.get_api_token()
        # token['access_token']
        vAPI_ENDPOINT_RESPONSES = myAPI_ENDPOINT_RESPONSES.replace("[param_survey]", pSurvey)
        vAPI_API_HEATHERS = json.loads(myAPI_API_HEATHERS)
        vAPI_API_HEATHERS['Authorization'] = "Bearer "+token['access_token']
        # res = vAPI_API_HEATHERS
        res = u.api_call_request('GET', vAPI_ENDPOINT_RESPONSES, vAPI_API_HEATHERS, '')
        return res

    def read_csv(self, path_file, delimiter, name_headers, headers=None):
       
        df = pd.read_csv(path_file,sep=delimiter,names=name_headers, header=headers)

        df.reset_index(inplace=False)
        
        return df

    def read_tsv(self, path_file, name_headers, headers=None):

        df = pd.read_csv(path_file,sep='\t', header=headers, names=name_headers)

        df.reset_index(inplace=False)
        
        return df

    def read_xml(self, path_file):

        df = pd.read_xml(path_file)
        df.reset_index(inplace=False)
        
        return df

    def read_json(self, path_file):

        df = pd.read_json(path_file)
        df.reset_index(inplace=False)

        return df

