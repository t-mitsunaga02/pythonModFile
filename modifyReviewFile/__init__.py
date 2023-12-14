import logging
import azure.functions as func

import os
from openai import AzureOpenAI

def main(req: func.HttpRequest) -> func.HttpResponse:
    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_KEY"),  
        api_version="2023-10-01-preview",
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
    deployment_name='REPLACE_WITH_YOUR_DEPLOYMENT_NAME' #This will correspond to the custom name you chose for your deployment when you deployed a model. 
        
    # Send a completion call to generate an answer
    print('Sending a test completion job')
    start_phrase = 'Write a tagline for an ice cream shop. '
    response = client.completions.create(model=deployment_name, prompt=start_phrase, max_tokens=10)
    print(response.choices[0].text)
    logging.info(response.choices[0].text)

    return func.HttpResponse(
                status_code=200
    )