import logging
import azure.functions as func

import os
import openai

def main(req: func.HttpRequest) -> func.HttpResponse:
    #Note: The openai-python library support for Azure OpenAI is in preview.
        #Note: This code sample requires OpenAI Python library version 0.28.1 or lower.
    openai.api_type = "azure"
    openai.api_version = "2023-07-01-preview"
    openai.api_key = os.getenv("AZURE_OPENAI_KEY")
    openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")

    message_text = [{"role": "user", "content": "ハリネズミの針は再生しますか？"}]

    completion = openai.ChatCompletion.create(
    engine="gpt4Scrape",
    messages = message_text,
    temperature=0.7,
    max_tokens=800,
    top_p=0.95,
    frequency_penalty=0,
    presence_penalty=0,
    stop=None
    )

    logging.info(completion.choices[0].message['content'])

    return func.HttpResponse(
                status_code=200
    )