import logging
from .get_chatgpt import scrape_gpt_get
from .chatgpt_merge import gpt_modify

import azure.functions as func
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1.GPT判定処理
    gpt_data = scrape_gpt_get()
    logging.info(f"GPT:{gpt_data}")
    print(f"GPT:{gpt_data}")

    # 2.購入動機判定データ蓄積前処理
    motive_data = gpt_modify(gpt_data)
    logging.info(f"Motive:{motive_data}")
    print(f"Motive:{motive_data}")

# データフレームをCSV形式の文字列に変換し、その文字列をメモリ上のストリームに書き込む
# csv_buffer = io.StringIO()
# link_list.to_csv(csv_buffer, encoding='utf_8', index=False)

# logging.warn(link_list)

# # Blobへのアップロード
# blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_out)
# blob_client.upload_blob(link_list)


    return func.HttpResponse(
                status_code=200
    )