import logging

import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import io

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1.レビューデータの読み込み
    ## BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    ## Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    ## BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_in = "dashboard_motive/raw/scrape/"
    blob_name_out = "dashboard_motive/modify/merge/motive_modify_mergefile.csv"

    ## レビューデータ取得
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=blob_name_in)
    csv_files = [blob.name for blob in blob_list if blob.name.endswith('.csv')]

    # CSVファイルを読み込んでマージ
    df_list = []
    for file_name in csv_files:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_data = blob_client.download_blob().readall() 
        df = pd.read_csv(io.BytesIO(blob_data))
        df_list.append(df)

    merged_df = pd.concat(df_list)

    # 2.マージファイルを出力
    # マージしたDataFrameを新しいファイルとして保存
    output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_out)
    output_blob_client.upload_blob(merged_df.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)

    # 3.元ファイルの削除
    for file_delete in csv_files:
        blob_client_delete = blob_service_client.get_blob_client(container=container_name, blob=file_delete)
        blob_client_delete.delete_blob()

    return func.HttpResponse(
                status_code=200
    )
