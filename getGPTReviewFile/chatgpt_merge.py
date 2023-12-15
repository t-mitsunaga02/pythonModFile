import pandas as pd
from azure.storage.blob import BlobServiceClient
import os
import io

def gpt_modify(gpt_data):
    # CSVファイルを読み込む
    # BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    # Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_diff_in = "dashboard_motive/modify/GPT/motive_modify_GPTfile.csv"

    blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_diff_in)
    blob_data = blob_client_in.download_blob()
    diff_data = blob_data.readall()
    ## DataFrame化
    df2 = pd.read_csv(io.BytesIO(diff_data))

    # ①から[review_ID][reason]カラムのみを抽出する
    df1_extracted = gpt_data[['review_id', 'reason']]

    # ①に[reason_ID]カラムを追加し、②の[reason_ID]カラムの最大値+1から始まる値を割り当てる
    max_reason_id_2 = df2['reason_id'].max()
    df1_extracted.insert(0, 'reason_id' , range(max_reason_id_2 + 1, max_reason_id_2 + 1 + len(df1_extracted)))

    # 新しいデータと過去データをユニオン結合し、CSVに保存
    union_df = pd.concat([df2,df1_extracted], ignore_index=True)
    # union_df.to_csv('購入動機データ.csv', index=False) 
    blob_client_in.upload_blob(union_df.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)

    return union_df
