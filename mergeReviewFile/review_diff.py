import logging

import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import io

def review_diff(merge_data):
    # 差分判定用ディレクトリ、ファイル存在チェック
    directory = "dashboard_motive/modify/diff/"

    # 1.過去レビューデータの読み込み
    # BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    # Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_diff_out = "dashboard_motive/modify/diff/motive_modify_difffile.csv"
    blob_name_merge_out = "dashboard_motive/modify/merge/motive_modify_mergefile.csv"

    # 2.差分・一括のファイル出力処理
    # 過去レビューデータの存在チェック
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_diff_out)
    if blob_client.exists():

        # 過去データがある場合
        # 差分実行
        print("差分")
        logging.info("差分")
        # 過去マスタデータ読み込み
        blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_merge_out)
        blob_data = blob_client_in.download_blob()
        review_data = blob_data.readall()
        ## DataFrame化
        df2 = pd.read_csv(io.BytesIO(review_data))

        # Itemが同じでPOS_IDが異なるデータを見つけ、新しくスクレイピングしたデータのPOS_IDに合わせる
        for item in merge_data['item'].unique():
            pos_id_1 = merge_data[merge_data['item'] == item]['pos_id'].iloc[0]
            df2.loc[df2['item'] == item, 'pos_id'] = pos_id_1

        # commentカラムで新しいデータに存在するレコードを抽出し、新しいデータフレームを作成
        df3 = merge_data[~merge_data['comment'].isin(df2['comment'])]

        # review_IDカラムを追加し、過去データのreview_IDカラムの最大値+1から始まる値を割り当てる
        max_review_id = df2['review_id'].max()
        df3.insert(0, 'review_id', range(max_review_id + 1, max_review_id + 1 + len(df3)))


        # データをCSVファイルとして出力
        output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_diff_out)
        output_blob_client.upload_blob(df3.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True) #chatGPT用のデータ

        # 新しいMasterデータを作成
        union_df = pd.concat([df2,df3], ignore_index=True)
        output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_merge_out)
        output_blob_client.upload_blob(union_df.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True) #マスタ用のデータ

    else :
        # 過去データがない（初回実行）場合
        # 初回実行
        # レビューIDの採番
        print("一括")
        logging.info("一括")
        merge_data.insert(0, 'review_id', range(1, 1 + len(merge_data)))

        # データをCSVファイルとして出力
        output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_diff_out)
        output_blob_client.upload_blob(merge_data.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True) #chatGPT用のデータ

        output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_merge_out)
        output_blob_client.upload_blob(merge_data.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True) #マスタ用のデータ

    return merge_data
