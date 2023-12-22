import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import asyncio
import pandas as pd
import os
import io
import time

async def main(req: func.HttpRequest) -> func.HttpResponse:
    func_url = req.url
    logging.getLogger("asyncio").setLevel(logging.INFO)

    logging.info(f"処理開始")

    # 非同期で処理を実行
    asyncio.create_task(long_running_task())

    logging.info(f"処理終了")

    # 監視用URLとともに応答を返す
    return func.HttpResponse(
        body=json.dumps({"status": "started", "monitor_url": func_url + "/status"}),
        status_code=202
    )

async def long_running_task():
    # 1.POSデータの読み込み
    ## BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    ## Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    ## BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_in = "dashboard_KPI/raw/pos/通販POS EXCEL_CUSTOMREPORT_JP_AIRPURIFIER_DAIKIN_JUN23.xlsx"
    blob_name_master_out = "dashboard_KPI/modify/data/KPI_modify_POS_master_file.csv"
    blob_name_sales_out = "dashboard_KPI/modify/data/KPI_modify_POS_sales_file.csv"
    # 読み込むシートの名前
    sheet_name = 'Hitlist_Item_24 month'

    # await asyncio.sleep(300)
    # time.sleep(300)

    ## POSデータ取得
    blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_in)
    with io.BytesIO() as input_blob:
        blob_data = blob_client_in.download_blob()
        blob_data.readinto(input_blob)
        input_blob.seek(0)
        ## DataFrame化
        df = pd.read_excel(input_blob, sheet_name=sheet_name)

        # 不要な行番号のリスト
        rows_to_delete = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]  # 10行目までが不要
        df = df.drop(index=rows_to_delete)

        # ヘッダーが2行あるカラムの開始列番号を指定する
        double_header_start_column = 7  # 11列目からヘッダーが2行ある

        # ここでインデックスをリセットすることで、正しい行番号を維持する
        df.reset_index(drop=True, inplace=True)
        df.insert(0, 'POS_ID', range(1, 1 + len(df))) #IDを振る
        df['POS_ID'] = df['POS_ID'] - 2

        # ヘッダーが1行のカラムに対する処理
        # ここで、dfの列数が2行目以降のヘッダー開始列よりも小さい場合にはこの処理をスキップ
        if df.shape[1] > double_header_start_column:
            single_header_df = df.iloc[:, :double_header_start_column].copy()
            single_header_df = single_header_df.drop(0).reset_index(drop=True)  # 1行目を削除
            single_header_df.columns = single_header_df.iloc[0]  # 1行目をカラム名に設定
            single_header_df = single_header_df.drop(0).reset_index(drop=True)  # 1行目を削除
            single_header_df = single_header_df.rename(columns={0: 'POS_ID'})

            logging.info(f"POSマスタ出力:{blob_name_master_out}")

            # POSマスタを出力
            output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_master_out)
            output_blob_client.upload_blob(single_header_df.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)

        await asyncio.sleep(610)

        # ヘッダーが2行のカラムに対する処理
        # まず、ヘッダーが2行ある部分が実際に存在するかチェック
        if df.shape[1] > double_header_start_column:
            double_header_df = df.iloc[:, double_header_start_column:].copy()
            first_row = double_header_df.iloc[0, :]  # 1行目を保存（ファイル名に使用）

            # NaNではないヘッダーの値を取得
            non_nan_headers = first_row.dropna().unique()

            # 縦持ち変換のための空のデータフレームを準備
            transformed_dfs = [] 

            # ヘッダー1行目のユニークな値ごとにデータを分割
            for header in non_nan_headers:
                header_mask = first_row == header
                if header_mask.sum() > 0:  # カラムに実際にデータが存在する場合のみ処理
                    filtered_df = double_header_df.loc[:, header_mask]
                    filtered_df.columns = filtered_df.iloc[1]  # 2行目をカラム名に設定
                    filtered_df = filtered_df.drop([0, 1]).reset_index(drop=True)  # 最初の2行を削除
                    # 元データの1列目を結合
                    id = df.iloc[:, 0].drop([0, 1]).reset_index(drop=True)
                    combined_df = pd.concat([id, filtered_df], axis=1)
                    combined_df = combined_df.rename(columns={0: 'POS_ID'})
        
                    # 結合したデータに対して縦持ち変換を実行
                    melted_df = combined_df.melt(id_vars=combined_df.columns[:1].tolist(),
                                                var_name="month",
                                                value_name=header)
                    melted_df.insert(0, 'sales_ID', range(1, 1 + len(melted_df))) #IDを振る
                    transformed_dfs.append(melted_df)

            # 縦持ち変換したデータの1つを選択
            selected_df = transformed_dfs[0]  # 最初のデータを選択

            # それ以外のデータの4列目を抽出し、選択したデータと結合
            for df in transformed_dfs[1:]:
                forth_column = df[df.columns[3]]
                selected_df = pd.concat([selected_df, forth_column], axis=1)

            logging.info(f"POS売上出力:{blob_name_sales_out}")

            # POS売上ファイルを出力
            output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_sales_out)
            output_blob_client.upload_blob(selected_df.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)