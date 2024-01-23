import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import unicodedata
import os
import io
import time

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1.POSデータの読み込み

    ## BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    ## Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    ## BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_in = "dashboard_KPI/raw/"
    blob_name_master_out = "dashboard_KPI/modify/data/KPI_modify_POS_master_file.csv"
    blob_name_sales_out = "dashboard_KPI/modify/data/KPI_modify_POS_sales_file.csv"
    # 読み込むシートの名前
    sheet_name = 'Hitlist_Item_24 month'

    ## POSデータ取得
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=blob_name_in)

    # 各Blobに対する処理
    for blob in blob_list:
        if "/" not in blob.name[len(blob_name_in):]:
            if blob.name.endswith('.xlsx'):
                logging.info(f"処理ファイル:{blob.name}")
                blob_client = blob_service_client.get_blob_client(container_name, blob.name)
                with io.BytesIO() as input_blob:
                    blob_data = blob_client.download_blob()
                    blob_data.readinto(input_blob)
                    input_blob.seek(0)
                    ## DataFrame化
                    df = pd.read_excel(input_blob, sheet_name=sheet_name, header=[18,19])

                    ## データ集約のためヘッダーを1行に結合
                    double_header_start_column = 7  # 7列目からヘッダーが2行ある
                    # 最初の7列の1行目ヘッダーを2行目のヘッダーで置き換える
                    for i in range(double_header_start_column):
                        # 2行目のヘッダーを取得して全角を半角に変換
                        new_header = unicodedata.normalize('NFKC', df.columns.values[i][1])
                        df.columns.values[i] = new_header
                    # ヘッダーを結合
                    df.columns = ["-".join(cols) for cols in df.columns]
                    # ヘッダー結合により名前が変更したため置換する
                    for j in range(double_header_start_column):
                        # ヘッダーの文字列置換
                        replace_header = df.columns.values[j].replace('-','')
                        df.columns.values[j] = replace_header

                    ## GroupByにてデータの集約
                    df.reset_index(drop=True, inplace=True)
                    # GroupBy用に各カラムの集約情報を設定
                    sales_units_columns = [col for col in df.columns if 'Sales Units' in col]
                    sales_value_columns = [col for col in df.columns if 'Sales Value JPY wo. VAT' in col]
                    price_columns = [col for col in df.columns if 'Price JPY wo. VAT' in col]
                    agg_dict = {'HUMIDIFIER FCT.': 'last','MAX.TANKCAP.LTR': 'last','FirstActivityMonthly': 'last'}
                    agg_dict.update({col: 'sum' for col in sales_units_columns})
                    agg_dict.update({col: 'sum' for col in sales_value_columns})
                    agg_dict.update({col: 'median' for col in price_columns})
                    # GroupBy実行
                    df_group = df.groupby(['Item', 'BRAND', 'TYPE AIRTREAT GC', 'APPLICABLE AREA'], as_index=False).agg(agg_dict)
                    logging.info(f"集約結果:{df_group}")

                    ## POS_IDの採番
                    # 過去ファイルの読み込み
                    blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_master_out)
                    blob_data = blob_client_in.download_blob()
                    review_data = blob_data.readall()
                    ## DataFrame化
                    df_old = pd.read_csv(io.BytesIO(review_data))

                    # キーでマージ（右結合）
                    merged = pd.merge(df_old, df_group, on=['Item', 'BRAND', 'TYPE AIRTREAT GC', 'APPLICABLE AREA'], how='right')
                    # 既存のPOS_IDの最大値を取得
                    max_pos_id = df_old['POS_ID'].max()
                    # ヒットしなかったデータ（newにしかないデータ）に新しいIDを振る
                    new_pos_ids = pd.RangeIndex(start=max_pos_id + 1, stop=max_pos_id + 1 + len(merged))
                    merged.loc[merged['POS_ID'].isna(), 'POS_ID'] = new_pos_ids[:len(merged[merged['POS_ID'].isna()])]
                    print(merged)

                    ## データの成型
                    # マージしたデータから必要な列のみを選択（oldのカラムをベースにする）
                    # 特定のカラムを除外
                    column_to_exclude = ['HUMIDIFIER FCT._y','MAX.TANKCAP.LTR_y','FirstActivityMonthly_y']
                    merged_without_column = merged.drop(column_to_exclude, axis=1)
                    # カラムリネーム＆並び替え
                    new_order_column = ['POS_ID', 'Item', 'BRAND', 'HUMIDIFIER FCT._x', 'TYPE AIRTREAT GC', 'APPLICABLE AREA', 'MAX.TANKCAP.LTR_x', 'FirstActivityMonthly_x']
                    remaining_columns = merged_without_column.columns[len(new_order_column):] # 並び替えるカラム以外はそのままの順番にしたい
                    new_order_column = new_order_column + remaining_columns.tolist() # 並び替えたカラム＋そのままのカラム
                    merged_new_order_column = merged_without_column[new_order_column]
                    merged_new_order_column = merged_new_order_column.rename(columns={'HUMIDIFIER FCT._x': 'HUMIDIFIER FCT.', 'MAX.TANKCAP.LTR_x': 'MAX.TANKCAP.LTR', 'FirstActivityMonthly_x': 'FirstActivityMonthly'})
                    merged_new_order_column['POS_ID'] = merged_new_order_column['POS_ID'].astype(int) # 初期の型がfloatのため

                    ## NewPOSMasterの生成
                    single_header_df = merged_new_order_column.iloc[:, :double_header_start_column+1].copy()
                    # print(single_header_df)
                    single_header_df = single_header_df.rename(columns={0: 'POS_ID'})

                    ## POSMasterの生成、差分結合
                    pos_master = pd.concat([df_old, single_header_df[~single_header_df['POS_ID'].isin(df_old['POS_ID'])]])
                    logging.info(f"POSマスタ出力:{pos_master}")
                    # POSマスタファイルを出力
                    output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_master_out)
                    output_blob_client.upload_blob(pos_master.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)



                    ## POSSalesの生成
                    # 2行ヘッダーに戻す
                    merged_new_order_column.columns = pd.MultiIndex.from_tuples([tuple(c.split('-')) for c in merged_new_order_column.columns])
                    # 台数・売上・単価のカラムを取得
                    double_header_df = merged_new_order_column.iloc[:, double_header_start_column+1:].copy()
                    first_row = double_header_df.columns.get_level_values(0)  # 1行目を保存（ファイル名に使用）
                    # NaNではないヘッダーの値を取得
                    non_nan_headers = first_row.dropna().unique()

                    ## 縦持ち変換
                    # 縦持ち変換のための空のデータフレームを準備
                    transformed_dfs = [] 
                    # ヘッダー1行目を使用する
                    left_header = merged_new_order_column.columns.get_level_values(0)
                    merged_new_order_column.columns = left_header

                    # ヘッダー1行目のユニークな値ごとにデータを分割
                    for header in non_nan_headers:
                        header_mask = first_row == header
                        if header_mask.sum() > 0:  # カラムに実際にデータが存在する場合のみ処理
                            filtered_df = double_header_df.loc[:, header_mask]
                            filtered_df.reset_index(drop=True, inplace=True)

                            right_header = filtered_df.columns.get_level_values(1)
                            filtered_df.columns = right_header

                            # 元データのヘッダー2行目を消す                            
                            id = merged_new_order_column.iloc[:, [0]].reset_index(drop=True)
                            combined_df = pd.concat([id, filtered_df], axis=1)
                            combined_df = combined_df.rename(columns={0: 'POS_ID'})
                            # print(combined_df)

                            # 結合したデータに対して縦持ち変換を実行
                            melted_df = combined_df.melt(id_vars=combined_df.columns[:1].tolist(),
                                                        var_name="month",
                                                        value_name=header)
                            transformed_dfs.append(melted_df)

                    # 縦持ち変換したデータの1つを選択
                    selected_df = transformed_dfs[0]  # 最初のデータを選択
                    print(selected_df)

                    # それ以外のデータの3列目を抽出し、選択したデータと結合
                    for df1 in transformed_dfs[1:]:
                        forth_column = df1[df1.columns[2]]
                        selected_df = pd.concat([selected_df, forth_column], axis=1)

                    # ②過去データ（POS売上のMasterデータ）の読み込み
                    blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_sales_out)
                    blob_data = blob_client_in.download_blob()
                    review_data = blob_data.readall()
                    ## DataFrame化
                    df2 = pd.read_csv(io.BytesIO(review_data)) 

                    # ①に[sales_ID]カラムを左から追加する
                    selected_df.insert(0, 'sales_ID', pd.NA)
                    # print(selected_df)

                    # キーでマージ（右結合）
                    unique_df1 = pd.merge(df2, selected_df, on=['POS_ID', 'month'], how='right')
                    # 既存のPOS_IDの最大値を取得
                    max_sales_id = df2['sales_ID'].max()
                    # ヒットしなかったデータ（newにしかないデータ）に新しいIDを振る
                    new_sales_ids = pd.RangeIndex(start=max_sales_id + 1, stop=max_sales_id + 1 + len(unique_df1))
                    print(new_sales_ids)
                    unique_df1.loc[unique_df1['sales_ID_x'].isna(), 'sales_ID_x'] = new_sales_ids[:len(unique_df1[unique_df1['sales_ID_x'].isna()])]

                    # データの成型
                    # 特定のカラムを除外
                    column_to_exclude = ['Sales Units_x','Sales Value JPY wo. VAT_x','Price JPY wo. VAT_x','sales_ID_y']
                    unique_without_column = unique_df1.drop(column_to_exclude, axis=1)
                    # カラムリネーム
                    unique_without_column = unique_without_column.rename(columns={'sales_ID_x': 'sales_ID','Sales Units_y': 'Sales Units','Sales Value JPY wo. VAT_y': 'Sales Value JPY wo. VAT','Price JPY wo. VAT_y': 'Price JPY wo. VAT'})
                    unique_without_column['sales_ID'] = unique_without_column['sales_ID'].astype(int) # 初期の型がfloatのため
                    print(unique_without_column)

                    ## POSSalesの生成、saled_IDを軸に差分結合
                    pos_sales = pd.concat([df2, unique_without_column[~unique_without_column['sales_ID'].isin(df2['sales_ID'])]])

                    logging.info(f"POS売上出力:{pos_sales}")

                    # POS売上ファイルを出力
                    output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name_sales_out)
                    output_blob_client.upload_blob(pos_sales.to_csv(index=False, encoding='utf_8'), blob_type="BlockBlob", overwrite=True)

                # POSデータ取り込みの次回再実施のため削除
                blob_client.delete_blob()

    return func.HttpResponse(
                status_code=200
    )





    # 1.POSデータの読み込み
    ## BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    ## Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    ## BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_in = "dashboard_KPI/raw/"
    blob_name_master_out = "dashboard_KPI/modify/data/KPI_modify_POS_master_file.csv"
    blob_name_sales_out = "dashboard_KPI/modify/data/KPI_modify_POS_sales_file.csv"
    # 読み込むシートの名前
    sheet_name = 'Hitlist_Item_24 month'

    ## POSデータ取得
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=blob_name_in)

    # 各Blobに対する処理
    for blob in blob_list:
        if "/" not in blob.name[len(blob_name_in):]:
            if blob.name.endswith('.xlsx'):
                blob_client = blob_service_client.get_blob_client(container_name, blob.name)
                with io.BytesIO() as input_blob:
                    blob_data = blob_client.download_blob()
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

                # POSデータ取り込みの次回再実施のため削除
                blob_client.delete_blob()

    return func.HttpResponse(
                status_code=200
    )