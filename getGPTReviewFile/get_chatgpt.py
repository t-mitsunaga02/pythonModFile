import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient

import pandas as pd
import openai
import time
import ast
import os
import io

def gpt_connect(comment,question, Prerequisites):
    #Note: The openai-python library support for Azure OpenAI is in preview.
    #Note: This code sample requires OpenAI Python library version 0.28.1 or lower.
    openai.api_type = "azure"
    openai.api_version = "2023-07-01-preview"
    openai.api_key = os.getenv("AZURE_OPENAI_KEY")
    openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")
    message_text = [ 
        {
        "role":"user","content":str(Prerequisites)
        },
        {
        "role":"user","content":str(question)+str(comment)
        }
    ]

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

    str_dict = completion['choices'][0]["message"]["content"]
    str_dict = remove_before_bracket(input_str=str_dict)
    str_dict = remove_after_bracket(input_str=str_dict)

    #辞書型に変換
    converted_dict = ast.literal_eval(str_dict)
    result_dict={}
    for key, value in converted_dict.items():
        if not isinstance(value, list):
            result_dict[key] = [value]
        else:
            result_dict[key] = value
    print(result_dict)

    return result_dict


def add_quotes(match):
    return f'"{match.group(1)}":'

def  remove_after_bracket(input_str):
    index = input_str.find('}')
    if index != -1:
        return input_str[:index+1]
    else:
        return input_str

def  remove_before_bracket(input_str):
    index = input_str.find('{')
    if index != -1:
        return input_str[index:]
    else:
        return input_str

def pickup_elem_from_dictionary(dict):
    reason_list = dict["reason"]
    #   favorite_list = dict["favorite"]
    #   dissatisfaction_list = dict["dissatisfaction"]
    return reason_list

def omit_char(values,omits):
    '''
    リストで指定した文字、又は文字列を削除する

    Params
    ---------------------
    values:str
        対象文字列
    omits:str
        削除したい文字、又は文字列

    Returns
    ---------------------
    return :str
        不要な文字を削除した文字列
    '''
    for n in range(len(values)):
        for omit in omits:
            values[n] = values[n].replace(omit,'')
    return values

def add_df(values,columns,omits = None):
    '''
    指定した値を　DataFrame に行として追加する
    omits に削除したい文字列をリストで指定可能

    Params
    ---------------------
    values:[str]
        列名
    omits:[str]
        削除したい文字、又は文字列
    '''
    if omits is not None:
        values = omit_char(values,omits)
        columns = omit_char(columns,omits)

    df = pd.DataFrame(values,index=rename_column(columns))
    df = pd.concat([df,df.T])
    #return self.df

def rename_column(columns):
    '''
    重複するカラム名の末尾に連番を付与し、ユニークなカラム名にする
        例 ['A','B','B',B'] → ['A','B','B_1','B_2']

    Params
    ---------------------
    columns: [str]
        カラム名のリスト

    Returns
    ---------------------
    return :str
        重複するカラム名の末尾に連番が付与されたリスト
    '''
    lst = list(set(columns))
    for column in columns:
        dupl = columns.count(column)
        if dupl > 1:
            cnt = 0
            for n in range(0,len(columns)):
                if columns[n] == column:
                    if cnt > 0:
                        columns[n] = f'{column}_{cnt}'
                    cnt += 1
    return columns


### GPT判定処理スタート
def scrape_gpt_get():
    # 1.差分データの読み込み
    # BLOBへの接続
    connect_str = os.getenv("AzureWebJobsStorage")
    # Create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    # BLOB入出力先の設定
    container_name = "scrapefile"
    blob_name_diff_in = "dashboard_motive/modify/diff/motive_modify_difffile.csv"

    blob_client_in = blob_service_client.get_blob_client(container=container_name, blob=blob_name_diff_in)
    blob_data = blob_client_in.download_blob()
    diff_data = blob_data.readall()
    ## DataFrame化
    df = pd.read_csv(io.BytesIO(diff_data))

    # 2.ChatGPT判定処理
    Prerequisites = "空気清浄機の口コミ文章から購入動機を調べます。\n口コミ文章に記載されている購入後に感じた感想は購入動機とは絶対に認めません。例えば「花粉対策のために購入しました。運転音も意外に静かで購入して良かったです。」であれば、[静音性]は購入動機とは認めません。\n購入動機が記載されていない口コミ文章も存在します。"
    question = "### 命令\n口コミ文章から、制約条件に示すカテゴリの中で該当する空気清浄機の購入動機があれば全て選択して下さい。該当する購入動機がない場合は「その他」を、口コミに購入動機が記載されていない場合は「記載なし」を選択してください。出力はjson形式しか認めません。\n### 制約条件：\n購入動機は次のカテゴリの中から選択する。\nカテゴリ：[PM2.5対策、黄砂対策、排ガス対策、ホコリ・ハウスダスト、タバコの煙対策、化学物質対策、花粉対策、部屋の乾燥対策、肌や喉の乾燥対策、ダニアレルギー対策、カビアレルギー対策、コロナウイルス対策、インフルエンザ対策、カビ発生対策、乳児・幼児・子供向け、高齢者向け、リビング用、寝室用、オフィス用、病院・クリック用、コンパクト、省スペース、軽い、持ち運びがしやすい、デザイン、本体色が白、本体色が白以外の色、静音性、加湿のフィルタ・タンクの手入れのしやすさ、フィルタ掃除のしやすさ、フィルタ交換頻度の低さ、ペット向け、タバコのにおい、体臭、コスパがいい、セール、安い、テレビ・CMで見た、量販店で見た、病院で見た、知人の紹介、口コミを見て、SNS、webサイト、ブランド・メーカーへの信頼、ブランド独自の機能(プラズマクラスター・ストリーマ・ナノイー)、加湿機能が搭載、加湿機能が非搭載、適用畳数の大きさ、風量の大きさ、フィルタ性能の高さ、リモコン操作可能、スマホ操作・連携可能、おまかせ運転搭載、しゃべる機能搭載、空気環境の見える化(本体)、空気環境の見える化(アプリ)、壊れたから、能力不足を感じて、新機種が出たから、古くなったから、贈答用、その他、記載なし]\n### 出力文：\n{""reason"":[選択した購入動機A,選択した購入動機B・・・,選択した購入動機Z]}のjson形式\n### 口コミ文章：\n"

    reasons_list =[]

    ## コメントごとにGPT判定するループ
    for index, row in df.iterrows():
        print(row['comment'])
        logging.info(f"コメント:{row['comment']}")
        try:
            result_dic = gpt_connect(comment=row['comment'],question=question,Prerequisites=Prerequisites)
            logging.info(f"結果:{result_dic}")
        except ValueError:
            print("ValueErrorが発生しました。")
        #    reasons_list.append(reason_list)
        #    print(reasons_list)
            time.sleep(15)
        except Exception as e:
            print(f"予期せぬErrorが発生しました:{e}")
        #    reasons_list.append(reason_list)
        #    print(reasons_list)
            time.sleep(15)

        reason_list = pickup_elem_from_dictionary(dict=result_dic)
        print(reason_list)
        reasons_list.append(reason_list)
        time.sleep(25)

    new_rows =[]
    #元のデータフレームとリストから行を生成し、新しいデータフレームに追加
    for index, row in df.iterrows():
        for reason in reasons_list[index]:
            new_row = row.copy()
            new_row['reason'] = reason
            new_rows.append(new_row)

    reason_df = pd.DataFrame(new_rows, columns=['review_id', 'pos_id', 'item', 'site_name', 'review_date', 'star', 'title', 'comment', 'reason'])
    return reason_df
