import logging
from .review_merge import review_merge
from .review_diff import review_diff

import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1.レビューファイルのマージ
    merge_data = review_merge()
    logging.info("merge:")
    logging.info(merge_data.head())
    print(merge_data.head())

    # 2.ID採番・差分抽出
    diff_data = review_diff(merge_data)
    logging.info("diff:")
    logging.info(diff_data)
    print(diff_data.head())


    return func.HttpResponse(
                status_code=200
    )