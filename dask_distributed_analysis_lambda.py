import boto3
import time
import json
import numpy as np
import pandas as pd
import io
import os

import dask.dataframe as dd

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    get_aggregated_data()


dtypes = {
    'reviewerID': np.str,
    'asin': np.str,
    'reviewerName': np.str,
    'helpful': np.object,
    'reviewText': np.str,
    'overall': np.float64,
    'summary': np.str,
    'unixReviewTime': np.float64,
    'reviewTime': np.str
}


def get_aggregated_data():
    start = time.time()

    user_reviews = dd.read_csv("s3://data-bucket/user_reviews_test_small.csv", dtype=dtypes)
    user_reviews = user_reviews[['reviewerID', 'helpful', 'overall', 'reviewTime']]

    users_data_cnt = user_reviews.groupby('reviewerID')['overall'].count(split_out=4, split_every=False).reset_index()
    users_data1 = users_data_cnt.rename(columns={'overall': 'number_products_rated'})

    users_data2 = user_reviews.groupby('reviewerID')['overall'].mean(split_out=4, split_every=False).reset_index()
    users_data2 = users_data2.rename(columns={'overall': 'avg_ratings'})

    year = user_reviews.reviewTime.map_partitions(lambda x: x.str.split(',', expand=True)[1], meta=object)
    user_reviews_year = user_reviews.assign(year=year)
    user_reviews_year.year = user_reviews_year.year.astype(int)
    user_reviews_year = user_reviews_year.groupby('reviewerID')['year'].min(split_out=4,
                                                                            split_every=False).reset_index()
    user_reviews_year = user_reviews_year.rename(columns={'year': 'reviewing_since'})

    ser1 = user_reviews.helpful.map_partitions(lambda x: x.str.replace("[", "").str.split(',', expand=True)[0],
                                               meta=object)
    ser2 = user_reviews.helpful.map_partitions(lambda x: x.str.replace("]", "").str.split(',', expand=True)[1],
                                               meta=object)
    user_reviews_votes = user_reviews.assign(votes=ser1)
    user_reviews_total_votes = user_reviews_votes.assign(total_votes=ser2)

    user_reviews_total_votes.votes = user_reviews_total_votes.votes.astype(int)
    user_reviews_total_votes.total_votes = user_reviews_total_votes.total_votes.astype(int)
    user_reviews_agg1 = user_reviews_total_votes.groupby('reviewerID')['votes'].sum(split_out=4,
                                                                                    split_every=False).reset_index()
    user_reviews_agg2 = user_reviews_total_votes.groupby('reviewerID')['total_votes'].sum(split_out=4,
                                                                                          split_every=False).reset_index()
    user_reviews_agg1 = user_reviews_agg1.rename(columns={'votes': 'helpful_votes'})

    users_df = dd.concat([users_data1, users_data2['avg_ratings'], user_reviews_year['reviewing_since'],
                          user_reviews_agg1['helpful_votes'], user_reviews_agg2['total_votes']], axis=1)

    submit = users_df.describe().compute().round(2)
    end = time.time()
    runtime = end - start

    print(runtime)
    print(submit.to_json())
    return runtime

