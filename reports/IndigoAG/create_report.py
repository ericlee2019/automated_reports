"""
Extracts and transforms data to match requested csv output file 
format for an automated weekly report for Indigo_AG.
Purpose: Indigo_AG wants participation and score data for their org segments.

This module has been slightly modified from its original version
created by Symphony team. The original existed in a .ipynb file.

Original code did not contain comments. Please read through carefully.


Author: Eric Lee
"""
import json
import numpy as np
import os
import pandas as pd
import time

# this import contains the sql queries as class variables
from sql import Queries
from DA_package import mysql

start = time.time()
# load response rates (RR) and wins count for each active org
# with open("config.json", "r") as f:
#     config = json.load(f)

org_id = 32995  # Indigo AG
Q = Queries()

# TINYPULSE_REPLICA priveleges no longer required for this query to run
def mod_query(query, var_val, var="@organization_id"):
    """
    Modifies the query by changing the varia    ble (var) for required data.

    Params:
    -------
    query (str): MySQL query
    var_val (int/str): variable value(s)
    db_config (str): database login in configuration
    var (str): "@--" variable in sql query
    """
    sql_query = query.replace(var, str(var_val))
    return mysql.sql_query_to_pd(sql_query)

def convert_question_type(type):
    if type == "Question::ScaleQuestion":
        return "Scale"
    if type == "Question::BooleanQuestion":
        return "Yes/No"
    if type == "Question::TextQuestion":
        return "Text"
    return None

def main():
    # create dataframes from queries
    survey_df = mod_query(Q.survey, org_id)
    response_df = mod_query(Q.response, org_id)
    segment_df = mod_query(Q.segment, org_id)
    filter_df = mod_query(Q.filter, org_id)

    # used to pull filter labels from 'filterships' table. see sql query if confused.
    str_filter_ids = ",".join(str(x) for x in filter_df["filter_id"].values)
    filter_apply_df = mod_query(Q.filter_apply, var_val=str_filter_ids, var="@filter_ids")

    # combine into one dataframe
    response_filter_df = filter_apply_df.merge(filter_df, on=["filter_id"], how="left")
    response_segment_df = response_df.merge(segment_df, on=["segment_id"], how="left")
    response_filter_segment_df = response_segment_df.merge(
        response_filter_df, on=["response_id"]
    )
    response_all_df = response_filter_segment_df.merge(
        survey_df, on=["survey_id"], how="left"
    )

    # group by and aggregations for output dataframe
    final_df = (
        response_all_df.groupby(
            [
                "survey_id",
                "prompt",
                "survey_day",
                "type",
                "filter_id",
                "filter_label",
                "segment_id",
                "segment_label",
            ]
        )
        .agg(
            {
                "response_id": pd.Series.nunique,
                "question_response_integer": np.sum,
                "submitted": np.sum,
            }
        )
        .reset_index()
    )

    org_survey_df = (
        response_df.groupby(["survey_id"])
        .agg(
            {
                "response_id": pd.Series.nunique,
                "question_response_integer": np.sum,
                "submitted": np.sum,
            }
        )
        .reset_index()
    )

    org_survey_df["org_participation_rate"] = (
        org_survey_df["submitted"] / org_survey_df["response_id"]
    )

    deliver_df = final_df.merge(
        org_survey_df[["survey_id", "org_participation_rate"]], on=["survey_id"]
    )

    deliver_df["avg_score"] = (
        deliver_df["question_response_integer"] / deliver_df["submitted"]
    )
    deliver_df["participation_rate"] = deliver_df["submitted"] / deliver_df["response_id"]

    deliver_df["avg_score"] = deliver_df[["type", "avg_score"]].apply(
        lambda x: x["avg_score"] if x["type"] == "Question::ScaleQuestion" else None, axis=1
    )
    deliver_df["type"] = deliver_df["type"].apply(convert_question_type)


    # create average score column by (filter & segment)
    deliver_df2 = deliver_df.copy()
    segments = deliver_df2.segment_label.unique()
    deliver_df2 = deliver_df2.loc[deliver_df2["type"] == "Scale"].copy()
    deliver_df["avg_score_segment&filter"] = np.nan
    for segment in segments:
        s_filters = (
            deliver_df2["filter_label"]
            .loc[deliver_df2["segment_label"] == segment]
            .unique()
        )
        for s_filter in s_filters:
            index_tmp = deliver_df2.loc[
                (deliver_df2["segment_label"] == segment)
                & (deliver_df2["filter_label"] == s_filter)
            ].index
            deliver_df.loc[index_tmp, "avg_score_segment&filter"] = deliver_df2.loc[
                index_tmp, "avg_score"
            ].mean()

    columns = [
        "prompt",
        "survey_day",
        "filter_label",
        "segment_label",
        "avg_score_segment&filter",
        "participation_rate",
        "org_participation_rate",
    ]

    header = [
        "Question",
        "Date Asked",
        "Filter",
        "Segment",
        "Avg Score by Segment&Filter",
        "Participation rate",
        "Org Avg Participation Rate",
    ]

    file_path = "~/airflow/files/Indigo_report.csv"
    deliver_df.to_csv(
        file_path, index=None, header=header, columns=columns, encoding="utf-8"
    )

    print(time.time() - start)

if __name__ == "__main__":
    main()