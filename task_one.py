from User_Data_Frames import *
import pyspark.sql.functions as f


def task_one():
    """
    Function for the first task.
    :return: dataframe  with a column "title" containing Ukrainian-language content
    """
    task_one_df = title_akas_df.select(f.col("title"), f.col("region"), f.col("language"))
    """Select the columns title, region, language with title.akas.tsv.gz"""
    task_one_ua_language_df = task_one_df.filter(f.col("region") == "UA")
    """ We leave only the value "UA" in the "region" column, which corresponds to Ukrainian-language content """
    task_one_ua_language_result_df = task_one_ua_language_df.select(f.col("title")).dropDuplicates(["title"])
    """This is the result,we leave only the names of the content (title) and remove duplicates"""
    return task_one_ua_language_result_df
