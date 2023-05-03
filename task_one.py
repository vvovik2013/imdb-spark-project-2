from User_Data_Frames import *
import pyspark.sql.functions as f


def task_one():
    task_one_df = title_akas_df.select(f.col("title"), f.col("region"), f.col("language"))
    task_one_ua_language_df = task_one_df.filter(f.col("region") == "UA")
    task_one_ua_language_result_df = task_one_ua_language_df.select(f.col("title")).dropDuplicates(["title"])
    return task_one_ua_language_result_df
