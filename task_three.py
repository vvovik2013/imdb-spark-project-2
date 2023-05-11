from User_Data_Frames import *
import pyspark.sql.functions as f


def task_three():
    """
    Function to output titles of all movies that last more than 2 hours
    :return: Dataframe
    """
    task_three_df = title_basics_df.select(f.col("primaryTitle"), f.col("originalTitle"), f.col("runtimeMinutes"))
    task_three_part_one_df = task_three_df.where(f.col("runtimeMinutes") >= 180)
    task_three_part_two_df = (task_three_part_one_df
                              .dropDuplicates(["primaryTitle", "originalTitle", "runtimeMinutes"]))
    task_three_result_df = task_three_part_two_df.select(f.col("primaryTitle"), f.col("originalTitle"))
    return task_three_result_df
