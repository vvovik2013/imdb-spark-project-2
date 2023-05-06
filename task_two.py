from User_Data_Frames import *
import pyspark.sql.functions as f


def task_two():
    """
    Function to outputthe list of people’s names, who were born in the 19th century
    :return: Dataframe
    """
    task_two_df = name_basics_df.select(f.col("primaryName"), f.col("birthYear"))
    task_two_centuries_part_one_df = task_two_df.where(f.col("birthYear") >= 1800)
    task_two_centuries_part_two_df = task_two_centuries_part_one_df.where(f.col("birthYear") < 1900)
    task_two_centuries_result_df = (task_two_centuries_part_two_df.select(f.col("primaryName"))
                                    .dropDuplicates(["primaryName"]))
    return task_two_centuries_result_df
