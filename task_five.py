from User_Data_Frames import *
import pyspark.sql.functions as f


def task_five():
    task_five_one_part_df = title_basics_df.select(f.col("tconst"), f.col("originalTitle"), f.col("isAdult"))
    task_five_two_part_df = task_five_one_part_df.filter(f.col("isAdult") == 1)
    task_five_tree_part_df = title_ratings_df.select(f.col("tconst"), f.col("averageRating"), f.col("numVotes"))
    task_five_four_part_df = title_akas_df.select(f.col("titleId"), f.col("title"), f.col("region"))
    task_five_five_part_df = (task_five_two_part_df.join(task_five_four_part_df, task_five_two_part_df.tconst ==
                                                         task_five_four_part_df.titleId, how="inner"))
    task_five_six_part_df = task_five_five_part_df.select(f.col("tconst"), f.col("region"))
    task_five_seven_part_df = task_five_six_part_df.groupBy("region").count()
    task_five_result_df = task_five_seven_part_df.orderBy(f.desc("count"), "region")
    task_five_result_two_df = task_five_result_df.select(f.col("count"),f.col("region")).limit(100)
    return task_five_result_two_df
