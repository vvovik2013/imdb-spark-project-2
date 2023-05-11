from User_Data_Frames import *
import pyspark.sql.functions as f


def task_four():
    """
    Function to output names of people, corresponding movies/series and characters they
    played in those films

    :return: Dataframe
    """
    task_four_one_part_df = (title_principals_df.select(f.col("tconst"),
                                                        f.col("nconst"),
                                                        f.col("category"),
                                                        f.col("characters")))
    task_four_two_part_df = task_four_one_part_df.filter(f.col("category") == "actor")
    tmp_task_four_name_df = (name_basics_df.select(f.col("nconst"), f.col("primaryName")))
    tmp_task_four_title_basics_df = (title_basics_df.select(f.col("tconst"), f.col("originalTitle")))
    test_df = (task_four_two_part_df.join(tmp_task_four_name_df, task_four_two_part_df.nconst
                                          == tmp_task_four_name_df.nconst, how="inner"))
    test_two_df = (test_df.join(tmp_task_four_title_basics_df, test_df.tconst
                                == tmp_task_four_title_basics_df.tconst, how="inner"))
    task_four_result_df = (test_two_df.select(f.col("primaryName"),
                                              f.col("originalTitle"),
                                              f.col("characters")))
    return task_four_result_df
