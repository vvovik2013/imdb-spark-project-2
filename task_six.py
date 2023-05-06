from User_Data_Frames import *
import pyspark.sql.functions as f


def task_six():
    """
    Function to output information about how many episodes in each TV Series AND
    the top 50 of them starting from the TV Series with the biggest quantity of
    episodes
    :return: Dataframe
    """
    task_six_one_part_dff = (title_basics_df
                             .select(f.col("tconst"), f.col("titleType"), f.col("originalTitle"))
                             .where(f.col("titleType").isin("tvSeries")))
    task_six_one_part_df = task_six_one_part_dff.select(f.col("tconst"), f.col("originalTitle"))
    task_six_two_part_df = title_episode_df.select(f.col("parentTconst"), f.col("episodeNumber"))
    task_six_join = (task_six_two_part_df
                     .join(task_six_one_part_df,
                           task_six_two_part_df.parentTconst == task_six_one_part_df.tconst, how="inner"))
    task_six_three_part_df = task_six_join.select(f.col("originalTitle"), f.col("episodeNumber"))
    tmp_preresult_df = (task_six_three_part_df.groupBy("originalTitle").count())
    task_six_result_df = tmp_preresult_df.orderBy(f.desc("count"), "originalTitle").limit(50)
    return task_six_result_df

