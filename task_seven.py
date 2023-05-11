from User_Data_Frames import *
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.functions import row_number


def task_seven():
    """
    Function to output 10 titles of the most popular movies/series etc. by each decade
    :return: Dataframe
    """
    new_dic_tmp = (title_basics_df.select(f.col("tconst").alias("index"), f.col("originalTitle"),
                                          f.col("startYear").substr(startPos=0, length=3).alias("YYY")))
    task_seven_part_one = title_ratings_df.select(f.col("tconst"), f.col("averageRating"))
    task_seven_part_two = (new_dic_tmp.join(task_seven_part_one, new_dic_tmp.index ==
                                            task_seven_part_one.tconst, how="inner"))
    task_seven_part_three = task_seven_part_two.select(f.col("index"),
                                                       f.col("originalTitle"),
                                                       f.col("averageRating"),
                                                       f.col("YYY"))
    tmp_seven_task = Window.partitionBy("YYY").orderBy(f.desc("averageRating"))
    task_seven_part_six = task_seven_part_three.withColumn("row_number", row_number().over(tmp_seven_task))
    task_seven_part_seven = task_seven_part_six.filter(f.col("row_number") <= 10)
    task_seven_part_eight = task_seven_part_seven.withColumn("Decades", f.col("YYY")*10)
    task_seven_part_nine = task_seven_part_eight.filter(f.col("Decades") > 0)
    task_seven_part_ten = (task_seven_part_nine.select(f.col("originalTitle").alias("Title"),
                                                       f.col("averageRating").alias("Rating"),
                                                       f.col("Decades").substr(startPos=0, length=4)
                                                                       .alias("Decades")))
    task_result_seven_fin = task_seven_part_ten
    return task_result_seven_fin
