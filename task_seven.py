from User_Data_Frames import *
import pyspark.sql.functions as f


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
    task_seven_part_six = (task_seven_part_three.filter(f.col("YYY") == "186")
                                                .orderBy(f.desc("averageRating"), f.asc("originalTitle")).limit(10))
    for i in range(187, 203):
        c = str(i)
        new_dic_ppm = (task_seven_part_three.filter(f.col("YYY") == c)
                                            .orderBy(f.desc("averageRating"), f.asc("originalTitle")).limit(10))
        task_seven_part_seven = new_dic_ppm.union(task_seven_part_six)
        task_seven_part_six = task_seven_part_seven
    task_seven_part_seven = (task_seven_part_six.select(f.col("originalTitle"),
                                                        f.col("YYY").alias("Decada"),
                                                        f.col("averageRating")).orderBy(f.asc("Decada")))
    task_seven_result_part_one = task_seven_part_seven.withColumn("Decades", f.col("Decada")*10)
    task_seven_result_part_two = (task_seven_result_part_one.select(f.col("originalTitle").alias("Title"),
                                                                    f.col("averageRating").alias("Rating"),
                                                                    f.col("Decades").substr(startPos=0, length=4)
                                                                                    .alias("Decades")))
    task_result_seven_fin = (task_seven_result_part_two.orderBy(f.asc("Decades"),
                                                                f.desc("averageRating"),
                                                                f.asc("originalTitle")))
    return task_result_seven_fin
