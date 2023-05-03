from User_Data_Frames import *
import pyspark.sql.functions as f


def task_eight():
    tmp_eight = (title_basics_df.withColumn('genres_one', f.regexp_extract(f.col('genres'), '(.+)\,(.+)', 1))
                                .withColumn('genres_two', f.regexp_extract(f.col('genres'), '(.+)\,(.+)', 2)))
    tmp_pt = (tmp_eight.withColumn('genres_three', f.regexp_extract(f.col('genres_one'), '(.+)\,(.+)', 1))
                       .withColumn('genres_four', f.regexp_extract(f.col('genres_one'), '(.+)\,(.+)', 2)))
    tmp_select_tmp_pt = (tmp_pt.select(f.col("tconst"),
                                       f.col("originalTitle").alias("Title"),
                                       f.col("genres_two").alias("cat_1"),
                                       f.col("genres_three").alias("cat_2"),
                                       f.col("genres_four").alias("cat_3")))
    tmp_pt_rating = (title_ratings_df.select(f.col("tconst"),
                                             f.col("averageRating").alias("Rating")))
    task_eight_part_one = (tmp_select_tmp_pt.join(tmp_pt_rating,
                                                  tmp_pt_rating.tconst == tmp_select_tmp_pt.tconst,
                                                  how="inner"))
    tmp_two = tmp_pt.select(f.col("genres_two")).dropDuplicates(["genres_two"]).orderBy(f.asc("genres_two"))
    tmp_three = tmp_pt.select(f.col("genres_three")).dropDuplicates(["genres_three"]).orderBy(f.asc("genres_three"))
    tmp_four = tmp_pt.select(f.col("genres_four")).dropDuplicates(["genres_four"]).orderBy(f.asc("genres_four"))
    tmp_res_one = tmp_two.join(tmp_three, tmp_two.genres_two == tmp_three.genres_three, how="full_outer")
    tm_res_two = tmp_res_one.join(tmp_four, tmp_res_one.genres_two == tmp_four.genres_four, how="outer")
    tmp_res_on = (tmp_res_one.select(f.when(tmp_res_one.genres_two != "null", tmp_res_one.genres_two)
                                     .otherwise(tmp_res_one.genres_three)).alias("keys_tmp"))
    tmp_res_tw = (tmp_res_on
                  .select(f.col("CASE WHEN (NOT (genres_two = null)) THEN genres_two ELSE genres_three END")
                          .alias("key_as")))
    tmp_not_null = tmp_res_tw.where((f.col("key_as") != "null") | (f.col("key_as") != ""))
    resukt_key = tmp_not_null.orderBy(f.desc("key_as")).limit(28)
    task_eight_part_two = (task_eight_part_one.join(resukt_key,
                                                    resukt_key.key_as == task_eight_part_one.cat_1,
                                                    how="inner"))
    task_eight_part_two_select = task_eight_part_two.select(f.col("Title"),
                                                            f.col("cat_1").alias("categori"),
                                                            f.col("Rating"))
    task_eight_part_three = (task_eight_part_one.join(resukt_key,
                                                      resukt_key.key_as == task_eight_part_one.cat_2,
                                                      how="inner"))
    task_eight_part_three_select = task_eight_part_three.select(f.col("Title"),
                                                                f.col("cat_2").alias("categori"),
                                                                f.col("Rating"))
    task_eight_part_four = (task_eight_part_one.join(resukt_key,
                                                     resukt_key.key_as == task_eight_part_one.cat_3,
                                                     how="inner"))
    task_eight_part_four_select = task_eight_part_four.select(f.col("Title"),
                                                              f.col("cat_3").alias("categori"),
                                                              f.col("Rating"))
    task_eight_part_four_union = (task_eight_part_two_select.union(task_eight_part_three_select)
                                                            .union(task_eight_part_four_select))
    task_eight_part_five_union_sort = (task_eight_part_four_union.orderBy(f.asc("categori"),
                                                                          f.desc("Rating"),
                                                                          f.asc("Title")))
    task_tmp_eight = resukt_key.collect()
    null_mas_eight = task_eight_part_five_union_sort.filter(f.col("categori") == "-").limit(10)
    for m in range(0, 18):
        marker_key = str(task_tmp_eight[m])
        marker_key_l = marker_key[12:-2]
        mas_df = task_eight_part_five_union_sort.filter(f.col("categori") == str(marker_key_l)).limit(10)
        null_mas_eight = mas_df.union(null_mas_eight)
    task_result_eight_fin = (null_mas_eight.orderBy(f.asc("categori"),
                                                    f.desc("Rating"),
                                                    f.asc("Title")))
    return task_result_eight_fin
