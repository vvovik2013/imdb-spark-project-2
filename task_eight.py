from User_Data_Frames import *
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql.functions import row_number


def task_eight():
    """
    Function to output 10 titles of the most popular movies/series etc. by each genre
    :return: Dataframe
    """
    select_genres_one = (title_basics_df.withColumn('genres_one', f.regexp_extract(f.col('genres'), '(.+)\,(.+)', 1))
                                        .withColumn('genres_two', f.regexp_extract(f.col('genres'), '(.+)\,(.+)', 2)))
    select_genres_two = (select_genres_one
                         .withColumn('genres_three', f.regexp_extract(f.col('genres_one'), '(.+)\,(.+)', 1))
                         .withColumn('genres_four', f.regexp_extract(f.col('genres_one'), '(.+)\,(.+)', 2)))
    create_table_with_cat = (select_genres_two.select(f.col("tconst"),
                                                      f.col("originalTitle").alias("Title"),
                                                      f.col("genres_two").alias("cat_1"),
                                                      f.col("genres_three").alias("cat_2"),
                                                      f.col("genres_four").alias("cat_3")))
    select_rating_table = (title_ratings_df.select(f.col("tconst"),
                                                   f.col("averageRating").alias("Rating")))
    create_big_table = (create_table_with_cat.join(select_rating_table,
                                                   select_rating_table.tconst == create_table_with_cat.tconst,
                                                   how="inner"))
    drop_duplicat_cat_two = (select_genres_two.select(f.col("genres_two"))
                                              .dropDuplicates(["genres_two"])
                                              .orderBy(f.asc("genres_two")))
    drop_duplicat_cat_three = (select_genres_two.select(f.col("genres_three"))
                                                .dropDuplicates(["genres_three"])
                                                .orderBy(f.asc("genres_three")))
    join_cat_two_and_cat_three = (drop_duplicat_cat_two.join(drop_duplicat_cat_three,
                                                             drop_duplicat_cat_two.genres_two
                                                             == drop_duplicat_cat_three.genres_three, how="full_outer"))
    drop_null_vol = (join_cat_two_and_cat_three.select(f.when(join_cat_two_and_cat_three.genres_two != "null",
                                                              join_cat_two_and_cat_three.genres_two)
                                                        .otherwise(join_cat_two_and_cat_three.genres_three)))
    alias_name_for_when_null = (drop_null_vol
                                .select(f.col("CASE WHEN (NOT (genres_two = null)) "
                                              "THEN genres_two ELSE genres_three END")
                                         .alias("key_as")))
    only_not_null = alias_name_for_when_null.where((f.col("key_as") != "null") | (f.col("key_as") != ""))
    resukt_key_col = only_not_null.orderBy(f.desc("key_as")).limit(28)
    task_eight_part_one = (create_big_table.join(resukt_key_col,
                                                 resukt_key_col.key_as == create_big_table.cat_1,
                                                 how="inner"))
    task_eight_part_one_select = task_eight_part_one.select(f.col("Title"),
                                                            f.col("cat_1").alias("categori"),
                                                            f.col("Rating"))
    task_eight_part_two = (create_big_table.join(resukt_key_col,
                                                 resukt_key_col.key_as == create_big_table.cat_2,
                                                 how="inner"))
    task_eight_part_two_select = task_eight_part_two.select(f.col("Title"),
                                                            f.col("cat_2").alias("categori"),
                                                            f.col("Rating"))
    task_eight_part_three = (create_big_table.join(resukt_key_col,
                                                   resukt_key_col.key_as == create_big_table.cat_3,
                                                   how="inner"))
    task_eight_part_three_select = (task_eight_part_three.select(f.col("Title"),
                                                                 f.col("cat_3").alias("categori"),
                                                                 f.col("Rating")))
    task_eight_part_all_cat_union = (task_eight_part_one_select.union(task_eight_part_two_select)
                                                               .union(task_eight_part_three_select))
    task_eight_part_all_cat_union_sort = (task_eight_part_all_cat_union.orderBy(f.asc("categori"),
                                                                                f.desc("Rating"),
                                                                                f.asc("Title")))
    eight_window = Window.partitionBy("categori").orderBy(f.desc("Rating"))
    task_eight_part_six = task_eight_part_all_cat_union_sort.withColumn("row_number", row_number().over(eight_window))
    task_eight_part_seven = task_eight_part_six.filter(f.col("row_number") <= 10)
    task_result_eight_fin = (task_eight_part_seven.select(f.col("categori"),
                                                          f.col("Rating"),
                                                          f.col("Title"))
                                                  .orderBy(f.asc("categori"),
                                                           f.desc("Rating"),
                                                           f.asc("Title")))
    return task_result_eight_fin
