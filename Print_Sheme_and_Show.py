"""
Modul for function read dataset with file  and print preview dataframe or his schemes
"""
from Read_Write import read_df
from Shemes_Data_Frames import *
from Patch_Csv import *


def print_all_sheme_df():
    """
    This function prints all dataframe schemes
    :return: None
    """
    akas_df = read_df(spark_session, shema_akas, title_akas)
    akas_df.printSchema()
    basics_df = read_df(spark_session, shema_basics, title_basics)
    basics_df.printSchema()
    crew_df = read_df(spark_session, shema_crew, title_crew)
    crew_df.printSchema()
    episode_df = read_df(spark_session, shema_episode, title_episode)
    episode_df.printSchema()
    principals_df = read_df(spark_session, shema_principals, title_principals)
    principals_df.printSchema()
    ratings_df = read_df(spark_session, shema_ratings, title_ratings)
    ratings_df.printSchema()
    name_basics_df = read_df(spark_session, shema_name_basics, name_basics)
    name_basics_df.printSchema()


def show_all_df():
    """
    This function prints all dataframe previews
    :return: None
    """
    akas_df = read_df(spark_session, shema_akas, title_akas)
    akas_df.show()
    basics_df = read_df(spark_session, shema_basics, title_basics)
    basics_df.show()
    crew_df = read_df(spark_session, shema_crew, title_crew)
    crew_df.show()
    episode_df = read_df(spark_session, shema_episode, title_episode)
    episode_df.show()
    principals_df = read_df(spark_session, shema_principals, title_principals)
    principals_df.show()
    ratings_df = read_df(spark_session, shema_ratings, title_ratings)
    ratings_df.show()
    name_basics_df = read_df(spark_session, shema_name_basics, name_basics)
    name_basics_df.show()


def show_one_df(shem, patch):
    """
    This function prints one dataframe schemes
    :param shem: StructType, Scheme of the selected dataset with file: Shemes_Data_Frames.py
    :param patch: StringType, The path to the selected dataset with file: Patch_Csv.py
    :return: None
    """
    one_df = read_df(spark_session, shem, patch)
    one_df.show()


def print_shema_one_df(shem, patch):
    """
    This function prints one dataframe schemes
    :param shem: StructType, Scheme of the selected dataset with file: Shemes_Data_Frames.py
    :param patch: StringType, The path to the selected dataset with file: Patch_Csv.py
    :return: None
    """
    one_df = read_df(spark_session, shem, patch)
    one_df.printSchema()
