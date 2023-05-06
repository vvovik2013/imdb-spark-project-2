"""
Modul for function read dataset with file  and write dataframe to file
"""
from Shemes_Data_Frames import spark_session


def read_df(sp_sess, shema_df, patch):
    """
    The function of creating a dataframe from a dataset with user separator (/t)
    :param sp_sess: sparksession initialization, variable in the file: Shemes_Data_Frames.py
    :param shema_df: StructType, scheme of the selected dataset, variable in the file: Shemes_Data_Frames.py
    :param patch: StringType, the path to the selected dataset, variable in the file: Patch_Csv.py
    :return: Dataframe
    """
    data_df = (sp_sess
               .read.options(delimiter='\t')
               .csv(patch, shema_df,  header=True, nullValue="null"))
    return data_df


def read_one_df(shem, patch_to_csv):
    """
    The function of creating a dataframe from a dataset,
    The function is based on  "read_df" function and only accepts the path and schema of the dataset
    :param shem: StructType, Scheme of the selected dataset, variable in the file: Shemes_Data_Frames.py
    :param patch_to_csv: StringType, the path to the selected dataset, variable in the file: Patch_Csv.py
    :return: Dataframe
    """
    one_df = read_df(spark_session, shem, patch_to_csv)
    return one_df


def write_df(df, patch_df_csv):
    """
    The function of writing a data frame to a file.
    At the end of the function, a message is printed: Its <path variable name> ok!
    :param df: Dataframe
    :param patch_df_csv: StringType, The path to save the CSV file,  variable in the file: patch_to_save.py
    :return: None
    """
    df.write.csv(patch_df_csv)
    messege_finish = "Its" + patch_df_csv + " ok!"
    print(messege_finish)
    return
