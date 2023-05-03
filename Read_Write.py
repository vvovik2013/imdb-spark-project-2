from Shemes_Data_Frames import spark_session


def read_df(sp_sess, shema_df, patch):
    data_df = (sp_sess.read.options(delimiter='\t').csv(patch,
                                                        shema_df,  header=True, nullValue="null"))
    return data_df


def read_one_df(shem, patch_to_csv):
    one_df = read_df(spark_session, shem, patch_to_csv)
    return one_df


def write_df(df, patch_df_csv):
    df.write.csv(patch_df_csv)
    messege_finish = "Its" + patch_df_csv + "ok!"
    print(messege_finish)
    return