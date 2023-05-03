from Read_Write import read_one_df
from Shemes_Data_Frames import *
from Patch_Csv import *

name_basics_df = read_one_df(shema_name_basics, name_basics)
title_akas_df = read_one_df(shema_akas, title_akas)
title_basics_df = read_one_df(shema_basics, title_basics)
title_crew_df = read_one_df(shema_crew, title_crew)
title_episode_df = read_one_df(shema_episode, title_episode)
title_principals_df = read_one_df(shema_principals, title_principals)
title_ratings_df = read_one_df(shema_ratings, title_ratings)
