import pandas as pd
import numpy as np
import class_PlayerStats

player = class_PlayerStats.PlayerStats()
player.populateData()

sch = player.team_schedule_df.merge(player.bracket, on='team_id')
sch.groupby('team_name').size()

#######
# Find the avg Pts by player.
#######
stats = player.player_stats_df.merge(sch, on=['team_id','game_id'])
stats.reset_index(inplace=True)
stats = stats.convert_objects(convert_numeric=True)
pts = stats.groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})


last_10 = stats.game_num > 20
home = stats.game_location == 'home'
away = stats.game_location == 'away'
pts_last_10 = stats[last_10].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})
pts_home = stats[home].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})
pts_away = stats[away].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})

stats.to_csv("../input/2016_pts.csv")
pts.to_csv("../input/2016_pts.csv")
pts_last_10.to_csv("../input/2016_pts_last_10.csv")
pts_home.to_csv("../input/2016_pts_home.csv")
pts_away.to_csv("../input/2016_pts_away.csv")

player.player_stats_df.info()

sch.info()
stats.info()
stats.team_name.head()

stats.head()