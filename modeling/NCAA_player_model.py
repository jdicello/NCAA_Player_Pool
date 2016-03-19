import csv
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

stats.to_csv("../output/2016_pts.csv")
pts.to_csv("../output/2016_pts.csv")
pts_last_10.to_csv("../output/2016_pts_last_10.csv")
pts_home.to_csv("../output/2016_pts_home.csv")
pts_away.to_csv("../output/2016_pts_away.csv")

player.team_schedule_df.to_csv("../output/pre_tournament_schedule.csv")
# Read in player schedule
pre_header = 'team_id,game_num,game_id,game_location,opp_id'
pre_team_sch = pd.read_csv("../output/pre_tournament_schedule.csv",
                                                header=0,
                                                usecols=pre_header.split(',')
                                                )
last_game = player.team_schedule_df.merge(player.bracket, on='team_id')[['team_id','team_name','game_num']].groupby(['team_id','team_name']).agg({"game_num": [np.max]})
last_game.reset_index(inplace=True)
player.team_schedule_df.groupby(['team_id','team_name']).agg({"game_num": [np.max]})

##### only tourney games
t_game_stats = stats.merge(last_game, how='inner', on=['team_id','game_num'])
t_game_stats = t_game_stats[t_game_stats.opp_id_y.isnull()]

t_game_stats[['team_name','player','PTS']].to_csv("../output/tourney_stats.csv")

# look at sch diff
l_sch = player.team_schedule_df.merge(pre_team_sch, how='left', on=['team_id','game_id'])
tourney_games = l_sch[l_sch.opp_id_y.isnull()]
t_games = tourney_games.merge(player.bracket[['team_id','team_name']], on='team_id')
player.player_stats_df.merge(t_games, on=['team_id','game_id'])

player.player_stats_df[player.player_stats_df.game_id==400871255]
player.team_schedule_df[player.team_schedule_df.game_id==400871255]

player.player_stats_df.info()
sch.info()
stats.info()
last_game.info()
stats.team_name.head()

last_game.columns

last_game[[2]]

stats.head()