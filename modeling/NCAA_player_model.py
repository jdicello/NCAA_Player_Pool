import csv
import pandas as pd
import numpy as np
#import class_PlayerStats
import PlayerStats

test_df = loadPlayerStats()

bracket = populateBracketology()
team_schedule_df = populateAllTeamSchedules(bracket)
filename = '2017_player_stats.csv'
player_stats_df = parseAllBoxScores(team_schedule_df, filename)

player_stats_df.to_csv("../input/2017_player_stats.csv",encoding='utf_8_sig')

#player = class_PlayerStats.PlayerStats()
#populateBracketology()
#populateAllTeamSchedules()
#parseAllBoxScores()
#
#populateData()

sch = team_schedule_df.merge(bracket, on='team_id')
sch.reset_index(inplace=True)
sch.team_id = sch.team_id.astype(int)
sch.game_id = sch.game_id.astype(int)
sch.groupby('team_name').size()

#######
# Find the avg Pts by player.
#######
player_stats_df.team_id = player_stats_df.team_id.astype(int)
player_stats_df.game_id = player_stats_df.game_id.astype(int)
stats = player_stats_df.merge(sch, on=['team_id','game_id'])
stats = stats[stats.player != 'TEAM']
stats.reset_index(inplace=True)
stats.MIN = stats.MIN.astype(int)
stats.PTS = stats.PTS.astype(int)
stats = stats.apply(lambda x: pd.to_numeric(x, errors='ignore'))
pts = stats.groupby(['team_id','team_name_x','player']).agg({'PTS': np.mean})
pts = stats.groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})
pts = stats.groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})


last_10 = stats.game_num > 20
home = stats.game_location == 'home'
away = stats.game_location == 'away'
pts_last_10 = stats[last_10].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})
pts_home = stats[home].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})
pts_away = stats[away].groupby(['team_id','team_name_x','player']).agg({'PTS': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FGA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_M': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'TREY_A': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'MIN': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTM': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero],'FTA': [np.mean, np.min, np.max, np.std,np.sum,np.count_nonzero]})

stats.to_csv("../output/2017_stats.csv")
pts.to_csv("../output/2017_pts.csv")
pts_last_10.to_csv("../output/2017_pts_last_10.csv")
pts_home.to_csv("../output/2017_pts_home.csv")
pts_away.to_csv("../output/2017_pts_away.csv")

team_schedule_df.to_csv("../output/pre_tournament_schedule.csv")
# Read in player schedule
pre_header = 'team_id,game_num,game_id,game_location,opp_id'
pre_team_sch = pd.read_csv("../output/pre_tournament_schedule.csv",
                                                header=0,
                                                usecols=pre_header.split(',')
                                                )
last_game = team_schedule_df.merge(bracket, on='team_id')[['team_id','team_name','game_num']].groupby(['team_id','team_name']).agg({"game_num": [np.max]})
last_game.reset_index(inplace=True)
team_schedule_df.groupby(['team_id']).agg({"game_num": [np.max]})

##### only tourney games
t_game_stats = stats.merge(last_game, how='inner', on=['team_id','game_num'])
t_game_stats = t_game_stats[t_game_stats.opp_id_y.isnull()]

t_game_stats[['team_name','player','PTS']].to_csv("../output/tourney_stats.csv")

# look at sch diff
l_sch = team_schedule_df.merge(pre_team_sch, how='left', on=['team_id','game_id'])
tourney_games = l_sch[l_sch.opp_id_y.isnull()]
t_games = tourney_games.merge(bracket[['team_id','team_name']], on='team_id')
player_stats_df.merge(t_games, on=['team_id','game_id'])

player_stats_df[player_stats_df.game_id==400871255]
team_schedule_df[team_schedule_df.game_id==400871255]

player_stats_df.info()
sch.info()
stats.info()
last_game.info()
stats.team_name.head()

last_game.columns

last_game[[2]]

stats.head()