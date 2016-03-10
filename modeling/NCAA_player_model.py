import pandas as pd
import numpy as np
import class_PlayerStats

player = class_PlayerStats.PlayerStats()
player.populateData()

player.bracket.info()
player.team_schedule_df.info()
player.player_stats_df.info()



tourney_team_games = player.team_schedule_df.merge(player.player_stats_df, on=('team_id','game_id'), how='left')

opp_tourney_teams = pd.merge(left=player.team_schedule_df, right=player.bracket, left_on='opp_id', right_on ='team_id', how='inner')
opp_team_games = opp_tourney_teams.merge(player.player_stats_df, left_on=('opp_id','game_id'), right_on=('team_id','game_id'), how='left')

games_to_parse = pd.merge(left=tourney_team_games[['team_id','game_id']], right=opp_team_games[['opp_id_x','game_id']], on='game_id', how='outer')

#
#df_game_data = pd.read_csv("game_data.txt")
#df_avg_pts = pd.pivot_table(df_game_data,values='PTS', index=['Team','Player'], aggfunc=[np.mean, np.max, np.min])
#
##.sort(ascending=False, inplace=False):
## for t,p,PTS in df_avg_pts:
#	# print t,p,PTS
#
#print df_avg_pts
#
#
#df = pd.read_csv("player_stats.csv")
#
#df.groupby(['team_name','team_id','player']).mean()
#
#df = df.convert_objects(convert_numeric=True)
#
#df[['team_name','player']].size
#
#t = pd.DataFrame( {
#   'A': [1,1,1,1,2,2,2,3,3,4,4,4],
#   'B': [5,5,6,7,5,6,6,7,7,6,7,7],
#   'C': [1,1,1,1,1,1,1,1,1,1,1,1]
#    } );
#
#t
#g = t.groupby(['A', 'B'])
#
#t['D'] = t.groupby(['A', 'B']).transform(np.size)
#
#d = pd.DataFrame(player.bracket, columns=('team_name','seed','team_id'))
#d.to_csv("bracket.csv")
#
#player.team_schedule_df.to_csv("team_schedule.csv")