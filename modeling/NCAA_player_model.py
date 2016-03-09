#import pandas as pd
#import numpy as np
import class_PlayerStats

player = class_PlayerStats.PlayerStats()
player.populateData()

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