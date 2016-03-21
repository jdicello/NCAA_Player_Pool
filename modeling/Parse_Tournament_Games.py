from bs4 import BeautifulSoup

import urllib2
import pandas as pd

def findTournamentGames():
    url_bracket = 'http://espn.go.com/mens-college-basketball/tournament/bracket'
    page_bracket = urllib2.urlopen(url_bracket)
    soup_bracket = BeautifulSoup(page_bracket.read(), "lxml")
    
    id_bracket = soup_bracket.find("div", {"id": "bracket"})
    
    
    tourney_games=[]
    for div_region in id_bracket.findAll("div", {"class": "region"}):
        for dl in div_region.findAll("dl"):
            try:
                rnd = dl['class'][1][5:6]
                pointer = dl.find("dd", {"class": "pointer"})
                game_id = pointer['onclick'][11:20]
                tourney_games.append([rnd,game_id])
            except:
                continue
    
    return tourney_games

def parseBoxScore(rnd, game_id):
    
    # debug
#    game_id = 400872131
#    team_id1 = 2393
#    team_id2 = 127
    away_team = ''
    home_team = ''
    try:            
        url_box = 'http://espn.go.com/mens-college-basketball/boxscore?gameId=' + str(game_id)
        page_box = urllib2.urlopen(url_box)
        soup_box = BeautifulSoup(page_box.read())
    except:
        print "Could not parse: " + url_box
    
    try:
        away_team = str(soup_box.findAll("div", {"class":"team away"})[0].findAll("span", {"class":"long-name"})[0].string)
        away_href_str = str(soup_box.findAll("div", {"class":"team away"})[0].a['href'])
        away_id = away_href_str[away_href_str.rfind('/',0)+1:]
    except:
        print "Error parsing GameID: " + str(game_id)
        if away_team == '':
            away_team = 'error'
        away_id = -1
    
    try:
        home_team = str(soup_box.findAll("div", {"class":"team home"})[0].findAll("span", {"class":"long-name"})[0].string)
        home_href_str = str(soup_box.findAll("div", {"class":"team home"})[0].a['href'])
        home_id = home_href_str[home_href_str.rfind('/',0)+1:]
    except:
        print "Error parsing GameID: " + str(game_id)
        if home_team == '':
            home_team = 'error'
        home_id = -1
    
    away = parseTeamBoxScore(soup_box, rnd, game_id, away_id, away_team, 'away', home_id, home_team)
    home = parseTeamBoxScore(soup_box, rnd, game_id, home_id, home_team, 'home', away_id, away_team)

    return pd.concat([away, home], ignore_index=True)
        
def parseTeamBoxScore(soup_box, rnd, game_id, team_id, team_name, home_away_indictor, opp_id, opp_name):
    header = 'rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    stats_df = pd.DataFrame(columns=header.split(','))
    i=0
    
    if home_away_indictor == 'home':
        box = soup_box.find("div", {"class":"col column-two gamepackage-home-wrap"})
    else:
        box = soup_box.find("div", {"class":"col column-one gamepackage-away-wrap"})
    
    try:
        for data_row in box.findAll("tbody"):
            try:
                for row in data_row.findAll("tr"):
                    data = str(rnd) + ',' + str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + home_away_indictor + ',' + str(opp_id) + ',' + opp_name + ','
                    for col in row.findAll("td"):
                        try:
                            if col.a != None:
                                if col.a.string.isalpha():
                                    data += str(col.a.string).replace('-',',') + ','
                                else:
                                    data += col.a.string.replace('-',',') + ','
                            elif col.string != None:
                                data += col.string.replace('--','0').replace('-',',') + ','
                        except:
                            continue
                            
                    data = data.rstrip(',')
                    if len(data.split(',')) == len(header.split(',')):
                        stats_df.loc[i] = [c for c in data.split(',')]
                        i += 1
            except:
                continue
    except:
        print 'no box score'
        
    return stats_df

header = 'rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
s_df = pd.DataFrame(columns=header.split(','))

for rnd, game_id in findTournamentGames():
    print 'parsing round: ' + str(rnd) + ', game_id: ' + game_id
    s_df = pd.concat([s_df, parseBoxScore(rnd, game_id)], ignore_index=True)



new_s_df = pd.concat([s_df.player + ' - ' + s_df.team_name, s_df], axis=1)
new_header = 'player-team,rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
new_s_df.columns = new_header.split(',')

output = new_s_df[new_s_df.rnd=='1'][['player-team','PTS']]
output.columns = ['player-team', 'R1']
r2 = new_s_df[new_s_df.rnd=='2'][['player-team','PTS']]
r2.columns = ['player-team', 'R2']
r3 = new_s_df[new_s_df.rnd=='3'][['player-team','PTS']]
r3.columns = ['player-team', 'R3']
r4 = new_s_df[new_s_df.rnd=='4'][['player-team','PTS']]
r4.columns = ['player-team', 'R4']
r5 = new_s_df[new_s_df.rnd=='5'][['player-team','PTS']]
r5.columns = ['player-team', 'R5']
r6 = new_s_df[new_s_df.rnd=='6'][['player-team','PTS']]
r6.columns = ['player-team', 'R6']

output.set_index('player-team', inplace=True)
r2.set_index('player-team', inplace=True)
r3.set_index('player-team', inplace=True)
r4.set_index('player-team', inplace=True)
r5.set_index('player-team', inplace=True)
r6.set_index('player-team', inplace=True)

if r2.size > 0 :
    output = pd.merge(output,r2,how='left', left_index=True, right_index=True, suffixes=('_R1', '_R2'))
    
if r3.size > 0 :
    output = pd.merge(output,r3,how='left', left_index=True, right_index=True, suffixes=('_R2', '_R3'))

if r4.size > 0 :
    output = pd.merge(output,r4,how='left', left_index=True, right_index=True, suffixes=('_R3', '_R4'))

if r5.size > 0 :
    output = pd.merge(output,r5,how='left', left_index=True, right_index=True, suffixes=('_R4', '_R5'))

if r6.size > 0 :
    output = pd.merge(output,r6,how='left', left_index=True, right_index=True, suffixes=('_R5', '_R6'))
 
output.to_csv("../output/tournament_stats.csv")

