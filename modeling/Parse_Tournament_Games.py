from __future__ import print_function
from bs4 import BeautifulSoup
from urllib.request import urlopen

import pandas as pd
import concurrent.futures
import time

import GoogleSheets as gs

def findTournamentGames():
    # url_bracket = 'http://espn.go.com/mens-college-basketball/tournament/bracket'
    # page_bracket = urlopen(url_bracket)
    # soup_bracket = BeautifulSoup(page_bracket.read(), "lxml")

    
    # # id_bracket = soup_bracket.find("div", {"id": "bracket"})
    # id_bracket = soup_bracket.findAll("a")
    
    # a = id_bracket[0]

    # print([a['href'] for a in id_bracket if a['href'] is not None])
    
    # tourney_games=[]
    # for div_region in id_bracket.findAll("div", {"class": "region"}):
    #     #Rounds 1-2
    #     for dl in div_region.findAll("dl"):
    #         try:
    #             rnd = dl['class'][1][5:6]
    #             pointer = dl.find("dd", {"class": "pointer"})
    #             game_id = pointer['onclick'][11:20]
    #             tourney_games.append([rnd,game_id])
    #         except:
    #             continue
        
    #     #Rounds 3-4    
    #     for div in div_region.findAll("div"):
    #         try:
    #             rnd = div['class'][1][5:6]
    #             pointer = div.find("dd", {"class": "pointer"})
    #             game_id = pointer['onclick'][11:20]
    #             tourney_games.append([rnd,game_id])
    #         except:
    #             continue
            
    # for div_finalfour in id_bracket.findAll("div", {"id": "finalfour"}):
    #     for div in div_finalfour.findAll("div"):
    #         try:
    #             rnd = div['class'][1][5:6]
    #             pointer = div.find("dd", {"class": "pointer"})
    #             game_id = pointer['onclick'][11:20]
    #             tourney_games.append([rnd,game_id])
    #         except:
    #             continue
    tourney_games = \
    [
        # [1,401522123], #Alabama
        # [1,401522122], #Maryland
        # [1,401522131], #San Diego St
        # [1,401522130], #Virginia
        # [1,401522152], #Creighton
        # [1,401522151], #Baylor
        # [1,401522135], #Mizzu
        # [1,401522134], #Zona
        # [1,401522145], #Purdue
        # [1,401522144], #Memphis
        # [1,401522128], #Duke
        # [1,401522129], #Tenn
        # [1,401522154], #Kentucky
        # [1,401522153], #Kansas St
        # [1,401522142], #MSU
        # [1,401522141], #Marq
        # [1,401522121], #Houston
        # [1,401522120], #Iowa
        # [1,401522139], #Miami
        # [1,401522138], #IU
        # [1,401522157], #Iowa St
        # [1,401522156], #Xavier
        # [1,401522127], #PSU
        # [1,401522124], #Texas
        # [1,401522125], #Kansas
        # [1,401522126], #Arkansas
        # [1,401522137], #St Marys
        # [1,401522136], #UConn
        # [1,401522149], #TCU
        # [1,401522148], #Zags
        # [1,401522133], #Northwestern
        # [1,401522132], #UCLA
        # [2,401522159], #bama
        # [2,401522167], #SDS
        # [2,401522170], #Mizu
        # [2,401522161], #Hou
        # [2,401522165], #PSU
        # [2,401522166], #Duke
        # [2,401522162], #Arkansas
        # [2,401522169], #UCLA
        # [2,401522179], #Creighton
        # [2,401522173], #IU
        # [2,401522183], #PITT
        # [2,401522174], #FAU
        # [2,401522180], #KSU
        # [2,401522175], #MSU
        # [2,401522172], #UCONN
        # [2,401522178], #Zags
        # [3,401522192], #Tenn
        # [3,401522191], #KSU
        # [3,401522188], #Arkansas
        # [3,401522186], #Zags
        # [3,401522197], #Bama
        # [3,401522198], #Creighton
        # [3,401522195], #HOU
        # [3,401522194], #Texas
        # [4,401522193], #KSU
        # [4,401522190], #Zags
        # [4,401522199], #Creighton
        # [4,401522196], #Texas
        # [5,401522200], #FAU
        # [5,401522201], #UConn
        [6,401522202], #UConn
    ]
    return tourney_games

def parseBoxScore(row):
    
    # debug
#    game_id = 400946417
#    team_id1 = 116
#    team_id2 = 222
    rnd = row[0]
    game_id = row[1]
    away_team = ''
    home_team = ''
    try:            
        url_box = 'http://www.espn.com/mens-college-basketball/boxscore?gameId=' + str(game_id)
        page_box = urlopen(url_box)
        soup_box = BeautifulSoup(page_box.read(), "lxml")
    except:
        print("Could not parse: " + url_box)
    
    try:
        away_team = str(soup_box.findAll("div", {"class":"team away"})[0].findAll("span", {"class":"long-name"})[0].string)
        away_href_str = str(soup_box.findAll("div", {"class":"team away"})[0].a['href'])
        away_id = away_href_str[away_href_str.find('/id/')+4:away_href_str.rfind('/',0)]
    except:
        print("Error parsing GameID: " + str(game_id))
        if away_team == '':
            away_team = 'error'
        away_id = -1
    
    try:
        home_team = str(soup_box.findAll("div", {"class":"team home"})[0].findAll("span", {"class":"long-name"})[0].string)
        home_href_str = str(soup_box.findAll("div", {"class":"team home"})[0].a['href'])
        home_id = home_href_str[home_href_str.find('/id/')+4:home_href_str.rfind('/',0)]
    except:
        print("Error parsing GameID: " + str(game_id))
        if home_team == '':
            home_team = 'error'
        home_id = -1
    
    away = parseTeamBoxScore(soup_box, rnd, game_id, away_id, away_team, 'away', home_id, home_team)
    home = parseTeamBoxScore(soup_box, rnd, game_id, home_id, home_team, 'home', away_id, away_team)

    return pd.concat([away, home], ignore_index=True)

def parseTeamBoxScore(soup_box, rnd, game_id, team_id, team_name, home_away_indictor, opp_id, opp_name):
    header = 'rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    player_stats_df = pd.DataFrame(columns=header.split(','))    
    player_stats_df_row = 0
    player_stats_df.team_id = player_stats_df.team_id.astype(int)
    player_stats_df.game_id = player_stats_df.game_id.astype(int)
    player_stats_df.opp_id = player_stats_df.opp_id.astype(int)
    
    if home_away_indictor == 'home':
        box = soup_box.find("div", {"class":"col column-two gamepackage-home-wrap"})
    else:
        box = soup_box.find("div", {"class":"col column-one gamepackage-away-wrap"})
    
    for data_row in box.findAll("tbody"):
        for row in data_row.findAll("tr"):
            data = str(rnd) + ',' + str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + home_away_indictor + ',' + str(opp_id) + ',' + opp_name + ','
            
            for col in row.findAll("td"):
                if col.a != None:
                    if col.a.span.text.isalpha():
                        data += str(col.a.text) + ','
                    else:
                        data += col.a.span.text  + ','
                elif col.text != None:
                    data += col.text.replace('--','0').replace('-', ',') + ','
                    
            data = data.rstrip(',')
            if len(data.split(',')) == len(header.split(',')):
                player_stats_df.loc[player_stats_df_row] = [c for c in data.split(',')]
                player_stats_df_row += 1
                
    return player_stats_df

def parseBoxScore_2023(row):
    # debug
#    game_id = 401522122
#    team_id1 = 116
#    team_id2 = 222
    # row = [3,401522197]
    rnd = row[0]
    game_id = row[1]
    away_team = ''
    home_team = ''
    header = 'rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    player_stats_df = pd.DataFrame(columns=header.split(','))
    player_stats_df_row = 0
    player_stats_df.team_id = player_stats_df.team_id.astype(int)
    player_stats_df.game_id = player_stats_df.game_id.astype(int)
    player_stats_df.opp_id = player_stats_df.opp_id.astype(int)

    try:
        url_box = 'https://www.espn.com/mens-college-basketball/boxscore/_/gameId/' + str(game_id)
        page_box = urlopen(url_box)
        soup_box = BeautifulSoup(page_box.read(), "lxml")
    except:
        print("Could not parse: " + url_box)

    try:
        teams = soup_box.findAll("a", {"class":"AnchorLink truncate"}) # should find 2
        boxscores = soup_box.find("div", {"class":"Boxscore Boxscore__ResponsiveWrapper"}).findAll("div", {"class":"Wrapper"}) # should find 2

        if len(teams) != len(boxscores):
            print("stuff")
            raise("Teams != boxscores")

        opp_id = -1
        opp_name = '@@@@'
        for i in range(len(teams)):
            team_name = teams[i].string
            team_id = teams[i]['href'][teams[i]['href'].find('/id/')+4:teams[i]['href'].rfind('/',0)]
            header_data = str(rnd) + ',' + str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + str(i) + ',' + str(opp_id) + ',' + opp_name + ','

            tbodys = boxscores[i].findAll("tbody")
            players = tbodys[0].findAll("tr")
            scores = tbodys[1].findAll("tr")

            for ii in range(len(players)):
                if players[ii].a != None:
                    # headerdata = str(rnd) + ',' + str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + str(i) + ',' + str(opp_id) + ',' + opp_name + ','
                    player_data = ''
                    row_data = ''
                    player_data += str(players[ii].a.text) + ','
                    player_name = str(players[ii].a.text)
                    for col in scores[ii].findAll("td"):
                        player_data += col.text.replace('--','0').replace('-', ',') + ','
                        row_data += col.text.replace('--','0').replace('-', ',') + ','
                else:
                    continue

                player_data = player_data.rstrip(',')
                row_data = row_data.rstrip(',')
                if len(player_data.split(',')) == 16:
                    data = header_data + player_name + ',0,' + row_data
                else:
                    data = header_data + player_data

                if len(data.split(',')) == len(header.split(',')):
                    player_stats_df.loc[player_stats_df_row] = [c for c in data.split(',')]
                    player_stats_df_row += 1

    except  BaseException as e:
        print(f"Error parsing game: {game_id} - {e}")

    return player_stats_df

def parseTourneyGames():
    header = 'rnd,game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    player_stats_df = pd.DataFrame(columns=header.split(','))

    player_stats_df = pd.read_csv("../output/raw_tournament_stats.csv",usecols=range(1,len(header.split(','))+1))
    player_stats_df.rnd = player_stats_df.rnd.astype(str)
    
    #games = to_parse.as_matrix()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor2:
        # Start the load operations and mark each future with its URL
        #future_team = {executor.submit(findTeamSchedule, team_id): team_id for team_id in bracket.team_id}
        #future_game = {executor2.submit(parseBoxScore, int(game_id), int(team_id1), int(team_id2)): game_id, team_id1, team_id2 for game_id, team_id1, team_id2 in games}
        future_game = {executor2.submit(parseBoxScore_2023, row): row for row in findTournamentGames()}
        for future in concurrent.futures.as_completed(future_game):
            game = future_game[future]
            try:
                data = future.result()
                player_stats_df = pd.concat([player_stats_df,data])
            except Exception as exc:
                print('%r generated an exception: %s' % (game, exc))
            else:
                print('Parsed game: %r.' % (game))
                
    return player_stats_df

def main():
    
    s_df = parseTourneyGames()
    # s_df.to_csv("../output/raw_tournament_stats.csv")
    
    new_s_df = pd.concat([s_df.team_name + ' - ' + s_df.player, s_df], axis=1)
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
    
    output.R1.loc[output.R1.isnull()] = ''
    if r2.size > 0 :
        output = pd.merge(output,r2,how='left', left_index=True, right_index=True, suffixes=('_R1', '_R2'))
        output.R2.loc[output.R2.isnull()] = ''
        
    if r3.size > 0 :
        output = pd.merge(output,r3,how='left', left_index=True, right_index=True, suffixes=('_R2', '_R3'))
        output.R3.loc[output.R3.isnull()] = ''
    
    if r4.size > 0 :
        output = pd.merge(output,r4,how='left', left_index=True, right_index=True, suffixes=('_R3', '_R4'))
        output.R4.loc[output.R4.isnull()] = ''
    
    if r5.size > 0 :
        output = pd.merge(output,r5,how='left', left_index=True, right_index=True, suffixes=('_R4', '_R5'))
        output.R5.loc[output.R5.isnull()] = ''
    
    if r6.size > 0 :
        output = pd.merge(output,r6,how='left', left_index=True, right_index=True, suffixes=('_R5', '_R6'))
        output.R6.loc[output.R6.isnull()] = ''   
     
    output.reset_index(inplace=True)
    output.to_csv("../output/tournament_stats.csv")

    gs.writeGoogleSheet(output.values.tolist(), '1ATMNebnHKSM8J0F_W86EDu2l9i_jvSJEOJnW6-9aFpg', 'All Stats!A2')
    # gs.writeGoogleSheet(['this','is','a','test'], '1ATMNebnHKSM8J0F_W86EDu2l9i_jvSJEOJnW6-9aFpg', 'All Stats!A2')

if __name__ == '__main__':
    while 1:
        main()
       time.sleep(60)
    
