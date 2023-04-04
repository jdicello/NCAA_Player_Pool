# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 21:54:53 2017

@author: jdicello
"""
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
#import csv
#import time
import concurrent.futures

#initialize variables
#bracket = pd.DataFrame(columns=('team_name','seed','team_id'))
##parsed_games = []
#bracket_df_row = 0
#team_schedule_df_row = 0
#player_stats_df_row = 0
##header = 'team_id, team_name, home_away_indictor, opp_id, opp_name,'
##header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,starters,MIN,FG,TREY,FT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
## could create a 
#header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
#player_stats_df = pd.DataFrame(columns=header.split(','))
#team_schedule_df = pd.DataFrame(columns=('team_id','game_num','game_id','game_location','opp_id'))
#pre_header = 'team_id,game_num,game_id,game_location,opp_id'

#functions


#
#test_df = loadPlayerStats('2017_player_stats_test.csv')
#test_df = loadPlayerStats('2017_player_stats_BOM.csv')
#test_df = loadPlayerStats('2017_player_stats.csv')

def loadPlayerStats(filename):
    # load player stats
    col_headers = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    player_stats_df = pd.DataFrame(columns=col_headers.split(','))
    player_stats_df.team_id = player_stats_df.team_id.astype(int)
    player_stats_df.game_id = player_stats_df.game_id.astype(int)
    player_stats_df.opp_id = player_stats_df.opp_id.astype(int)
    try:
        player_stats_df = pd.read_csv("../input/%s" % (filename),
                                            engine='c',
                                            sep=',',
                                            header=0,
                                            usecols=col_headers.split(','),
                                            index_col=False
                                            )
        #player_stats_df_row = len(self.player_stats_df)
    except Exception as exc:
        print('Loading player stats generated an exception: %s' % (exc))
    
    return player_stats_df
            
def saveParsedGames(parsed_games):
    # save games that have been parsed to save progress
    with open('../input/parsed_games.csv', 'w') as f:
        for g in parsed_games:
            f.writelines(g + '\n')
            
def populateBracketology():
    
    url_bracket = 'http://espn.go.com/ncb/bracketology'
    page_bracket = urlopen(url_bracket)
    soup_bracket = BeautifulSoup(page_bracket.read(), "lxml")

    # teams = soup_bracket.findAll("div", {"class": "team"})
    teams = soup_bracket.findAll("li", {"class": "bracket__item"})
    
    bracket = pd.DataFrame(columns=('team_name','seed','team_id'))
    bracket_df_row = 0
    
    for team in teams:
        try:
            team_name = str(team.a.text.replace(' - aq','').replace('aq - ','').strip())
            seed = str(team.span.string)
            team_id = team.a['href'].replace('https://www.espn.com/mens-college-basketball/team/_/id/','')
            # team_id = href[0:href.find('/',0)]
            print((str(team_name), int(seed), int(team_id)))
            bracket.loc[bracket_df_row] = (str(team_name), int(seed), int(team_id))
            bracket_df_row += 1
            
            if len(team.findAll("a")) > 1:
                new_team = team.findAll("a")[1]
                team_name = str(new_team.text.replace(' - aq','').replace('aq - ','').strip())
                seed = str(team.findAll("span")[1].string)
                team_id = new_team['href'].replace('https://www.espn.com/mens-college-basketball/team/_/id/','')
                # team_id = new_href[0:new_href.find('/',0)]
                
                bracket.loc[bracket_df_row] = (str(team_name), int(seed), int(team_id))
                bracket_df_row += 1
            #self.bracket.append([team_name, seed, team_id])
        except:
            continue

    bracket.seed = bracket.seed.astype(int)
    bracket.team_id = bracket.team_id.astype(int)
    
    return bracket

def populateBracketology_tourney():
    
    url_bracket = 'http://espn.go.com/mens-college-basketball/tournament/bracket'
    page_bracket = urlopen(url_bracket)
    soup_bracket = BeautifulSoup(page_bracket.read(), "lxml")

    bracket = pd.DataFrame(columns=('team_name','seed','team_id'))
    bracket_df_row = 0

    id_bracket = soup_bracket.find("div", {"id": "bracket"})

    for div_region in id_bracket.findAll("div", {"class": "region"}):
        for dl in div_region.findAll("dl"):
                for a in dl.findAll("a"):
                    try:
                        team_name = str(a.string)
                        seed = 99
                        tmp = a['href'].replace('http://www.espn.com/mens-college-basketball/team/_/id/','')
                        team_id = tmp[0:tmp.find('/')]
                        
                        bracket.loc[bracket_df_row] = (str(team_name), int(seed), int(team_id))
                        bracket_df_row += 1
                    except:
                        continue

    bracket.seed = bracket.seed.astype(int)
    bracket.team_id = bracket.team_id.astype(int)
    
    return bracket

def populateAllTeamSchedules(bracket):

    team_schedule_df = pd.DataFrame(columns=('team_id','game_num','game_id','game_location','opp_id'))
    team_schedule_df.team_id = team_schedule_df.team_id.astype(int)
    team_schedule_df.game_num = team_schedule_df.game_num.astype(int)
    team_schedule_df.game_id = team_schedule_df.game_id.astype(int)
    team_schedule_df.opp_id = team_schedule_df.opp_id.astype(int)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        # Start the load operations and mark each future with its URL
        future_team = {executor.submit(findTeamSchedule, team_id): team_id for team_id in bracket.team_id}
        for future in concurrent.futures.as_completed(future_team):
            team_id = future_team[future]
            try:
                data = future.result()
#                print(data)
                team_schedule_df = pd.concat([team_schedule_df,data])
                # team_schedule_df = team_schedule_df.append(data)
            except Exception as exc:
                print('%r generated an exception: %s' % (team_id, exc))
            else:
                print('Schedule complete: %r.' % (team_id))
    
    return team_schedule_df
    
def findTeamSchedule(team_id):
    url_sch = 'http://espn.go.com/mens-college-basketball/team/schedule/_/id/' + str(team_id)
    page_sch = urlopen(url_sch)
    soup_sch = BeautifulSoup(page_sch.read(), "lxml")
    
    team_schedule_df = pd.DataFrame(columns=('team_id','game_num','game_id','game_location','opp_id'))
    team_schedule_df_row = 0
    
    try:
        box = soup_sch.find("table", {"class":"Table"}).findAll("tr")
#        box = soup_sch.find("table", {"class":"Table2__table-scroller Table2__table"}).findAll("tr")
        game_num = 1
        for row in box:
            
            try:
                # find home or away
                game_status = row.find("span", {"class":"pr2"})
                if game_status.string == 'vs':
                    game_location = 'home'
                else:
                    game_location = 'away'
    
                # Make sure the game has a result and is completed
                # Don't want to parse games that are in progress or didn't play
                game_id = row.find("span", {"class":"ml4"}).a['href']
                game_id = game_id[game_id.rfind('/',0)+1:]
            except:
                continue
                
            try:
                # find team id, if exists
                opp_id = row.find("span", {"class":"tc pr2"}).a['href'].replace('/mens-college-basketball/team/_/id/','')
                opp_id = opp_id[0:opp_id.find('/')]
            except:
                # opp not tracked by ESPN
                opp_id = '-1'
            
            #print(team_id + ' ' + game_location + ' ' + opp_id + ' ' + game_id
            team_schedule_df.loc[team_schedule_df_row] = (int(team_id), int(game_num), int(game_id), str(game_location), int(opp_id))
            team_schedule_df_row += 1
            game_num += 1
        
        team_schedule_df.team_id = team_schedule_df.team_id.astype(int)
        team_schedule_df.game_num = team_schedule_df.game_num.astype(int)
        team_schedule_df.game_id = team_schedule_df.game_id.astype(int)
        team_schedule_df.opp_id = team_schedule_df.opp_id.astype(int)
    except BaseException as e:
        print("Schedule error for: " + url_sch + "\n" + str(e))
    
    return team_schedule_df

def findTeamSchedule_2018(team_id):
    url_sch = 'http://espn.go.com/mens-college-basketball/team/schedule/_/id/' + str(team_id)
    page_sch = urlopen(url_sch)
    soup_sch = BeautifulSoup(page_sch.read(), "lxml")
    
    team_schedule_df = pd.DataFrame(columns=('team_id','game_num','game_id','game_location','opp_id'))
    team_schedule_df_row = 0
    
    try:
        box = soup_sch.find("table", {"class":"tablehead"}).findAll("tr")
        game_num = 1
        for row in box:
            try:
                # find home or away
                game_status = row.find("li", {"class":"game-status"})
                if game_status.string == 'vs':
                    game_location = 'home'
                else:
                    game_location = 'away'
    
                # Make the game has a result and is completed
                # Don't want to parse games that are in progress or didn't play
                game_win = row.find("li", {"class":"game-status win"})
                game_loss = row.find("li", {"class":"game-status loss"})
                
                if game_win == None and game_loss == None:
                    continue
            except:
                continue
                
            try:
                # find team id, if exists
                opp_id = row.find("li", {"class":"team-name"}).a['href'].replace('http://www.espn.com/mens-college-basketball/team/_/id/','')
                opp_id = opp_id[0:opp_id.find('/')]
            except:
                # opp not tracked by ESPN
                opp_id = '-1'
                
            try:
                # find game id
                game_id = row.find("li", {"class":"score"}).a['href']
                game_id = game_id[game_id.rfind('/',0)+1:]
            except:
                # couldn't parse box score
                print("Could not find box score")
                continue
            
            #print(team_id + ' ' + game_location + ' ' + opp_id + ' ' + game_id
            team_schedule_df.loc[team_schedule_df_row] = (int(team_id), int(game_num), int(game_id), str(game_location), int(opp_id))
            team_schedule_df_row += 1
            game_num += 1
        
        team_schedule_df.team_id = team_schedule_df.team_id.astype(int)
        team_schedule_df.game_num = team_schedule_df.game_num.astype(int)
        team_schedule_df.game_id = team_schedule_df.game_id.astype(int)
        team_schedule_df.opp_id = team_schedule_df.opp_id.astype(int)
    except BaseException as e:
        print("Schedule error for: " + url_sch + "\n" + str(e))
    
    return team_schedule_df

def parseTeamBoxScore(soup_box, game_id, team_id, team_name, home_away_indictor, opp_id, opp_name):
    
    header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
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
            data = str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + home_away_indictor + ',' + str(opp_id) + ',' + opp_name + ','
            
            for col in row.findAll("td"):
                if col.a != None:
                    if col.a.span.text.isalpha():
                        data += str(col.a.text) + ','
                    else:
                        data += col.a.span.text + ','
                elif col.text != None:
                    data += col.text.replace('--','0').replace('-', ',') + ','
                    
            data = data.rstrip(',')
            if len(data.split(',')) == len(header.split(',')):
                player_stats_df.loc[player_stats_df_row] = [c for c in data.split(',')]
                player_stats_df_row += 1
                
    return player_stats_df    

def parseBoxScore(row):
    game_id = row[0]
    team_id1 = row[1]
    team_id2 = row[2]
    away_team = ''
    home_team = ''
    header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
    player_stats_df = pd.DataFrame(columns=header.split(',')) 
    player_stats_df.team_id = player_stats_df.team_id.astype(int)
    player_stats_df.game_id = player_stats_df.game_id.astype(int)
    player_stats_df.opp_id = player_stats_df.opp_id.astype(int)
    
    try:            
        url_box = 'http://espn.go.com/mens-college-basketball/boxscore?gameId=' + str(game_id)
        page_box = urlopen(url_box)
        soup_box = BeautifulSoup(page_box.read(), "lxml")
    except:
        print("Could not parse: " + url_box)
    
    try:
        away_team = str(soup_box.findAll("div", {"class":"team away"})[0].findAll("span", {"class":"long-name"})[0].string)
        away_href_str = str(soup_box.findAll("div", {"class":"team away"})[0].a['href'])
        away_id = away_href_str[away_href_str.find('/id/')+4:away_href_str.rfind('/',0)]
    except:
        print("Error parsing GameID: " + str(game_id) + " team_id: " + str(team_id1) + " opp_id: " + str(team_id2))
        if away_team == '':
            away_team = 'error'
        away_id = -1

    try:
        home_team = str(soup_box.findAll("div", {"class":"team home"})[0].findAll("span", {"class":"long-name"})[0].string)
        home_href_str = str(soup_box.findAll("div", {"class":"team home"})[0].a['href'])
        home_id = home_href_str[home_href_str.find('/id/')+4:home_href_str.rfind('/',0)]
    except:
        print("Error parsing GameID: " + str(game_id) + " team_id: " + str(team_id1) + " opp_id: " + str(team_id2))
        if home_team == '':
            home_team = 'error'
        home_id = -1
    # If the away temm is a tournament team, parse their data
    #if away_id in [team[2] for team in self.bracket]:
    if int(away_id) in [team_id1,team_id2]:
        # Check to make sure this team/game hasn't been parsed yet.
        player_stats_df = pd.concat([player_stats_df, parseTeamBoxScore(soup_box, game_id, away_id, away_team, 'away', home_id, home_team)])
    
    # If the home temm is a tournament team, parse their data    
#        if home_id in [team[2] for team in self.bracket]:
    if int(home_id) in [team_id1,team_id2]:
        player_stats_df = pd.concat([player_stats_df,parseTeamBoxScore(soup_box, game_id, home_id, home_team, 'home', away_id, away_team)])
        
    return player_stats_df

def parseBoxScore_2023(row):
    game_id = row[0]
    # team_id1 = row[1]
    # team_id2 = row[2]
    # away_team = ''
    # home_team = ''
    header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
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
            data = str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + str(i) + ',' + str(opp_id) + ',' + opp_name + ','

            tbodys = boxscores[i].findAll("tbody")
            players = tbodys[0].findAll("tr")
            scores = tbodys[1].findAll("tr")

            for ii in range(len(players)):
                if players[ii].a != None:
                    data = str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + str(i) + ',' + str(opp_id) + ',' + opp_name + ','
                    data += str(players[ii].a.text) + ','
                    for col in scores[ii].findAll("td"):
                        data += col.text.replace('--','0').replace('-', ',') + ','
                else:
                    continue

                data = data.rstrip(',')
                if len(data.split(',')) == len(header.split(',')):
                    player_stats_df.loc[player_stats_df_row] = [c for c in data.split(',')]
                    player_stats_df_row += 1

    except  BaseException as e:
        print(f"Error parsing game: {game_id} - {e}")

    return player_stats_df

def parseAllBoxScores(team_schedule_df,filename):
    
#    header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
#    player_stats_df = pd.DataFrame(columns=header.split(','))    
#    player_stats_df.team_id = player_stats_df.team_id.astype(int)
#    player_stats_df.game_id = player_stats_df.game_id.astype(int)
#    player_stats_df.opp_id = player_stats_df.opp_id.astype(int)
    
    player_stats_df = loadPlayerStats(filename)
    
    ##########
    # step 1: Find all the games that have been parsed
    ##########
    parsed = player_stats_df.groupby(['game_id','team_id']).size()
    parsed = parsed.reset_index()
    parsed.columns = ['game_id','team_id','total']
    
    ##########
    # step 2: Left join the schedule of tourney teams to the player stats collected
    # only keep games with no stats
    ##########
    tourney_team_games = team_schedule_df.merge(parsed,how='left', on=['game_id','team_id'])
    tourney_team_games_not_parsed = tourney_team_games[tourney_team_games.total.isnull()]
    if len(tourney_team_games_not_parsed) == 0:
        return player_stats_df
    ##########
    # step 3: remove duplicate games (when both teams in a game are tourney teams) 
    ##########
    tourney_team_games_not_parsed = tourney_team_games_not_parsed.groupby('game_id').agg({'team_id': np.min, 'opp_id':np.max, 'total':np.max})
    tourney_team_games_not_parsed.reset_index(inplace=True)
    ##########
    # step 4: find games where the "opp" is a tourney team that has already been parsed
    ##########
    away = pd.merge(tourney_team_games_not_parsed, parsed, how='left', left_on=['game_id','opp_id'], right_on=('game_id','team_id'))
    away_not_parsed = away[away.total_y.isnull()][['game_id','team_id_x','opp_id']]
    away_not_parsed.columns = ('game_id','team_id','opp_id')
    # adding another opp_id column because it keeps getting lost in the next merge
#    away_not_parsed = pd.concat([away_not_parsed, away_not_parsed.opp_id], axis=1)
#    away_not_parsed.columns = ('game_id','team_id','opp_id','id')
    
    ##########
    # step 5: combine the gamees to parse,
    #         indicating where the "opp" should not be parsed
    ##########
    to_parse = pd.merge(tourney_team_games_not_parsed[['game_id','team_id','opp_id']], away_not_parsed[['game_id','opp_id']], how='left', on=['game_id', 'opp_id'], suffixes=['_left','_right'])
    games = to_parse.values
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor2:
        # Start the load operations and mark each future with its URL
        #future_team = {executor.submit(findTeamSchedule, team_id): team_id for team_id in bracket.team_id}
        #future_game = {executor2.submit(parseBoxScore, int(game_id), int(team_id1), int(team_id2)): game_id, team_id1, team_id2 for game_id, team_id1, team_id2 in games}
        future_game = {executor2.submit(parseBoxScore_2023, row): row for row in games}
        for future in concurrent.futures.as_completed(future_game):
            game = future_game[future]
            try:
                data = future.result()
                # player_stats_df = player_stats_df.append(data)
                player_stats_df = pd.concat([player_stats_df, data])
            except Exception as exc:
                print('%r generated an exception: %s' % (game, exc))
            else:
                print('Parsed game: %r.' % (game))
    
    return player_stats_df
    
#    for i in range(len(to_parse)):
#        if i%25 == 0:
#            print('Parsing box : ' + str(i))
#        parseBoxScore(to_parse.loc[i].game_id.astype(int),to_parse.loc[i].team_id.astype(int),to_parse.loc[i].opp_id.astype(int))
    