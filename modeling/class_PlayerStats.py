# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup

import urllib2
import pandas as pd
import numpy as np
import csv
import time

class PlayerStats():
    
    def __init__(self):
        
        self.bracket = pd.DataFrame(columns=('team_name','seed','team_id'))
        #self.parsed_games = []
        self.bracket_df_row = 0
        self.team_schedule_df_row = 0
        self.player_stats_df_row = 0
        #self.header = 'team_id, team_name, home_away_indictor, opp_id, opp_name,'
        #self.header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,starters,MIN,FG,TREY,FT,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
        # could create a 
        self.header = 'game_id,team_id,team_name,home_away_indictor,opp_id,opp_name,player,MIN,FGM,FGA,TREY_M,TREY_A,FTM,FTA,OREB,DREB,REB,AST,STL,BLK,TO,PF,PTS'
        self.player_stats_df = pd.DataFrame(columns=self.header.split(','))
        self.team_schedule_df = pd.DataFrame(columns=('team_id','game_num','game_id','game_location','opp_id'))
        pre_header = 'team_id,game_num,game_id,game_location,opp_id'
        pre_team_sch = pd.read_csv("../output/pre_tournament_schedule.csv",
                                                header=0,
                                                usecols=pre_header.split(',')
                                                )
        #self.populateData(self)
        
    def loadSavedData(self):
        #self.loadParsedGames()
        self.loadPlayerStats()
    
    def loadParsedGames(self):    
        # load previously parsed games
        try:
            with open('../input/parsed_games.csv', 'rb') as parsed_game_file:
                pg_reader = csv.reader(parsed_game_file, delimiter=',')
                for row in pg_reader:
                    self.parsed_games.append(row[0])
        except:
            print "No previously parsed games"

    def loadPlayerStats(self):
        # load player stats
        try:
            self.player_stats_df = pd.read_csv("../input/2016_player_stats.csv",
                                                header=0,
                                                usecols=self.header.split(',')
                                              )
            self.player_stats_df_row = len(self.player_stats_df)
        except:
            print "No previous player stats"

    def saveData(self):
        #self.saveParsedGames()
        self.savePlayerStats()
    
    def saveParsedGames(self):
        # save games that have been parsed to save progress
        with open('../input/parsed_games.csv', 'w') as f:
            for g in self.parsed_games:
                f.writelines(g + '\n')
    
    def savePlayerStats(self):
        # load saved raw data
        self.player_stats_df.to_csv("../input/2016_player_stats.csv")
        
    def populateData(self):
        begin_time = time.time()
        print 'Loading saved data...'
        #load data from previous runs to save time
        self.loadSavedData()
        
        print 'Loading bracket...'
        # Create the bracket
        #self.populateBracket()
        self.populateRealBracket()
        
        print 'Loading team schedules...'
        # Create the team schedules
        self.populateAllTeamSchedules()

        print 'Loading parsing box scores...'
        # Load box scores
        self.parseAllBoxScores()
        
        print 'Saving data...'
        #save data
        self.saveData()
        
        print time.time() - begin_time

        # Loop through each team
#        begin_time = time.time()
#        for index, row in self.bracket.iterrows():
#            print "Pulling data for team #" + str(i) + ": " + row.team_name + ' ' + str(row.seed) + ' ' + str(row.team_id)
#            start_time = time.time()
#            
#            # First find their schedule
#            self.findTeamSchedule(row.team_id)
#    
#            # Now parse each box score
#            # function call will handle parsing a game only once
#            #for game_id in self.team_schedule_df[self.team_schedule_df['team_id'] == row.team_id].game_id:
#             #   self.parseBoxScore(game_id)
#                
#            print time.time() - start_time
#            i += 1

    def populateRealBracket(self):
        url_bracket = 'http://espn.go.com/mens-college-basketball/tournament/bracket'
        page_bracket = urllib2.urlopen(url_bracket)
        soup_bracket = BeautifulSoup(page_bracket.read())
        
        div_bracket = soup_bracket.find("div", {"id": "bracket"})
        
        #bracket=[]
        
        dt = div_bracket.findAll("dt")
        for game in dt:
            try:
                seeds = []
                seed = int(game.text[0:2].strip())
                if seed in [11,16]:
                    seeds.append(seed)
                    seeds.append(seed)
                else:
                    seeds.append(17-seed)
                    seeds.append(seed)
                
                for a in game.findAll("a"):
                    team_name = a['title']
                    team_id = a['href'].replace("http://espn.go.com/mens-college-basketball/team/_/id/","")
                    team_id = int(team_id[0:team_id.find('/',0)])
                    seed = seeds.pop()
                    #bracket.append([team_name, team_id,seed])
                    self.bracket.loc[self.bracket_df_row] = (str(team_name), int(seed), int(team_id))
                    self.bracket_df_row += 1
            except:
                continue

    def populateBracketology(self):
        
        url_bracket = 'http://espn.go.com/ncb/bracketology'
        page_bracket = urllib2.urlopen(url_bracket)
        soup_bracket = BeautifulSoup(page_bracket.read())
    
        teams = soup_bracket.findAll("div", {"class": "team"})
            
        for team in teams:
            team_name = str(team.a.string)
            seed = str(team.span.string)
            team_id = team.a['href'][team.a['href'].rfind('/',0)+1:]
            
            self.bracket.loc[self.bracket_df_row] = (str(team_name), int(seed), int(team_id))
            self.bracket_df_row += 1
            #self.bracket.append([team_name, seed, team_id])
            
        #self.bracket = self.bracket.convert_objects(convert_numeric=True)
    
    def populateAllTeamSchedules(self):
        # if the bracket is empty, find teams first
        if self.bracket.empty:
            self.createBracket()
        # only populate if team_schedules are empty
        if self.team_schedule_df.empty:
            self.bracket.team_id.apply(lambda x: self.findTeamSchedule(int(x)))
        
    def findTeamSchedule(self, team_id):
        url_sch = 'http://espn.go.com/mens-college-basketball/team/schedule/_/id/' + str(team_id)
        page_sch = urllib2.urlopen(url_sch)
        soup_sch = BeautifulSoup(page_sch.read())
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
                    opp_id = row.find("li", {"class":"team-name"}).a['href'].replace('http://espn.go.com/mens-college-basketball/team/_/id/','')
                    opp_id = opp_id[0:opp_id.find('/')]
                except:
                    # opp not tracked by ESPN
                    opp_id = '-1'
                    
                try:
                    # find game id
                    game_id = row.find("li", {"class":"score"}).a['href']
                    game_id = game_id[game_id.rfind('=',0)+1:]
                except:
                    # couldn't parse box score
                    print "Could not find box score"
                    continue
                
                #print team_id + ' ' + game_location + ' ' + opp_id + ' ' + game_id
                self.team_schedule_df.loc[self.team_schedule_df_row] = (int(team_id), int(game_num), int(game_id), str(game_location), int(opp_id))
                self.team_schedule_df_row += 1
                game_num += 1
            
            self.team_schedule_df = self.team_schedule_df.convert_objects(convert_numeric=True)
        except:
            print "Schedule error for: " + url_sch
            
    def parseAllBoxScores(self):
        ##########
        # step 1: Find all the games that have been parsed
        ##########
        parsed = self.player_stats_df.groupby(['game_id','team_id']).size()
        parsed = parsed.reset_index()
        parsed.columns = ['game_id','team_id','total']
        
        ##########
        # step 2: Left join the schedule of tourney teams to the player stats collected
        # only keep games with no stats
        ##########
        tourney_team_games = self.team_schedule_df.merge(parsed,how='left', on=['game_id','team_id'])
        tourney_team_games_not_parsed = tourney_team_games[tourney_team_games.total.isnull()]
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
        away_not_parsed = pd.concat([away_not_parsed, away_not_parsed.opp_id], axis=1)
        away_not_parsed.columns = ('game_id','team_id','opp_id','id')
        
        ##########
        # step 5: combine the gamees to parse,
        #         indicating where the "opp" should not be parsed
        ##########
        to_parse = pd.merge(tourney_team_games_not_parsed, away_not_parsed[['game_id','opp_id','id']], how='left', on=['game_id', 'opp_id'], suffixes=['_left','_right'])
        
        for i in range(len(to_parse)):
            if i%25 == 0:
                print 'Parsing box : ' + str(i)
            self.parseBoxScore(to_parse.loc[i].game_id.astype(int),to_parse.loc[i].team_id.astype(int),to_parse.loc[i].id.astype(int))
        
        
    def parseBoxScore(self, game_id, team_id1, team_id2):
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
            print "Error parsing GameID: " + str(game_id) + " team_id: " + str(team_id1) + " opp_id: " + str(team_id2)
            if away_team == '':
                away_team = 'error'
            away_id = -1
    
        try:
            home_team = str(soup_box.findAll("div", {"class":"team home"})[0].findAll("span", {"class":"long-name"})[0].string)
            home_href_str = str(soup_box.findAll("div", {"class":"team home"})[0].a['href'])
            home_id = home_href_str[home_href_str.rfind('/',0)+1:]
        except:
            print "Error parsing GameID: " + str(game_id) + " team_id: " + str(team_id1) + " opp_id: " + str(team_id2)
            if home_team == '':
                home_team = 'error'
            home_id = -1
        # If the away temm is a tournament team, parse their data
        #if away_id in [team[2] for team in self.bracket]:
        if int(away_id) in [team_id1,team_id2]:
            # Check to make sure this team/game hasn't been parsed yet.
            self.parseTeamBoxScore(soup_box, game_id, away_id, away_team, 'away', home_id, home_team)
        
        # If the home temm is a tournament team, parse their data    
#        if home_id in [team[2] for team in self.bracket]:
        if int(home_id) in [team_id1,team_id2]:
            self.parseTeamBoxScore(soup_box, game_id, home_id, home_team, 'home', away_id, away_team)
            
        #self.parsed_games.append(game_id)
        
    def parseTeamBoxScore(self, soup_box, game_id, team_id, team_name, home_away_indictor, opp_id, opp_name):
        
        if home_away_indictor == 'home':
            box = soup_box.find("div", {"class":"col column-two gamepackage-home-wrap"})
        else:
            box = soup_box.find("div", {"class":"col column-one gamepackage-away-wrap"})
        
        for data_row in box.findAll("tbody"):
            for row in data_row.findAll("tr"):
                data = str(game_id) + ',' + str(team_id) + ',' + team_name + ',' + home_away_indictor + ',' + str(opp_id) + ',' + opp_name + ','
                
                for col in row.findAll("td"):
                    if col.a != None:
                        if col.a.string.isalpha():
                            data += str(col.a.string).replace('-',',') + ','
                        else:
                            data += col.a.string.replace('-',',') + ','
                    elif col.string != None:
                        data += col.string.replace('-',',') + ','
                        
                data = data.rstrip(',')
                if len(data.split(',')) == len(self.header.split(',')):
                    self.player_stats_df.loc[self.player_stats_df_row] = [c for c in data.split(',')]
                    self.player_stats_df_row += 1

#        def parseHeader()
#            #call self.parseHeader(soup_box)
#            header_row = box.find("thead")
#    
#            for row in header_row.find("tr").findAll("th"):
#                self.header += row.string + ', '
#                s
#            self.header = self.header.rstrip(', ')
#            self.player_stats_df.reindex(columns=self.header.split(','))
#            
#            self.first_time = True   

    