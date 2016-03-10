# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup

import urllib2
import pandas as pd
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
        #load data from previous runs to save time
        self.loadSavedDate()
        
        # Create the bracket
        self.populateBracket()
        
        # Create the team schedules
        self.populateAllTeamSchedules()

        # Load box scores
        self.pareseAllBoxScores()
        
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
        
    def populateBracket(self):
        
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
            
        self.bracket = self.bracket.convert_objects(convert_numeric=True)
    
    def populateAllTeamSchedules(self):
        # if the bracket is empty, find teams first
        if len(self.bracket) == 0:
            self.createBracket()
        # only populate if team_schedules are empty
        if len(self.team_schedule_df) == 0:
            self.bracket.team_id.apply(lambda x: self.findTeamSchedule(x))
        
    def findTeamSchedule(self, team_id):
        url_sch = 'http://espn.go.com/mens-college-basketball/team/schedule/_/id/' + str(team_id)
        page_sch = urllib2.urlopen(url_sch)
        soup_sch = BeautifulSoup(page_sch.read())
        
        box = soup_sch.find("table", {"class":"tablehead"}).findAll("tr")
        game_num = 1
        for row in box:
            game_location = '-1'
            opp_id = '-1'
            game_id = '-1'
            
            try:
                # find home or away
                game_status = row.find("li", {"class":"game-status"})
                if game_status.string == 'vs':
                    game_location = 'home'
                else:
                    game_location = 'away'
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
    def parseAllBoxScores(self):
        #do something
        home_team_id = 218
        home_team_name = ''
        away_team_id = -1
        away_team_name = ''
        game_id = 400809426
        self.parseBoxScore(game_id, home_team_id, home_team_name, away_team_id, away_team_name)
        
    def parseBoxScore(self, game_id, home_team_id, home_team_name, away_team_id, away_team_name):
        # Check to see if this game has been parsed for this team.
        #l = self.team_schedule_df[self.team_schedule_df['team_id'] == team_id][self.team_schedule_df['game_id'] == game_id]
        #r = self.player_stats_df[self.player_stats_df['team_id'] == team_id][self.player_stats_df['game_id'] == game_id]

#        if game_id in self.parsed_games:
#            print "skipping :" + str(game_id)
#            return
            
        url_box = 'http://espn.go.com/mens-college-basketball/boxscore?gameId=' + str(game_id)
#        away_team = ''
#        away_id = '-1'
#        home_team = ''
#        home_id = '-1'
#        # print url_box
        try:
            page_box = urllib2.urlopen(url_box)
            soup_box = BeautifulSoup(page_box.read())
        except:
            print "Could not parse: " + url_box
        
#        try:
#            away_team = str(soup_box.findAll("div", {"class":"team away"})[0].findAll("span", {"class":"long-name"})[0].string)
#            away_href_str = str(soup_box.findAll("div", {"class":"team away"})[0].a['href'])
#            away_id = away_href_str[away_href_str.rfind('/',0)+1:]
#        except:
#            print "Error: " + away_team
#            print "GameID: " + str(game_id)
#            if away_team == '':
#                away_team = 'error'
#    
#        try:
#            home_team = str(soup_box.findAll("div", {"class":"team home"})[0].findAll("span", {"class":"long-name"})[0].string)
#            home_href_str = str(soup_box.findAll("div", {"class":"team home"})[0].a['href'])
#            home_id = home_href_str[home_href_str.rfind('/',0)+1:]
#        except:
#            print "Error: " + home_team
#            print "GameID: " + str(game_id)
#            if home_team == '':
#                home_team = 'error'
    
        # If the away temm is a tournament team, parse their data
        #if away_id in [team[2] for team in self.bracket]:
        if away_team_id > 0:
            # Check to make sure this team/game hasn't been parsed yet.
            self.parseTeamBoxScore(soup_box, game_id, away_team_id, away_team, 'away', home_id, home_team)
        
        # If the home temm is a tournament team, parse their data    
#        if home_id in [team[2] for team in self.bracket]:
        if home_team_id > 0:
            self.parseTeamBoxScore(soup_box, game_id, home_team_id, home_team, 'home', away_id, away_team)
            
        self.parsed_games.append(game_id)
        
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

    