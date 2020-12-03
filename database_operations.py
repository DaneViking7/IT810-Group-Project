import csv
import sys 
import mysql.connector #need to run 'sudo pip3 install mysql-connector-python' to get the package
from mysql.connector import Error

#  This program is intended to be imported as a module and controlled from other programs
#  This program manages connections to the database used to store parsed information
#  The "install_database", "collect_data_statistics" programs depend upon this module.
#


class DB_Parser(object):
    def __init__(self):
        self.discussion_id=[]
        self.title=[]
        self.time=[]
        self.message=[]
        self.url=[]
        self.csv_file_name = 'civ_comments.csv'
        self.count_messages=0
        self.select_query_description=[]
        self.select_query_list=[]
        self.database_name="CIV_COMMENTS"
        self.password=''
        self.user='root'
        self.host='localhost'
        self.table_name='MESSAGES'
        self.create_messages_table="CREATE TABLE "+str(self.table_name)+" (DISCUSSION_ID INT, TITLE TEXT, TIME TEXT, MESSAGE TEXT, URL TEXT);"
        self.add_message="INSERT INTO MESSAGES (DISCUSSION_ID, TITLE, TIME, MESSAGE, URL) VALUES (%s,%s,%s,%s,%s)"
        self.creation_database_string="CREATE DATABASE "+str(self.database_name)+";"
        try:
            self.connection = mysql.connector.connect(host=self.host, database=self.database_name,user=self.user,password=self.password)
        except:
            print ('Error connecting to database in class declaration', sys.exc_info()[0])
        self.cursor=self.connection.cursor()
        
    def func_create_database(self):
        ## creating a new connection, rather than using class connection, as the database has not yet been created
        print("Running func_create_database")  
        try:
            connection= mysql.connector.connect(host=self.host, user=self.user, password=self.password)
            db_info=connection.get_server_info()
            print('connected to database in create database function',db_info)
            db_cursor=connection.cursor(buffered=True)
            try:
                db_cursor.execute("use "+str(self.database_name)+";")
                db_cursor.execute("select database();")
                record = db_cursor.fetchall()
            except Error as e:
                print ('Error selecting the database ',e)
            #print('response from query',response)
            try:
                db_list=[]
                for database in record:
                    db_list.append(database[0])#fetchall returns a list of tuples, only interested in 0 index of the tuples
                if db_list:
                    if self.database_name in db_list:
                        print("database ",self.database_name," already exists.  skipping creation")
                    else:
                        print("database ",self.database_name," not in list of existing databases.  creating db")
                        db_cursor.execute(self.creation_database_string)
                        connection.commit()    
                        connection.close()
                        db_cursor.close()
                else:
                    print("no databases returned, creating ",self.database_name," ")
                    db_cursor.execute(self.creation_database_string)
                    connection.commit()
                    connection.close()
                    db_cursor.close()
            except Error as e:
                print("Error checking for database within db_list")
        except Error as e:
            print ("Error while creating database", e)
    
    def func_create_table(self):
        print("Running func_create_table")  
        try:
            connection = mysql.connector.connect(host=self.host, user=self.user, password=self.password)
        except:
            print ('Error connecting to database in class declaration', sys.exc_info()[0])   
        try:
            db_Info = self.connection.get_server_info()
            print("Connected to MYSQL Server verion", db_Info)
            #cursor = self.connector.cursor()
            cursor=connection.cursor(buffered=True)
            try:
                cursor.execute("use "+str(self.database_name)+";")
                cursor.execute("select database();")
                record = cursor.fetchall()
                print("You're connected to the database: ", record)
            except Error as e:
                print ('Error selecting the database ',e)
            #insert all records from csv into database
            ## Check to see if table already exists in database
            try:
                table_list=[]
                #table_select_string="SHOW TABLES FROM "+str(self.database_name)+" ;"
                table_select_string="SHOW TABLES FROM "+str(self.database_name)+";"
                cursor.execute(table_select_string)
                record=cursor.fetchall()
                for table in record:
                    table_list.append(table[0])#fetchall returns a list of tuples, only interested in 0 index of the tuples
                    
                if self.table_name in table_list:
                    print("Table ",self.table_name," already exists.  skipping creation")
                else:
                    print("table ",self.table_name," not in database ",self.database_name,".  creating table")
                    cursor.execute(self.create_messages_table)
                    connection.commit()
                    connection.close()
                    cursor.close()  
            except Error as e:
                print('Problem searching through table results ',e)  
        except Error as e:
            print ("Error while connecting to MySQL", e)
       
         
    def func_read_data_from_CSV(self):
        #local_count_messages=0 #used to count total number of messages
        print("Loading data from csv file into memory")
        try:
            try:
                with open(self.csv_file_name, newline='', encoding='utf-8') as csvfile:
                    dbd_file_reader = csv.reader(csvfile)                
                    try:
                        for row in dbd_file_reader:
                            #Store CSV fields into arrays
                            #local_count_messages+=1
                            try:
                                self.count_messages+=1
                            except:
                                print ('Error adding to count_messages in func_read_data_from_CSV', sys.exc_info()[0])
                            try:
                                self.discussion_id.append(row[0])
                            except:
                                print ('Error updating discussion_id in func_read_data_from_CSV', sys.exc_info()[0])
                            try:
                                self.title.append(row[1])
                            except:
                                print ('Error updating title in func_read_data_from_CSV', sys.exc_info()[0])
                            try:
                                self.time.append(row[2])
                            except:
                                print ('Error updating time in func_read_data_from_CSV', sys.exc_info()[0])
                            try:
                                self.message.append(row[3])
                            except:
                                print ('Error updating message in func_read_data_from_CSV', sys.exc_info()[0])
                            try:
                                self.url.append(row[4])
                            except:
                                print ('Error iterating through arrays in func_read_data_from_CSV', sys.exc_info()[0])
                    #self.count_messages=local_count_messages
                #except:
                #    print ('Error iterating through arrays in func_read_data_from_CSV', sys.exc_info()[0])
                    except ValueError as e:
                        print ('Value Error in iterating through csv ',e)
            except:
                print ('Error opening csv file in func_read_data_from_CSV', sys.exc_info()[0])
        except:
            print ('Error in func_read_data_from_CSV', sys.exc_info()[0])  
 
    def func_load_record_into_database(self,record_to_add):
        self.cursor.execute(self.add_message,record_to_add)
        self.connection.commit()
        #data_to_add_to_row = [discussion_id,title,commentTime,comment,url]
        
                    
    def func_load_csv_into_database(self):
        try:
            #check that csv file has been read
            if self.discussion_id:#checks that there is at least 1 record beyond the title row in the csv
                print("CSV File has been loaded previously")
            else:
                print("No date in data arrays.  Must load data from CSV File")
                print("loading data from CSV File")
                self.func_read_data_from_CSV()#load CSV data into arrays
        except:
            print ('Error loading data from CSV to support SQL queries', sys.exc_info()[0])  
        #Import records into database
        try:
            #connection = mysql.connector.connect(host='localhost', database='CIV_COMMENTS',user='root',password='14bAdm1n!')
        #connection = mysql.connector.connect(host='localhost', database='CIV_COMMENTS', user='root')
            #self.connect_to_db()
            #create_messages_table="CREATE TABLE MESSAGES (DISCUSSION_ID INT, TITLE VARCHAR(512), TIME DATETIME, MESSAGE TEXT, URL TEXT);"
            #add_message="INSERT INTO MESSAGES (DISCUSSION_ID, TITLE, TIME, MESSAGE, URL) VALUES (%s,%s,%s,%s,%s)"
            message_record_values=""
            db_Info = self.connection.get_server_info()
            print("Connected to MYSQL Server verion", db_Info)
            #cursor = self.connector.cursor()
            self.cursor.execute("select database();")
            record = self.cursor.fetchone()
            print("You're connected to the database: ", record)
            print("Importing data into MySQL database.  Please wait")
            #insert all records from csv into database
            print("writing messages from file into database...please wait....")
            for index, D in enumerate(self.discussion_id):
                #print(".")
                if (index != 0):#the zero index is the title, so starts with 1
                    message_record_values=(D,self.title[index],self.time[index],self.message[index],self.url[index])
                    self.cursor.execute(self.add_message,message_record_values)
            self.connection.commit()#commit the query to the database
        
        except Error as e:
            print ("Error while connecting to MySQL", e)
 
    def func_stats_from_csv_file(self):
        ##count unique discussions and messages
        try:
            #check that csv file has been read
            if self.discussion_id:#checks that there is at least 1 record beyond the title row in the csv
                print("CSV File has been loaded previously")
            else:
                print("No date in data arrays.  Must load data from CSV File")
                #print("loading data from CSV File")
                self.func_read_data_from_CSV()#load CSV data into arrays
        except:
            print ('Error loading data from CSV to support func_stats_from_csv', sys.exc_info()[0])  
            #Import records into database
        unique_ids=set(self.discussion_id)
        count_ids=0

        for item in unique_ids:
            count_ids+=1
    
        print('There are '+str(count_ids)+' unique discussions containing: '+str(self.count_messages)+' messages')

        ## Count the occurrence of key words in messages
        count_animation=0
        count_animation_cancel=0
        count_animation_cancel_blah=0
        count_weave=0
        count_block=0
        count_block_cancel=0
        count_cheat=0
        count_bug=0
        count_poll=0
        count_lag=0
        for item in self.message:
            if 'animation' in item:
                count_animation+=item.count('animation')
            if 'animation cancel' in item:
                count_animation_cancel+=item.count('animation cancel')
            if 'animation cancel blah' in item:
                count_animation_cancel_blah+=item.count('animation cancel blah')
            if 'weave' in item:
                count_weave+=item.count('weave')
            if 'block' in item:
                count_block+=item.count('block')
            if 'block cancel' in item:
                count_block_cancel+=item.count('block cancel')
            if 'cheat' in item:
                count_cheat+=item.count('cheat')
            if 'bug' in item:
                count_bug+=item.count('bug')
            if 'poll' in item:
                count_poll+=item.count('poll')
            if 'lag' in item:
                count_lag+=item.count('lag')
    
        
        print('The word animation occurred: '+str(count_animation)+' times')
        print('The word "animation cancel" occurred: '+str(count_animation_cancel)+' times')
        print('The word "animation cancel blah" occurred: '+str(count_animation_cancel_blah)+' times')
        print('The word "weave" occurred: '+str(count_weave)+' times')
        print('The word "block" occurred: '+str(count_block)+' times')
        print('The word "block cancel" occurred: '+str(count_block_cancel)+' times')
        print('The word "cheat" occurred: '+str(count_cheat)+' times')
        print('The word "bug" occurred: '+str(count_bug)+' times')
        print('The word "poll" occurred: '+str(count_poll)+' times')
        print('The word "lag" occurred: '+str(count_lag)+' times')

### main program section

#func_load_csv_into_database(csv_file_name)

    def func_extract_stats_from_mySQLdb(self,query):
        try:
            
            #db_Info = self.connection.get_server_info()
            #print("Connected to MYSQL Server version", db_Info)
            #self.cursor.execute("select database();")
            #record = self.cursor.fetchone()
            #print("You're connected to the database: ", record)
            #print("Querying database")
            #insert all records from csv into database
            self.cursor.execute(query)
            query_response=self.cursor.fetchone()
            
            #connection = mysql.connector.connect(host='localhost', database='CIV_COMMENTS',user='root',password='14bAdm1n!')
#             if connection.is_connected():
#                 db_Info = connection.get_server_info()
#                 print("Connected to MYSQL Server version", db_Info)
#                 cursor = connection.cursor()
#                 cursor.execute("select database();")
#                 record = cursor.fetchone()
#                 print("You're connected to the database: ", record)
#                 print("Querying database")
#                 #insert all records from csv into database
#                 cursor.execute(query)
#                 query_response=cursor.fetchone()

        
        except Error as e:
            print ("Error while connecting to MySQL", e)
        return query_response
    
    
#func_stats_from_csv_file()#gathers and prints stats from the data extracted

#construct list of queries and titles for the queries

    def func_run_select_queries(self):
        #self.connect_to_db()#connect to database
        response_tuple=''
        for index,query in enumerate(self.select_query_list):
            response_tuple=self.func_extract_stats_from_mySQLdb(query)
            print(self.select_query_description[index]+" : "+str(response_tuple[0]))
        #self.disconnect_db()#close database after finished with queries

    def func_load_default_select_queries(self):
        #get total number of messages
        self.select_query_description.append("Total number of messages")
        self.select_query_list.append("select count(*) from MESSAGES;")
        
        #get total number of distinct discussions
        self.select_query_description.append("Total number of discussion topics")
        self.select_query_list.append("select count(distinct title) from MESSAGES;")
        
        
        
        ##this section is misleading because messages can be updated.  This discussions into buckets based on when they died
        
        #get total number of distinct discussions in 2020
        self.select_query_description.append("Distinct discussions in 2020")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2020%';")
        
        #get total number of distinct discussions in 2019
        self.select_query_description.append("Distinct discussions in 2019")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2019%';")
        
        #get total number of distinct discussions in 2018
        self.select_query_description.append("Distinct discussions in 2018")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2018%';")
        
        #get total number of distinct discussions in 2017
        self.select_query_description.append("Distinct discussions in 2017")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2017%';")
        
        #get total number of distinct discussions in 2016
        self.select_query_description.append("Distinct discussions in 2016")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2016%';")
        
        #get total number of distinct discussions in 2015
        self.select_query_description.append("Distinct discussions in 2015")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2015%';")
        
        #get total number of distinct discussions in 2014
        self.select_query_description.append("Distinct discussions in 2014")
        self.select_query_list.append("select count(distinct title) from MESSAGES where time like '%2014%';")
 
        #get total number of messages in  in 2020
        self.select_query_description.append("Messages posted in 2020")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2020%';")
        
        #get total number of messages in  in 2019
        self.select_query_description.append("Messages posted in 2019")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2019%';")
        
        #get total number of messages in  in 2018
        self.select_query_description.append("Messages posted in 2018")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2018%';")
        
        #get total number of messages in  in 2017
        self.select_query_description.append("Messages posted in 2017")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2017%';")
        
        #get total number of messages in  in 2016
        self.select_query_description.append("Messages posted in 2016")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2016%';")
        
        #get total number of messages in  in 2015
        self.select_query_description.append("Messages posted in 2015")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2015%';")
        
        #get total number of messages in  in 2014
        self.select_query_description.append("Messages posted in 2014")
        self.select_query_list.append("select count(*) from MESSAGES where time like '%2014%';")
        
        
        
        
        
        
        
        
        
        
        
        
        #distinct discussions where civilization selection is in the title
        self.select_query_description.append("Distinct discussions with title about civilization selection ")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (title like '%civ%' or title like '%leader%' or title like '%country%');") 
        
        #distinct discussions where multiplayer aspects are in the title
        self.select_query_description.append("Distinct discussions with title about multiplayer gameplay")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (title like '%multiplayer%' or title like '%online%' or title like '%LAN%' or title like '%players%' or title like '%hotseat%');") 
        
        #distinct discussions about top performance
        self.select_query_description.append("Distinct discussions with title about being the best, top, or meta")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (title like '%meta%' or title like '%top%' or title like '%best%');") 
        
        #distinct discussions about fairness
        self.select_query_description.append("Distinct discussions with title about fairness containing nerf, buff, over powered, fair or balance")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (title like '%nerf%' or title like '%buf%' or title like '%over powered%' or title like'%fair%' or title like '%balance%');") 
        
        #distinct discussions about high skill game tasks
        self.select_query_description.append("Distinct discussions with title about very difficult game challenges such as immortal, deity, veteran, or achievement")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (title like '%immortal%' or title like '%deity%' or title like '%veteran%' or title like'%achievement%' or title like '%hard mode%');")  
        
        #query database for pvp in title
        self.select_query_description.append("Distinct discussions with 'PVP' in title ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where title like '%pvp%';") 
             
        #query database for pve in title
        self.select_query_description.append("Distinct discussions with 'PVE' in title ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where title like '%pve%';")       
        
        #query database for dps in title
        self.select_query_description.append("Dstinct discussions with 'dps' in title ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where title like '%dps%';")
        
        #query database for performance issues
        self.select_query_description.append("Distinct discussions with 'performance', 'lag', server, or maintenance in title ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where title like '%performance%' or title like '%lag' or title like '%server%' or title like '%maintenance%' or title like '%network%';")
        
        
        #query database for lag switching, rage quitting,. and stream sniping in title
        self.select_query_description.append("Distinct discussions with 'lag switch', 'rage quit', or 'stream snip' in title ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where title like '%lag switch%' or title like '%rage quit%' or title like '%stream snip%';")
        
        #distinct discussions where civilization selection is in the title
        self.select_query_description.append("Distinct discussions with messages about civilization selection ")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (message like '%civ%' or message like '%leader%' or message like '%country%');") 
        
        #distinct discussions where multiplayer aspects are in the title
        self.select_query_description.append("Distinct discussions with messages about multiplayer gameplay")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (message like '%multiplayer%' or message like '%online%' or message like '%LAN%' or message like '%players%' or message like '%hotseat%');") 
        
        #distinct discussions about top performance
        self.select_query_description.append("Distinct discussions with messages about being the best, top, or meta")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (message like '%meta%' or message like '%top%' or message like '%best%');") 
        
        #distinct discussions about fairness
        self.select_query_description.append("Distinct discussions with messages about fairness containing nerf, buff, over powered, fair or balance")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (message like '%nerf%' or message like '%buf%' or message like '%over powered%' or message like'%fair%' or message like '%balance%');") 
        
        #distinct discussions about high skill game tasks
        self.select_query_description.append("Distinct discussions with messages about very difficult game challenges such as immortal, deity, veteran, or achievement")
        self.select_query_list.append("select COUNT(distinct title) from MESSAGES where (message like '%immortal%' or message like '%deity%' or message like '%veteran%' or message like'%achievement%' or message like '%hard mode%');")  
        
        #query database for pvp in title
        self.select_query_description.append("Distinct discussions with 'PVP' in messages ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where message like '%pvp%';") 
             
        #query database for pve in title
        self.select_query_description.append("Distinct discussions with 'PVE' in messages ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where message like '%pve%';")       
        
        #query database for dps in title
        self.select_query_description.append("Distinct discussions with 'dps' in messages ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where message like '%dps%';")
        
        #query database for performance issues
        self.select_query_description.append("Distinct discussions with 'performance', 'lag', server, or maintenance in messages ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where message like '%performance%' or message like '%lag' or message like '%server%' or message like '%maintenance%' or message like '%network%';")
        
        
        #query database for lag switching, rage quitting,. and stream sniping in title
        self.select_query_description.append("Distinct discussions with 'lag switch', 'rage quit', or 'stream snip' in messages ")
        self.select_query_list.append("select count(distinct title) from MESSAGES where message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%';")
        
        
        
        
        
        
        
        
        
        
        
        #distinct discussions where civilization selection is in the title
        self.select_query_description.append("Total discussions with title about civilization selection ")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (title like '%civ%' or title like '%leader%' or title like '%country%');") 
        
        #distinct discussions where multiplayer aspects are in the title
        self.select_query_description.append("Total discussions with title about multiplayer gameplay")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (title like '%multiplayer%' or title like '%online%' or title like '%LAN%' or title like '%players%' or title like '%hotseat%');") 
        
        #distinct discussions about top performance
        self.select_query_description.append("Total discussions with title about being the best, top or meta")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (title like '%meta%' or title like '%top%' or title like '%best%');") 
        
        #distinct discussions about fairness
        self.select_query_description.append("Total discussions with title about fairness containing nerf, buff, over powered, fair or balance")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (title like '%nerf%' or title like '%buf%' or title like '%over powered%' or title like'%fair%' or title like '%balance%');") 
        
        #distinct discussions about high skill game tasks
        self.select_query_description.append("Total discussions with title about very difficult game challenges such as trials, veteran, or achievement")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (title like '%immortal%' or title like '%deity%' or title like '%veteran%' or title like'%achievement%' or title like '%hard mode%');")  
        
        #query database for pvp in title
        self.select_query_description.append("Total discussions with 'PVP' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where title like '%pvp%';") 
             
        #query database for pve in title
        self.select_query_description.append("Total discussions with 'PVE' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where title like '%pve%';")       
        
        #query database for dps in title
        self.select_query_description.append("Total discussions with 'dps' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where title like '%dps%';")
        
        #query database for performance issues
        self.select_query_description.append("Total discussions with 'performance', 'lag', 'server', or 'maintenance' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where title like '%performance%' or title like '%lag' or title like '%server%' or title like '%maintenance%' or title like '%network%';")
        
        
        #query database for lag switching, rage quitting,. and stream sniping in title
        self.select_query_description.append("Total discussions with 'lag switch','rage quit', or 'stream snip' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where title like '%lag switch%' or title like '%rage quit%' or title like '%stream snip%';")
        
        #distinct discussions where civilization selection is in the title
        self.select_query_description.append("Total messages about civilization selection ")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (message like '%civ%' or message like '%leader%' or message like '%country%');") 
        
        #distinct discussions where multiplayer aspects are in the title
        self.select_query_description.append("Total messages about multiplayer gameplay")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (message like '%multiplayer%' or message like '%online%' or message like '%LAN%' or message like '%players%' or message like '%hotseat%');") 
        
        #distinct discussions about top performance
        self.select_query_description.append("Total messages about being the best, top, or meta")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (message like '%meta%' or message like '%top%' or message like '%best%');") 
        
        #distinct discussions about fairness
        self.select_query_description.append("Total messages about fairness containing nerf, buff, over powered, fair or balance")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (message like '%nerf%' or message like '%buf%' or message like '%over powered%' or message like'%fair%' or message like '%balance%');") 
        
        #distinct discussions about high skill game tasks
        self.select_query_description.append("Total messages about very difficult game challenges such as trials, veteran, or achievement")
        self.select_query_list.append("select COUNT(*) from MESSAGES where (message like '%immortal%' or message like '%deity%' or message like '%veteran%' or message like'%achievement%' or message like '%hard mode%');")  
        
        #query database for pvp in title
        self.select_query_description.append("Total messages with 'PVP'")
        self.select_query_list.append("select count(*) from MESSAGES where message like '%pvp%';") 
             
        #query database for pve in title
        self.select_query_description.append("Total messages with 'PVE'")
        self.select_query_list.append("select count(*) from MESSAGES where message like '%pve%';")       
        
        #query database for dps in title
        self.select_query_description.append("Total messages with 'dps' in title ")
        self.select_query_list.append("select count(*) from MESSAGES where message like '%dps%';")
        
        #query database for performance issues
        self.select_query_description.append("Total messages with 'performance', 'lag', 'server', or 'maintenance'")
        self.select_query_list.append("select count(*) from MESSAGES where message like '%performance%' or message like '%lag' or message like '%server%' or message like '%maintenance%' or message like '%network%';")
        
        
        #query database for lag switching, rage quitting,. and stream sniping in title
        self.select_query_description.append("Total messages with 'lag switch', 'rage quit', or 'stream snip'")
        self.select_query_list.append("select count(*) from MESSAGES where message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%';")
        
        
        
        
        
        
        
        

        
        
        #Message title contain pvp and message contain animation cancel
        self.select_query_description.append("title contains pvp and message contains ['lag switch', 'rage quit' or 'stream snip']")
        self.select_query_list.append("select COUNT(message) from MESSAGES where title like '%pvp%' and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")
        
        #Message title contain animation cancel and message contain bug
        self.select_query_description.append("title contains 'lag switch', 'rage quit', or 'stream snip' and message contains 'bug' ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where (title like '%lag switch%' or title like '%rage quit%' or title like '%stream snip%') and message like '%bug%';")


        #Message title contain bestand message contain animation cancel
        self.select_query_description.append("title contains best, or meta and message contains ['lag switch', 'rage quit', or 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where (title like '%meta%' or title like '%top%' or title like '%best%') and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")
        
        
        #Message title contains meta and animation cancel or weave
        self.select_query_description.append("Message  contains meta, top or best and ['lag switch', 'rage quit', 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where (message like '%meta%' or message like '%top%' or message like '%best%') and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")

        #Message title contains pvp and animation cancel or weave
        self.select_query_description.append("Message  contains pvp and ['lag switch', 'rage quit', or 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where message like '%pvp%' and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")

        #Message title contains pve and animation cancel or weave
        self.select_query_description.append("Message  contains pve and ['lag switch', 'rage quit', or 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where message like '%pve%' and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")

        #Message title contains dps and animation cancel or weave
        self.select_query_description.append("Message  contains dps and ['lag switch', 'rage quit', or 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where message like '%dps%' and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")

        #Message title contains end game and animation cancel or weave
        self.select_query_description.append("Message  contains end game, veteran, hard mode, trial etc and ['lag switch', 'rage quit', or 'stream snip'] ")
        self.select_query_list.append("select COUNT(message) from MESSAGES where (message like '%immortal%' or message like '%deity%' or message like '%veteran%' or message like'%achievement%' or message like '%hard mode%') and (message like '%lag switch%' or message like '%rage quit%' or message like '%stream snip%');")
        
        
        
        
        
        
        
        
        
        
        
        
    def func_purge_database(self):
        print("Running func_purge_database")  
        try:
            connection = mysql.connector.connect(host=self.host, user=self.user, password=self.password)
        except:
            print ('Error connecting to database in purge_database', sys.exc_info()[0])   
        try:
            db_Info = self.connection.get_server_info()
            print("Connected to MYSQL Server verion", db_Info)
            #cursor = self.connector.cursor()
            cursor=connection.cursor(buffered=True)
            try:
                cursor.execute("use "+str(self.database_name)+";")
                cursor.execute("select database();")
                record = cursor.fetchall()
                print("You're connected to the database: ", record)
            except Error as e:
                print ('Error selecting the database ',e)
            
            print("Searching database for "+self.table_name+" ")
            #insert all records from csv into database
            ## Check to see if table already exists in database
            try:
                table_list=[]
                #table_select_string="SHOW TABLES FROM "+str(self.database_name)+" ;"
                table_select_string="SHOW TABLES FROM "+str(self.database_name)+";"
                cursor.execute(table_select_string)
                record=cursor.fetchall()
                for table in record:
                    table_list.append(table[0])#fetchall returns a list of tuples, only interested in 0 index of the tuples
                    
                if self.table_name in table_list:
                    print("Table ",self.table_name," already exists.  Dropping table")
                    cursor.execute('DROP TABLE '+str(self.table_name)+';')
                    connection.commit()
                    connection.close()
                    cursor.close() 
                else:
                    print("table ",self.table_name," not in database ",self.database_name,". no need to purge")
                     
            except Error as e:
                print('Problem searching through table results ',e)  
        except Error as e:
            print ("Error while connecting to MySQL", e)

    def test_database(self):
        try:
            print('testing connection string used by the create database routine')
            connection= mysql.connector.connect(host=self.host, user=self.user, password=self.password)
            db_info=connection.get_server_info()
            print('connected to database in create database function',db_info)
            db_cursor=connection.cursor(buffered=True)
            print('Printing the database before the "select database();" command')
            print(db_cursor.execute("SHOW DATABASES;"))
            
            print('Printing the database after the "select database();" command')
            db_cursor.execute("select database();")
            record = db_cursor.fetchone()
            print(record)
            print('Printing the database after the "use CIV_COMMENTS" command')
            db_cursor.execute("use "+str(self.database_name)+";")
            db_cursor.execute("select database();")
            record = db_cursor.fetchall()
            print(record)
        except Error as e:
            print('Something done brokeafied ',e)
  
def main():   
    parser= DB_Parser()

    #Note only necessary to load file into database once.  This assumes an empty messages table in the database
    #parser.func_load_csv_into_database()

    parser.func_load_default_select_queries()
    parser.func_run_select_queries()
    parser.connection.close()
    parser.cursor.close()
    print('Program complete')

if __name__ == "__main__":    
        main()


