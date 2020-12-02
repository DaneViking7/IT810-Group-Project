import csv
import sys
import os
import pickle ## used to store queues efficiently

#  This program is intended to be imported as a module and controlled from other programs
#  This program performs operations associated with reading or writing data to disk
#  Several operations depend on the "web_file_operations" module to read or write data to disk
#


class web_file_operations(object):
    def __init__(self, outfile):
        self.outfile_name=outfile
        self.file_reader=type('csv',(object,),{})()
        self.url_downloaded_queue=set()
        self.url_need_to_download_queue=set()
        
        
    def func_parse_existing_file_for_already_downloaded_sites(self, url_field):
    #  This function will read a csv file and append the url field to a url_downloaded set
    #  By default the URL field should be set to 4
    #
        if (os.path.isfile('./'+self.outfile_name)):
            try:
                print('existing .csv file.  Parsing for downloaded links')
                with open(self.outfile_name, newline='', encoding='utf-8') as csvfile:
                    file_reader = csv.reader(csvfile)                
                    try:
                        for row in file_reader:
                            self.url_downloaded_queue.add(row[url_field])
                            #Store CSV fields into arrays
                            #local_count_messages+=1
                        return self.url_downloaded_queue
                    except:
                        print ('Error extracting urls from existing .csv', sys.exc_info()[0], sys.exc_info()[1])
            except:
                print ('Error opening existing .csv', sys.exc_info()[0], sys.exc_info()[1])
        else:
            print('No existing csv file found.  No updates to downloaded queue necessary')
    
    def append_list_as_row(self,file_name, method, list_of_elem):
        with open(file_name, method, newline='', encoding='utf-8') as csv_file:
        #f = csv.writer(csv_file, delimiter=';')
            f = csv.writer(csv_file)
        #f = csv.writer(open('civ_comments.csv','w', netline='', encoding='utf-8'))
            f.writerow(list_of_elem) ## creates header row for the File
    
    def func_check_for_csv(self):            
        if not (os.path.isfile('./'+self.outfile_name)):
            header_fields = ['discussion_id','title','time','message','url']
            self.append_list_as_row(self.outfile_name,'w', header_fields)
    
    def append_download_manifest(self,file_name,url):
        if not (os.path.isfile('./'+file_name)):
            with open(file_name, 'w') as download_file:
                download_file.write(url+'\n')
        else:
            with open(file_name, 'a') as download_file:
                download_file.write(url+'\n')
                
                
    def func_parse_download_file_for_sites(self, file_name):
    #  This function will read a csv file and append the url field to a url_downloaded set
    #  By default the URL field should be set to 4
    #
        if (os.path.isfile('./'+file_name)):
            try:
                print('existing ' , file_name, ' file.  Parsing for downloaded links')
                with open(file_name, newline='\n', encoding='utf-8') as downloadfile:               
                    try:
                        for row in downloadfile:
                            self.url_downloaded_queue.add(row)
                            #Store CSV fields into arrays
                            #local_count_messages+=1
                        return self.url_downloaded_queue
                    except:
                        print ('Error extracting urls from existing .csv', sys.exc_info()[0], sys.exc_info()[1])
            except:
                print ('Error opening existing download file', sys.exc_info()[0], sys.exc_info()[1])
        else:
            print('No existing download file found.  No updates to downloaded queue necessary')
            return self.url_downloaded_queue

    def store_need_to_download_queue_to_disk(self,need_to_download_queue, queue_file_name):
        with open(queue_file_name,"wb+") as queue_save_file:
            pickle.dump(need_to_download_queue,queue_save_file)
    
    def load_need_to_download_queue_from_disk(self,queue_file_name):
        if (os.path.isfile('./'+queue_file_name)):
            with open(queue_file_name,"rb") as queue_save_file:\
                need_to_download_queue=pickle.load(queue_save_file)
            return need_to_download_queue
        else:
            print('No need to download queue file to load')
            
            
            
            
