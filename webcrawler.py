import web_file_operations
import database_operations
import sys
import string
import requests
from bs4 import BeautifulSoup
import re

#  This is program is intended to be imported as a module and controlled from another "main" program
#  This program performs operations associated with downloading and parsing webpage information
#  Several operations depend on the "web_file_operations" module to read or write data to disk
#


class WebCrawler(object):
        def __init__(self, url_seed, url_seed_minus_language_tag, discussion_seed, regex_urls_to_ignore, url_downloaded_queue, need_to_download_queue, outfile, download_file_manifest_name,save_file_for_need_to_download_queue):
                self.url_downloaded_queue=url_downloaded_queue
                self.need_to_download_queue=need_to_download_queue
                self.relative_url=''
                self.times_func_download_page_ran=0
                self.outfile_name=outfile
                self.file_handler=web_file_operations.web_file_operations(outfile)
                self.url_discussion_seed=discussion_seed
                self.url_seed_minus_language_tag=url_seed_minus_language_tag
                self.regex_urls_to_ignore=regex_urls_to_ignore
                self.url_seed=url_seed
                self.download_file_manifest_name=download_file_manifest_name
                self.save_file_for_need_to_download_queue=save_file_for_need_to_download_queue
                self.db_interface=database_operations.DB_Parser()
  
        
        def remove_html_tags(self,string_to_clean):
            try:
            #tags = re.compile('(b\')|((\<script.*?\>).*?(\<\/script\>))|((\<style.*?\>).*?(\<\/style\>))|(\<.*?\>)|(\<.*?\/\>)|(\<\/.*?\>)|(&\w+;)|(html)|(\\\\n)|(\\\\x\w\w)') #works at removing style tags
                tags = re.compile('(\\n)|(b\')|((\<script.*?\>).*?(\<\/script\>))|((\<style.*?\>).*?(\<\/style\>))|(\<.*?\>)|(\<.*?\/\>)|(\<\/.*?\>)|(&\w+;)|(html)|(\\\\n)|(\\\\x\w\w)') #works at removing style tags
                tr = str.maketrans(" ", " ", string.punctuation)#used to strip punctuation
                try:
                    line = tags.sub(' ',str(string_to_clean)) #remove html tags
                except:
                    print ('Error removing html tags', sys.exc_info()[0])
                try:
                    #line= (line.lower().translate(tr).split())#convert line to lower case, remove punctionation and tokenize
                    line= (line.lower().translate(tr))#convert line to lower case, remove punctionation and tokenize
                except:
                    print ('Error Changing case, removing punctuation and spliting', sys.exc_info()[0])                           
                return line
            except:
                print ('Error in tokenizer function', sys.exc_info()[0])
          
        #downloads a page, extracts message and time data, updates .csv file
        def func_download_page(self,url):
            self.times_func_download_page_ran+=1
            
            self.relative_url=url[:url.rfind('/')]
            #print('relative_url= '+self.relative_url+'')
            self.file_handler.func_check_for_csv()
            
            #url='https://forums.elderscrollsonline.com/en/discussion/536250/complete-sorcerer-rework'
            headers= {'User-Agent': 'Mozilla/5.0'}
            # Collect and parse first page
            discussion_id='0'
            try:
                page = requests.get(url,headers=headers)
                #self.url_downloaded_queue.append(url)
                if url not in self.url_downloaded_queue:
                    self.file_handler.append_download_manifest(self.download_file_manifest_name, url)
                    self.url_downloaded_queue.add(url)#using a set now so necessary
                
                print('items left to download: '+str(len(self.need_to_download_queue))+'')
                print('items already downloaded: '+str(len(self.url_downloaded_queue))+'')
                print('Times the page download function has run: '+str(self.times_func_download_page_ran)+'')
                page_text=page.text
                try:
                    if url in self.need_to_download_queue:
                        self.need_to_download_queue.remove(url)#remove the url just downloaded from the list
                except:
                    print ('Cannot remove link from list', sys.exc_info()[0], sys.exc_info()[1])
            except:
                print ('Error updating url downloaded queue', sys.exc_info()[0], sys.exc_info()[1])
            
            #store all messages in a list
            # check that the page  page first
            try:
                if self.url_discussion_seed in url:
                    try:
                        print()
                        print (url+" is a discussion page")
                        print('Parsing page for discussion information')
                        print()                        
                        soup = BeautifulSoup(page.text, 'html.parser')
                        messages =soup.find_all(class_='messageInfo primaryContent')
                    except:
                        print ('Error no message section exists on page', sys.exc_info()[0], sys.exc_info()[1])
                        return page
                    try:
                        #msgTime = soup.find_all('time')
                        parse_message_time= re.compile('(?P<title>\w+ \w+, \w+ at \w+\:\w+ \w+)')
                        if parse_message_time.findall(page_text):
                            #msgTime=parse_message_time.search(page_text).group('datetime')
                            msgTime=parse_message_time.findall(page_text)
                        
                    except:
                        print ('Error no time data exists on page', sys.exc_info()[0], sys.exc_info()[1])
                    try:
                        title = soup.find_all('title')[0].text
                    except:
                        print ('Error no title data exists on page', sys.exc_info()[0], sys.exc_info()[1])
                    try:
                        #old code
                        #parse_discussion_id= re.compile('id=\"Discussion_(\d+)\"')##extract discussion ID from url
                        #if parse_discussion_id.search(page_text):
                        #    discussion_id=parse_discussion_id.search(page_text).group(1)
                        #
                        #new code
                        parse_discussion_id=re.compile('.*?\/threads\/.*?\.(\d+)')
                        if parse_discussion_id.search(page.url):
                            discussion_id=parse_discussion_id.search(page.url).group(1)
                        
                    except:
                        print('Error extracting discussion id',sys.exc_info()[0], sys.exc_info()[1])
                        
                    #loop through the messages and time lists and write them to the csv
                    for index, message in enumerate(messages):
                        remtab = re.sub('\t', '', str(message))
                        remline = re.sub('\n', '', str(remtab))
                        comment=self.remove_html_tags(remline)
                        commentTime=msgTime[index]#using regex match
                        #commentTime=remove_html_tags(msgTime[index].text)#old approach using the beautiful soup methods
                        data_to_add_to_row = [discussion_id,title,commentTime,comment,url]
                        #print('adding following data \n ', data_to_add_to_row)
                        self.file_handler.append_list_as_row(self.outfile_name,'a', data_to_add_to_row)
                        
                        #the following line updates the sql database on the fly
                        self.db_interface.func_load_record_into_database(data_to_add_to_row)
                    return page  #used to harvest hyperlinks etc in other methods
                else:
                    return page #return the page to be parsed for more urls, but don't waste time on beautiful soup or writing to file    
            except:
                print ('Error identifying discussion page', sys.exc_info()[0], sys.exc_info()[1])
                return page
            
       
        def func_bs4_find_urls_on_page(self,page):

            try:
                soup = BeautifulSoup(page.text, 'html.parser')
                #list_of_urls_on_page=[]
                set_of_urls_on_page=set()#using set to avoid duplicates
                
                #remove problematic tags from page
                if soup.script:
                    soup.script.decompose()
                if soup.find_all('div', class_='MeBox MeBox-SignIn Inline'):
                    for remove in soup.find_all('div', class_='MeBox MeBox-SignIn Inline'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('div', class_='SignInLinks'):
                    for remove in soup.find_all('div', class_='SignInLinks'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()        
                if soup.find_all('a', class_='SignInPopup'):
                    for remove in soup.find_all('a', class_='SignInPopup'):
                        #print('Removing SignInPopup from page')
                        remove.decompose()
                if soup.find_all('a', class_='ApplyButton'):
                    for remove in soup.find_all('a', class_='ApplyButton'):
                        #print('Removing ApplyButton from page')
                        remove.decompose()     
                if soup.find_all('a', class_='UserLink BlockTitle is-staff'):
                    for remove in soup.find_all('a', class_='UserLink BlockTitle is-staff'):
                        #print('Removing UserLink BlockTitle is-staff from page')
                        remove.decompose() 
                if soup.find_all('a', class_='UserLink BlockTitle'):
                    for remove in soup.find_all('a', class_='UserLink BlockTitle'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='ReactButton Button Quote'):
                    for remove in soup.find_all('a', class_='ReactButton Button Quote'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='Username'):
                    for remove in soup.find_all('a', class_='Username'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='Permalink'):
                    for remove in soup.find_all('a', class_='Permalink'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='PhotoWrap'):
                    for remove in soup.find_all('a', class_='PhotoWrap'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='Popup'):
                    for remove in soup.find_all('a', class_='Popup'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='btn-joinus'):
                    for remove in soup.find_all('a', class_='btn-joinus'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                if soup.find_all('a', class_='Button'):
                    for remove in soup.find_all('a', class_='Button'):
                        #print('Removing UserLink BlockTitle from page')
                        remove.decompose()
                 
                    
                for link in soup.find_all('a'):
                    href_parse=link.get('href')
                    #if not regex_urls_to_ignore.match(href_parse):#used to remove ignore url prefixes
                        #print ('not in ignore url.  regex match '+str(regex_urls_to_ignore.match(href_parse)))
                    if href_parse ==None:
                        continue
                    if href_parse =='#':
                        #print('Just #: ignoring')
                        continue 
                    if href_parse=='/':
                        #print('Just /:.  reverting to url seed')
                        href_parse=self.url_seed
                        continue                   
                    if self.regex_urls_to_ignore.match(href_parse) or self.regex_urls_to_ignore.match('https://forums.civfanatics.com/' + href_parse):
                        #print('ignore link matched.  ignoring: '+str(href_parse))
                        continue
                    else:
                        #if href_parse not in self.url_downloaded_queue:
                        if 'http' not in href_parse:
                            href_parse = 'https://forums.civfanatics.com/' + href_parse
                        if href_parse in self.url_downloaded_queue:
                            #print('Already downloaded : '+str(href_parse))
                            continue
                        else:
                            #if href_parse not in self.need_to_download_queue:
                            if href_parse in self.need_to_download_queue:
                                #print('Already planning to download: '+str(href_parse))
                                continue
                            else:
                                #list_of_urls_on_page.append(href_parse)
                                set_of_urls_on_page.add(href_parse)

            except:
                print('Error extracting URLs and storing in list',sys.exc_info()[0], sys.exc_info()[1])
            
            #parse list of URLs on page and compare with download queues
            
            try:
                #for page_link in list_of_urls_on_page:
                if set_of_urls_on_page:#check that the set is not empty
                    for page_link in set_of_urls_on_page:
                        #print('parsing url: '+ str(page_link)+'')
                        if 'https://' in page_link: #check to see if the link is an absolute link
                            try:
                                if self.url_seed in page_link:#used to limit search to same website
                                    if page_link not in self.need_to_download_queue:
                                        self.need_to_download_queue.append(str(page_link))
                                #self.need_to_download_queue.add(str(page_link))#using set instead of list
                                else:
                                    continue
                            except:
                                print('Error parsing https:// '+page_link+" ",sys.exc_info()[0], sys.exc_info()[1])
                        elif 'http://' in page_link: #check to see if the link is an absolute link
                            try:
                                if self.url_seed in page_link:#used to limit search to same website
                                    if page_link not in self.need_to_download_queue:
                                        self.need_to_download_queue.append(str(page_link))  #using list
                                #self.need_to_download_queue.add(str(page_link))  #using set instead of list
                                else:
                                    continue
                            except:
                                print('Error parsing http://',sys.exc_info()[0], sys.exc_info()[1])                                
                        elif '/en/' in page_link:
                            try:
                                page_link=str(self.url_seed_minus_language_tag+page_link)                
                                if self.url_seed in page_link:#used to limit search to same website
                                    if page_link not in self.need_to_download_queue:
                                        self.need_to_download_queue.append(str(page_link)) #using list
                                #self.need_to_download_queue.add(str(page_link))#using set instead of list
                                else:
                                    continue
                            except:
                                print('Error parsing /en/ links',sys.exc_info()[0], sys.exc_info()[1])
                        elif page_link =='/':
                            try:
                                #print('link is just /.  skipping')                
                                continue
                            except:
                                print('Error with sole / link'+ page_link,sys.exc_info()[0], sys.exc_info()[1])
                        elif page_link =='#':
                            try:
                                #print('link is just #.  skipping')                
                                continue
                            except:
                                print('Error with sole / link ' + page_link,sys.exc_info()[0], sys.exc_info()[1])
                        else:
                            try:
                                if page_link not in self.need_to_download_queue:
                                    print ("no defined parsing case for "+page_link ) #print word  
                                    print("adding "+page_link+"to queue")
                                    self.need_to_download_queue.append(page_link)#using list instead of set
                                    
                                #self.need_to_download_queue.add(page_link)#using set instead of list
                            except:
                                print('Errorwith undefined parsing case',sys.exc_info()[0], sys.exc_info()[1])
                
                
                    try:
                        self.file_handler.store_need_to_download_queue_to_disk(self.need_to_download_queue, self.save_file_for_need_to_download_queue)
                    except:
                        print('Error pickling the need to download queue')                          
            except:
                print('Error parsing url list and updating download queues',sys.exc_info()[0], sys.exc_info()[1])
                
                
                
                
