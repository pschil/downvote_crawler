'''
Created on Feb 26, 2015

@author: patrik
'''

import urllib2
import json
import time
import psycopg2
from multiprocessing.pool import Pool

#from simplejson.decoder import JSONDecoder


def split_into_chunks(l, n):
    if n < 1:
        n = 1
    return [l[i:i + n] for i in range(0, len(l), n)]

def get_post_details_from_api(rows):
    
    try:
        conn = psycopg2.connect("dbname='data' user='rstudio' host='localhost' password='rstudio'")
    except:
        print "I am unable to connect to the database"
        return
    conn.autocommit = True
    cur = conn.cursor()
    
    allIds = list()

    
    for r in rows:
        try:
            response = urllib2.urlopen("http://disqus.com/api/3.0/posts/listUsersVotedPost.json?post="+str(r[0])+"&thread="+str(r[1])+"&api_key=PUBLICAPIKEYCOMESHERE&vote=-1&limit=100")
            obj = json.loads(response.read())
        except: 
            continue;
        
        thread_l = list()

        
        #get object
        if(obj['code'] != 0):
            print "Error : " + str(obj['code'])
            continue
        else:
            for user in obj['response']:
                if(user['isAnonymous'] == True):
                    continue
                else:
                    thread_l.append( str(user['id']))
                    
        allIds.append( (r[0],  ", ".join( thread_l) ) ) 
       
        if(len(allIds) > 50): #save progress if crash!
            for i in allIds:
               # print "update crawler_post set downvoter_ids = \'" + str(i[1]) +  "\' , details_updated=true where disqus_id=" +str(i[0])+";"
                #cur.execute( "update crawler_post set downvoter_ids = \'" + str(i[1]) +  "\' , details_updated=true where disqus_id=" +str(i[0])+";" )
                cur.execute( "insert into tmp_table (disqus_id, downvoter_ids) values ("  +str(i[0])+  ", '" + str(i[1])+  "' )" ) # downvoter_ids = \'" + +  "\' , details_updated=true where disqus_id=" ++";" )
                
            allIds = list() #delete so won't be inserted again
            print "update 100"
            conn.commit()
            
    #always save at the end            
    for i in allIds:
        #cur.execute( "update crawler_post set downvoter_ids = \'" + str(i[1]) +  "\' , details_updated=true where disqus_id=" +str(i[0])+";" )
        cur.execute( "insert into tmp_table (disqus_id, downvoter_ids) values ("  +str(i[0])+  ", '" + str(i[1])+  "' )" ) 
    conn.commit()   
    
    return True
            
if __name__ == '__main__':
    print "test"
    pupKey = "PUBLICAPIKEYCOMESHERE"
    disqusURL = "https://disqus.com/api/3.0/posts/listUsersVotedPost.json?"
    
    
    try:
        conn = psycopg2.connect("dbname='data' user='rstudio' host='localhost' password='rstudio'")
    except:
        print "I am unable to connect to the database"
        exit()
    
    cur = conn.cursor()

    print "going to load"
    cur.execute("SELECT disqus_id, disqus_thread_id from crawler_post where details_updated=false") # and details_updated=true")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print "loaded from DB"
    chunks = split_into_chunks(rows, len(rows)/1000)
    
    threads=[]
    print time.time()

    pool = Pool(1000)
    results = pool.map(get_post_details_from_api, chunks)

    cur.close()
    conn.close()
    #flatten the results to one list [disqus_id, string]
  #  cur.execute( "update crawler_post set downvoter_ids = " + str(i[1]) +  "where disqus_id=" +str(i[0]) )
    
        
   # final = [ i for r in results for i in r]
    
   # cur.executemany("update crawler_post set downvoter_ids = %(final)s", final)
    #write to db
    
   # for c in chunks:
  #      async_result = pool.apply_async(get_post_details_from_api, (c,i))
    
    
#     i = 0
#     for c in chunks:
#         t = threading.Thread(target=get_post_details_from_api, args=(c,i))
#         threads.append(t)
#         t.start();
#         i=i+1
#      
 #   print "running threads: "+str(len(threads))
     
    #wait for finish
  #  for t in threads:
  #      t.join();
     
    print time.time()
    
    
#     for x in range(0, 100):
#         threadID = 2017896649
# #         postID = 1147940402
# #     
#     URL=
# #    
# #     
#     obj = json.loads(urllib2.urlopen(URL).read())
#     
#     
#     
#     if(obj['code'] != 0):
#        # continue;
#       print "Error : " + str(obj['code'])
#     else:
#         for user in obj['response']:
#             if(user['isAnonymous'] == True):
#                 continue
#             else:
#                 allIds.append( str(user['id']))
#                 
#                 
#     print allIds
            #print user['id']#obj['response'][1]['username']  # type(obj['response'][0]['username']).__name__
    
    
  #  data = JSONDecoder.decode(response.read())
 #   print data
  #  obj = json.loads(response.read())
    #print obj['response'][]