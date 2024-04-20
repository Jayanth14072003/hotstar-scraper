
import requests
import re
import sys
import os
import json
import csv
from requests.auth import HTTPDigestAuth
from collections import defaultdict
import paramiko


site = 'Hotstar'
feed_dir = '/home/justdial/data/output/'+site
#file = feed_dir+'/'+site+'.csv'
file = feed_dir+'/HT.csv'

output_file = {}
uid = ''

csv.register_dialect('myDialect', delimiter = '|')

headers = {'Host': 'api.hotstar.com',
'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
'Accept': '*/*',
'Accept-Language': 'en-US,en;q=0.5',
'Accept-Encoding': 'gzip, deflate, br',
'Referer': 'https://www.hotstar.com/list/new-on-hotstar/t-3741_25_2',
'x-variant-list': 'lb_gravity,tpfy_gravity',
'x-country-code': 'IN',
'x-platform-code': 'PCTV',
'x-client-code': 'LR',
'hotstarauth': 'st=1551173873~exp=1551179873~acl=/*~hmac=9aff79059fb79440b2c6919b2b07aa1b17eacf7157576e529c87ff29d3f57fa6',
'x-region-code': 'MH',
'Origin': 'https://www.hotstar.com',
'DNT': '1',
"authority": "api.hotstar.com",
"sec-fetch-site": "same-site",
           }

s = requests.Session()

def fetch():

    i = 1
    offset = 20

    while True:
        if i == 1 :
            url = 'https://hotstar-sin.gravityrd-services.com/grrec-hotstar-war/JSServlet4?rn=1&uid=c46cb5690d014f02aaa7756f9763e701&cid=169290db215-7acba0b83fd99df4&v=a3e29c2&ts=1551173762&grd=&eh=&rd=0,RECENTLY_ADDED,20,[*isContext:Home;*gacid:1743556529.1551171923;*_referrer:;*_location:https-5%2F%2Fwww.hotstar.com%2Flist%2Fnew-0on-0hotstar%2Ft-03741_25_2],[itemId]&r=1a7ca97d'
        else:
            url = 'https://hotstar-sin.gravityrd-services.com/grrec-hotstar-war/JSServlet4?rn=3&uid=c46cb5690d014f02aaa7756f9763e701&cid=169290db215-7acba0b83fd99df4&v=a3e29c2&ts=1551174281&grd=&eh=&rd=0,RECENTLY_ADDED,20,[*isContext:Home;*offset:'+str(offset)+';*gacid:1743556529.1551171923;*_referrer:;*_location:https-5%2F%2Fwww.hotstar.com%2Flist%2Fnew-0on-0hotstar%2Ft-03741_25_2],[itemId]&r=9f3b457e'

        req = s.get(url)
        #print req.text
        #sys.exit(0)

        cont = req.text

        item_id = re.findall(r"\{\"itemid\"\:\"(.*?)\",", cont)

        if not item_id:
            break
        else:
            str_ids = get_ids(item_id)

            second_url = 'https://api.hotstar.com/o/v1/multi/get/m/content?ids='+str_ids
            #print second_url
            

            req1 = s.get(second_url, headers=headers)
            #print req1.text
            #sys.exit(0)
            
            cont1 = req1.text
            urls = re.findall(r"\"uri\":\"(.*?)\"",cont1)
            #print urls
            #sys.exit()
            #print "\n\n----------------\n\n"

            for url in urls:
                req2 = s.get(url, headers=headers)
                #print req2.text
                final_cont = req2.text

                parse_page(final_cont)
                #sys.exit()
                #break



        print "%d\n%s" % (i,item_id)
        i += 1
        offset += 20
        #break



def get_ids(ids):
    string = ''
    for id in ids:
        if not string:
            string = id
        else:
            string = string+","+id
    return string



def fetch2():
    headersP = {'Host': 'api.hotstar.com',
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.hotstar.com/in/premium/list/new-on-hotstar-premium/t-6252',
            'x-variant-list': 'lb_gravity,tpfy_gravity',
            'x-country-code': 'IN',
            'x-platform-code': 'PCTV',
            'x-client-code': 'LR',
            'hotstarauth': 'st=1551173873~exp=1551179873~acl=/*~hmac=9aff79059fb79440b2c6919b2b07aa1b17eacf7157576e529c87ff29d3f57fa6',
            'x-region-code': 'MH',
            'Origin': 'https://www.hotstar.com',
            'DNT': '1',
            "authority": "api.hotstar.com",
            "sec-fetch-site": "same-site",
                }

    url = "https://api.hotstar.com/o/v1/tray/e/6252/detail?tao=0&tas=100"
    req = s.get(url,headers=headersP)
    cont1 = req.text
    #print cont1

    #premium_parse = json.loads(cont1)
    urls = re.findall(r"\"contentId\":\d+,\"uri\":\"(.*?)\"",cont1)
    #print urls
    
    for url in urls:
        req2 = s.get(url, headers=headers)
        #print req2.text
        final_cont = req2.text

        parse_page(final_cont)

    #exit()




def parse_page(json_string):

    parsed_json = json.loads(json_string)
    #print "\n~~~~~~~~~~~~~~~\n"
    content = json.dumps(parsed_json['body']['results']['item'])
    content = re.sub(r"\\\"",r"'", content)
    #print content

    title = re.search(r"\"title\":\s*\"(.*?)\"",content).group(1)    
    detail = re.search(r"\"detail\":\s*\"(.*?)\"",content).group(1) if '"detail"' in content else ''
    parentalRating = re.search(r"\"parentalRating\":\s*(\d+)",content).group(1) if '"parentalRating"' in content else ''
    parentalRatingName = re.search(r"\"parentalRatingName\":\s*\"(.*?)\"",content).group(1) if '"parentalRatingName"' in content else ''
    contentType = re.search(r"\"contentType\":\s*\"(.*?)\"",content).group(1) if '"contentType"' in content else ''
    duration = re.search(r"\"duration\":\s*(\d+),",content).group(1) if '"duration"' in content else ''
    description = re.search(r"\"description\":\s*\"(.*?)\"",content).group(1) if '"description"' in content else ''
    productionHouse = re.search(r"\"productionHouse\":\s*\"(.*?)\"",content).group(1) if '"productionHouse"' in content else ''
    premium = re.search(r"\"premium\":\s*(.*?),",content).group(1) if '"premium"' in content else ''
    genre = re.search(r"\"genre\":\s*\[(.*?)\]",content).group(1) if '"genre"' in content else ''
    studioName = re.search(r"\"studioName\":\s*\"(.*?)\"",content).group(1) if '"studioName"' in content else ''
    actors = re.search(r"\"actors\":\s*\[(.*?)\]",content).group(1) if '"actors"' in content else ''
    directors = re.search(r"\"directors\":\s*\[(.*?)\]",content).group(1) if '"directors"' in content else ''
    language = re.search(r"\"lang\":\s*\[(.*?)\]",content).group(1) if '"lang"' in content else ''
    year = re.search(r"\"year\":\s*(.*?),",content).group(1) if '"year"' in content else ''
    shortTitle = re.search(r"\"shortTitle\":\s*\"(.*?)\"",content).group(1) if '"shortTitle"' in content else ''
    channelName = re.search(r"\"channelName\":\s*\"(.*?)\"",content).group(1) if '"channelName"' in content else ''
    assetType = re.search(r"\"assetType\":\s*\"(.*?)\"",content).group(1) if '"assetType"' in content else ''
    contentId = re.search(r"\"contentId\":\s*\"?(\d+)\"?",content).group(1) if '"contentId"' in content else ''
    seasonCnt = re.search(r"\"seasonCnt\":\s*(\d+)",content).group(1) if '"seasonCnt"' in content else ''
    episodeCnt = re.search(r"\"episodeCnt\":\s*(\d+)",content).group(1) if '"episodeCnt"' in content else ''

    detail = re.sub(r'\\\u2019', r"'", detail)
    detail = re.sub(r'\\\u00a0', r" ", detail)
    detail = re.sub(r'\|', r" - ", detail)
    detail = re.sub(r'\\\n', r"", detail)
    detail = re.sub(r"^\"|\"$", r"", detail)

    images = re.search(r"\"images\":\s*\{(.*?)\},",content).group(1) if '"images"' in content else ''
    image_array = re.findall(r"\"(sources.*?)\"", images)
    for i in range(len(image_array)):
        sm = image_array[i].split("/")
        sm[-1] = str(int(sm[-1].split("-")[0])+1)+"-v"
        sm[-2] = str(int(sm[-2])+1)
        url_new = "/".join(sm)
        
        # CHANGE HAS TO MADE IN THIS LINK https://img1.hotstarext.com/image/upload/f_auto,t_web_hs_1x/

        image_array[i] = "https://img1.hotstarext.com/image/upload/f_auto,t_web_hs_1x/"+url_new
        
    image = " ~ ".join(image_array)
    #print image+"\n"

    if (not image):
        last2cont = re.search(r"(\d{2})$", contentId).group(1)
        image = "https://secure-media1.hotstarext.com/r1/thumbs/PCTV/"+last2cont+"/"+contentId+"/PCTV-"+contentId+"-hcdl.jpg"
    

    adContent = 'tv' if assetType == 'SHOW' else 'movies'
    toLower = shortTitle.lower() if shortTitle else title.lower()
    toLower = re.sub(r"\s+",r"-",toLower)
    toLower = re.sub(r":",r"",toLower)
    toLower = re.sub(r"\?",r"",toLower)

    description = re.sub(r"\"",r"'", description)
    genre = re.sub(r"\"", r"", genre)
    actors = re.sub(r"\"", r"", actors)
    directors = re.sub(r"\"", r"", directors)
    directors = '' if directors == 'NA' else directors
    language = re.sub(r"\"", r"", language)

    toTime = ''
    if duration:
        toMinute = int(duration)/60
        toHour = toMinute/60
        toMinute = toMinute%60

        toTime = str(toHour)+" Hour "+str(toMinute)+" Minutes" if not toHour == 0  else str(toMinute)+" Minutes"

    watchUrl = 'https://www.hotstar.com/'+adContent+'/'+toLower+'/'+contentId+'/watch' if adContent == 'movies' else 'https://www.hotstar.com/'+adContent+'/'+toLower+'/'+contentId


    premium = 'Flatrate' if premium == 'true' else 'Free'
    Certification = ''
    if (parentalRating == '1'):
        Certification = 'UA'
    elif (parentalRating == '3'):
        Certification = 'A'
    elif (parentalRating == '999'):
        Certification = 'PG'

    age_group = ''
    if (re.search(r'\d',parentalRatingName)):
        age_group = parentalRatingName
        parentalRatingName = ''
    

    # if contentId == '1260003484':
    #     print content

    uid = contentId
    #print "uid inside: "+uid
    output_file[uid] = {}
    output_file[uid]['Title'] = title
    output_file[uid]['Detail'] = detail
    output_file[uid]['Audio language'] = language
    output_file[uid]['Certification'] = parentalRatingName
    output_file[uid]['Age Group'] = age_group
    output_file[uid]['contentType'] = contentType.title()
    output_file[uid]['duration'] = toTime
    output_file[uid]['Description'] = description
    output_file[uid]['productionHouse'] = productionHouse
    output_file[uid]['Monetization Type'] = premium
    output_file[uid]['genre'] = genre
    output_file[uid]['studioName'] = studioName
    output_file[uid]['cast'] = actors
    output_file[uid]['Director'] = directors
    output_file[uid]['Release Year'] = year
    output_file[uid]['shortTitle'] = shortTitle
    output_file[uid]['channelName'] = channelName
    output_file[uid]['contentId'] = contentId
    output_file[uid]['Total Seasons'] = seasonCnt
    output_file[uid]['Total Episodes'] = episodeCnt
    output_file[uid]['URL'] = watchUrl
    output_file[uid]['Image'] = image
    output_file[uid]['Type'] = assetType.title()
    output_file[uid]['Provider Id'] = '122'
    output_file[uid]['Source'] = "HT"

    #print output_file
    # print(l)



fetch()
fetch2()

print "Complete!! Writing to OUTPUT FILE.."
with open(file, 'w') as csvFile:
    #fields = ['Title', 'Detail','language','parentalRating','contentType','assetType','duration','description','productionHouse','premium','genre','studioName','actors','directors','year','shortTitle','channelName','contentId','seasonCnt','episodeCnt','watchUrl']
    fields = ["Date created","URL","Title","Certification","Age Group","Type","Provider Id","Monetization Type","Price","Season No","Episode no","Episode name","Total Seasons","Total Episodes","Description","Audio language","Subtitles","IMDB Rating","Rotten tomatoes Ratings","Ratings","cast","Director","Crew","genre","duration","Image","Poster","Release date","Release Year","Quality","Source"]
    writer = csv.DictWriter(csvFile, fieldnames=fields, extrasaction='ignore', dialect="myDialect")
    writer.writeheader()
    for uid in output_file:
        writer.writerows([output_file[uid]])
        

print("Writing completed: " + file)
csvFile.close()    



def scp_file():
    remotehost = 'justdial@172.29.132.222'
    remotefile = '/home/justdial/Desktop/Online_Movies/'
    
    ssh_client =paramiko.SSHClient()
    ssh_client.load_host_keys(os.path.expanduser(os.path.join("~", ".ssh", "known_hosts")))
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname='172.29.132.222',username='justdial',password='justdial')
    ftp_client=ssh_client.open_sftp()
    ftp_client.put(file,remotefile+"/HT.csv")
    ftp_client.close()
    ssh_client.close()
    print ("FILE SCP Done!")

scp_file()
