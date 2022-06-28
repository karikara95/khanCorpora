from configparser import ConfigParser
import json
from bs4 import BeautifulSoup
import requests as rqs
from pymongo import MongoClient, errors
import errno,sys
from termcolor import colored
from bs2json import bs2json
import urllib.parse as urlparse
from urllib.parse import parse_qs

config_object = ConfigParser()
config_object.read("khanCorpora_config.ini")

def mongo_connection():
    mongos = config_object["mongoDB"]
    
    connection_string = %mongodb connection string%

    client = MongoClient(connection_string, serverSelectionTimeoutMS=2000)
    client.server_info()  # raises ServerSelectionTimeoutError

    db_name = mongos["db_name"]
    if db_name not in client.list_database_names():
        print("The %s database does not exist" % (db_name))
        raise errors.ServerSelectionTimeoutError
    db = client[db_name]

    col_name=mongos["col_name"]
    if col_name not in db.list_collection_names():
        print("The %s collection does not exist" % (col_name))
        raise errors.ServerSelectionTimeoutError
    col = db[col_name]

    print(colored('Connection established. '+ col.name,"green"))

    return client,col

def get_ENids(homepage):

    n=0
    converter = bs2json()

    k=0
    Enids=[]

    while n<250:#TODO depth check
        n+=1
        param={'page':n}

        print(colored(' '.join([homepage,str(param),str(header)]),'green'))

        r = rqs.get(homepage,headers=header,params=param) #02-3701-1114

        soup = BeautifulSoup(r.content,features="lxml")

        lNsoup=soup.find_all('div', class_=['latestNews_list',"latestNews_list first"])

        for lNlist in lNsoup: #5 articles bunch
            # print("==========================================================")

            dl_data = lNlist.find_all("dl")
            for dlitem in dl_data:#article
                k+=1
                dtjson = converter.convert(dlitem).get('dl')


                try:
                    aid=parse_qs(urlparse.urlparse(dtjson.get('dt').get('a').get('attributes').get('href')).query)['artid'][0]
                except:
                    continue
                Enids.append(aid) #not limited to integers

    return Enids

def get_EN_articles(ENids):

    ENpage="https://english.{}".format(pages["main"] + '/' + pages["art_postfix"])
    linker=pages["originKO_icon"]

    ENarts=[]
    # ENids=ENids[:2] # get rid of the limit#TEST
    # ENids.append("20110609164806A") #TEST purpose - no linkage
    # ENids.append("202101121729537") #TEST purpose - OK

    print(ENids)

    for ENid in ENids:
        converter = bs2json()
        param={'artid':ENid}

        # print("\n"+colored(' '.join([ENpage, str(param), str(header)]), 'yellow'))

        r = rqs.get(ENpage, headers=header, params=param)  # 02-3701-1114
        soup = BeautifulSoup(r.content, features="lxml")
        try:
            titlesoup = soup.find_all('div', class_='article_title')
            ENtitle=converter.convert(titlesoup[0].find_all('dt')[0]).get('dt').get('text')

            textsoup = soup.find_all('div', class_='article_txt')
            ENtext=textsoup[0].text #TODO save the newlines
            #TODO + make a checksum by paragraph
        except:
            continue

        try:
            original = textsoup[0].find_all('a')[-1]
            KOid = parse_qs(urlparse.urlparse(original.attrs.get('href')).query)['artid'][0]
            # print(KOid)
        except IndexError:
            print(colored('Next EN article does not have a linkage to the KO source, passing:'+(ENid), 'red'))
            print(r.url)
            continue

        try:
            if original.contents[0].attrs.get('src')!=linker:
                print(colored('Next EN article has a wrong linker to the source, passing:'+(ENid), 'red'))
                continue
        except:
            print(colored('Next EN article does not have linker to the source, passing:' + (ENid), 'red'))
            continue

        try:
            KOtitle, KOtext=get_KO_article(KOid)
        except:
            print(colored('Smth wrong with the KO article, passing:' + (KOid), 'red'))
            print("https://news.{}".format(pages["main"] + '/kh_news/' + pages["art_postfix"])+"?artid="+str(KOid))
            continue

        art={"ENid":ENid,"KOid":KOid,
             "ENtitle": ENtitle, "KOtitle":KOtitle,
             "ENtext":ENtext, "KOtext":KOtext} #TODO change the schema
        # print(art)
        ENarts.append(art)

    return ENarts

def get_KO_article(KOid):
    converter=bs2json()
    KOpage="https://news.{}".format(pages["main"] + '/kh_news/' + pages["art_postfix"])
    param = {'artid': KOid}

    # print(colored(' '.join([KOpage, str(param), str(header)]), 'yellow'))

    r = rqs.get(KOpage, headers=header, params=param)  # 02-3701-1114
    soup = BeautifulSoup(r.content, features="lxml")

    titlesoup = soup.find_all('h1', id='article_title')
    KOtitle = converter.convert(titlesoup[0]).get('h1').get('text')
    # print(KOtitle)

    textsoup = soup.find_all('div', class_='art_body')
    KObunch = textsoup[0].find_all('p',class_="content_text",recursive=False)
    KOtext=[]
    for textblock in KObunch:
        if textblock.findAll(class_="strapline"):
            continue
        else:
            # print(textblock.text)
            KOtext.append(textblock.text)


    return KOtitle, KOtext

def main():
    global pages,header

    pages=config_object["URLs"]
    header={'User-Agent':pages["user-agent"]}
    try:
        client, col= mongo_connection()
    except errors.ServerSelectionTimeoutError:
        print("Invalid MongoDB connection.")
    finally:
        try: client.close()
        except: exit(errno.EINVAL)


    ENids = get_ENids("https://english.{}".format(pages["main"] + '/' + pages["EN_postfix"]))

    ENarticles=get_EN_articles(ENids)

    col.remove() # remove is deprecated

    col.insert_many(ENarticles)

    print(colored('Uploaded articles: ' + str(len(ENarticles))+'/'+str(len(ENids)), "green"))

    try:
        client.close()
    except:
        exit(errno.EINVAL)

if __name__ == "__main__":
    main()