#-*-coding:UTF-8-*-
#-*-encoding=UTF-8-*-

from bs4 import BeautifulSoup
from time import sleep
import re
import requests
import traceback

# Global Variables

wait = 1
defaultHeaders = {'User-Agent': ''}
defaultSeperator = ','

sourceFilePath = './step-1-result.csv'
resultFilePathBase = 'chain-2-31-result'

print('Starting crawling more doctor links ...')

def getOfflineComments(ocLink):

    comments = []
    if ocLink=='':
        return comments

    stop   = False
    pageNo = 0


    while not stop:

        # next page
        pageNo += 1

        # REMOVE ME
        # if (pageNo > 1):
        #     stop = True
        #     break
        # END

        pageUrl = ocLink+'?p='+str(pageNo)
        try:
            res = requests.get(pageUrl, headers=defaultHeaders)
            soup = BeautifulSoup(res.text,'html.parser')
            commentTables = soup.select('[class=doctorjy]')

            # stop on empty comments
            if (len(commentTables) == 0):
                stop = True
                break

            # sleep(float(wait))                

            for st in commentTables:

                patientDetails = st.select('[class=dlemd]')[0].select('td')
                patientComment = st.select('[class=spacejy]')[0]
                clinicDetails  = st.select('table')[1].select('tr')[2].select('div')

                infoMap = {
                    '患者': '',
                    '时间': '',
                    '所患疾病': '',
                    '看病目的': '',
                    '治疗方式': '',
                    '疗效': '',
                    '态度': '',
                    '分享': '',
                    '该患者的其他分享': '',
                    '选择该医生就诊的理由': '',
                    '本次挂号途径': '',
                    '目前病情状态': '',
                    '本次看病费用总计': ''
                }

                for pd in patientDetails:
                    pattr = pd.text.strip().split('：')
                    if len(pattr)>1 :
                        infoMap[pattr[0]] = pattr[1]

                for cd in clinicDetails:
                    cattr = cd.text.strip().split('：')
                    if len(cattr)>1 :
                        infoMap[cattr[0]] = cattr[1]

                infoMap['分享'] = patientComment.text.strip().replace('\n',' ')

                comments.append(infoMap)

        except Exception as e:
            raise Exception(e)

    return comments


def crawling(rounds):

    with open(sourceFilePath) as sf:

        # skip header line
        next(sf)


        counter = 0

        resultFilePath = './'+resultFilePathBase+'-'+str(rounds)+'.csv'

        with open(resultFilePath, 'w') as rf:

            resultHeaders = [ '医生姓名','医院科室','医生ID','患者','时间','所患疾病',
                              '看病目的','治疗方式','疗效','态度',
                              '选择该医生就诊的理由','本次挂号途径','目前病情状态',
                              '本次看病费用总计','分享','该患者的其他分享',
                              '信息中心页','地区' ]

            rf.write(defaultSeperator.join(resultHeaders))

            for row in sf:

                # [n,dp,il,rg] ~= [医生姓名,医院科室,信息中心页,地区]
                [n,dp,il,rg] = row.strip().split(defaultSeperator)
                counter+=1
                print('No '+str(counter)+', Crawling more links of '+n+', '+il)

                try:
                    personalWeb = ''
                    doctorID = ''
                    offlineCommentLink = ''

                    # CRAWL OFFLINE COMMENT LINKS
                    jingyanLink = il.replace('.htm','/jingyan/1.htm')
                    offlineCommentLink = 'https:'+requests.get(jingyanLink, headers=defaultHeaders, allow_redirects=False).headers.get('Location')

                    offlineComments = getOfflineComments(offlineCommentLink)

                    # CRAWL PERSONAL WEBSITE LINKS
                    sleep(float(wait))
                    res = requests.get(il, headers=defaultHeaders)
                    personalWebMatch = re.search('<a class=blue href="(\/\/.+\.haodf\.com\/)', res.text)

                    # 已开通主页
                    if personalWebMatch:
                        personalWeb = 'https:'+personalWebMatch.group(1)
                            
                        # CRAWL DOCTOR ID
                        sleep(float(wait))
                        clinicLink = personalWeb+'clinic/selectclinicservice'
                        r = requests.get(clinicLink, headers=defaultHeaders, allow_redirects=False)
                        doctorID = re.search('host_user_id=(\d+)\&', r.headers.get('Location') ).group(1)

                    for oc in offlineComments:
                        rf.write('\n'+defaultSeperator.join([n,dp,doctorID,oc['患者'],oc['时间'],oc['所患疾病'],
                                                         oc['看病目的'],oc['治疗方式'],oc['疗效'],
                                                         oc['态度'],oc['选择该医生就诊的理由'],
                                                         oc['本次挂号途径'],oc['目前病情状态'],
                                                         oc['本次看病费用总计'],oc['分享'],oc['该患者的其他分享'],il,rg]))

                except Exception as e:
                    raise Exception(e)

for i in range(1,6):
    try:
        crawling(i)
    except Exception as e:
        traceback.print_exc()
        continue

print('Finished crawling more doctor links.\n')
