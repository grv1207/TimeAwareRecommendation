import pandas as pd
import datetime as dt
from collections import OrderedDict
import ast
from TrainRecommendLearningObject import TrainRecommendLearningObject as TS
from StudentData import StudentData as SD
import math
class AccuracyCalculator:
    """
    This class calculate all the metrics like MATD, MATD_Clean, RMSTD, RMSTD_Clean, Precision and recall and writes them to file.
    """
    def __init__(self,ts):
        self.ts = ts
        self.sd = SD() 
        self.minimumTime = self.ts.student.groupedData[self.sd.TimeStamp].min() 
        self.minimumTime = self.minimumTime.replace(hour=0,microsecond=0,minute=0,second=0)
        self.minimumTime = self.minimumTime - dt.timedelta(days=(0+self.minimumTime.weekday()-7)%7)
        self.ts = None
    def SetTestData(self,weekNumber):
        """
        This function returns recommendation data and testing data based on the specified week.
        based on the week number the testing data is read from Data folder. normally testing data for any week is testing data from the previous week.
        """
        
        week = str(weekNumber)
        loDictionary = pd.Series.from_csv('Data/week'+str(weekNumber-1)+'/loMapDictionary.csv',index_col=0)
        data = pd.read_csv('Data/week'+week+'/recommendations.csv')
        data['Recommendations'] = data['Recommendations'].apply(lambda x : ast.literal_eval(x)[0])
        #data.head()
        testingData = pd.read_csv('Data/week'+str(weekNumber-1)+'/testingData.csv')
        testingData[self.sd.TopicsKey] = testingData[self.sd.TopicsKey].apply(lambda x : loDictionary[x] if loDictionary.index.contains(x) else '')
        testingData[self.sd.TopicsKey] = testingData[testingData[self.sd.TopicsKey]!='']
        testingData[self.sd.TimeStamp] = pd.to_datetime(testingData[self.sd.TimeStamp],format=self.sd.DateTimeFormat)
        testingData.sort_values([self.sd.StudentKey,self.sd.TimeStamp],inplace=True)
        #testingData.head()
        return data,testingData,loDictionary

    def CalculateRecommendationAccuracy(self,weekNumber,numberOfRecommedations,numberOfWeeks,considerAllUsers=True,writeFile = True):
        """
        this calculates all the metrics using the data stored in the Data folder
        """
        days = 7*(weekNumber-1)
        recommendationTime = self.minimumTime + dt.timedelta(days)
        #recommendationEndTime = recommendationTime+dt.timedelta(days=numberOfWeeks*7)
        accesDataFrame = pd.DataFrame(columns= ['Student','MATD','MATD_Clean','RMSTD','RMSTD_Clean','Precision','Recall','AccessedDays'])
        data,testingData,loDictionary = self.SetTestData(weekNumber)
        allLearningObjects = loDictionary.values
        totalLeaningObjects = len(allLearningObjects)
        endDate = testingData[self.sd.TimeStamp].max()
        recommendations = numberOfRecommedations
        if(totalLeaningObjects < numberOfRecommedations):
            recommendations = totalLeaningObjects
        for index,row in data.iterrows():
            userRecords = testingData[testingData[self.sd.StudentKey]==row['StudentId']]
            #userRecords = userRecords[userRecords[self.sd.TopicsKey]!='']
            matdDict = OrderedDict()
            matd=0.0
            matdCleanDict = OrderedDict()
            matdClean=0.0
            rmstdDict = OrderedDict()
            rmstd = 0.0
            rmstdCleanDict = OrderedDict()
            rmstdClean = 0.0
            truePositives = 0
            falsePositives = 0
            trueNegatives = 0
            totalValidRecommendations = 0
            validUsers = 0
            invalidUsers = 0
            if(len(userRecords) > 0):
                accessedTopics = len(userRecords[self.sd.TopicsKey].unique())
                recCleanStartTime = userRecords[self.sd.TimeStamp].min()
                #userRecords = userRecords[userRecords[self.sd.TimeStamp] <=recommendationEndTime]
                validUsers+=1
                if(len(userRecords)>0):
                    for recommendation in row['Recommendations']:
                        #topicKey = loDictionary[recommendation] 
                        topics = userRecords[userRecords[self.sd.TopicsKey]==recommendation]
                        if(len(topics) > 0 & (not topics is None)):
                            record = userRecords.loc[topics[self.sd.TimeStamp].argmin()]
                            matdDict[recommendation] = (record[self.sd.TimeStamp] - recommendationTime).days
                            matdCleanDict[recommendation] = (record[self.sd.TimeStamp] - recCleanStartTime).days
                            rmstdDict[recommendation] = ((record[self.sd.TimeStamp] - recommendationTime).days)**2
                            rmstdCleanDict[recommendation] = ((record[self.sd.TimeStamp] - recCleanStartTime).days)**2
                            #matdDict[recommendation] = (record[self.sd.TimeStamp] - recommendationTime).total_seconds()
                            #matdCleanDict[recommendation] = (record[self.sd.TimeStamp] - recCleanStartTime).total_seconds()
                            #rmstdDict[recommendation] = ((record[self.sd.TimeStamp] - recommendationTime).total_seconds())**2
                            #rmstdCleanDict[recommendation] = ((record[self.sd.TimeStamp] - recCleanStartTime).total_seconds())**2
                            truePositives+=1
                        else :
                            matdDict[recommendation] = 0
                            matdCleanDict[recommendation] = 0
                            rmstdDict[recommendation] = 0
                            rmstdCleanDict[recommendation] = 0
                            falsePositives +=1
                        matd+=matdDict[recommendation]
                        matdClean+=matdCleanDict[recommendation]
                        rmstd+=rmstdDict[recommendation]
                        rmstdClean+=rmstdCleanDict[recommendation]
                    if(truePositives==0):
                        totalValidRecommendations = 1
                    else:
                        totalValidRecommendations = truePositives
    
                    accesDataFrame = accesDataFrame.append({'Student':row['StudentId'],'MATD':matd/(totalValidRecommendations*1.0),'MATD_Clean':matdClean/(totalValidRecommendations*1.0),'RMSTD':math.sqrt((float)(rmstd/(totalValidRecommendations*1.0))),'RMSTD_Clean':math.sqrt((float)(rmstdClean/(totalValidRecommendations*1.0))),'Precision':((truePositives)/((truePositives+falsePositives)*1.0)),'Recall':((truePositives)/(accessedTopics*1.0)),'AccessedDays':matdDict},ignore_index=True)
            else:
                invalidUsers+=1
                if(considerAllUsers):
                    accesDataFrame = accesDataFrame.append({'Student':row['StudentId'],'MATD':0.0,'MATD_Clean':0.0,'RMSTD':0.0,'RMSTD_Clean':0.0,'Precision':0.0,'Recall':0.0,'AccessedDays':0},ignore_index=True)
        if(writeFile):
            week = str(weekNumber)
            accesDataFrame.to_csv('Data/week'+week+'/userAccuracy.csv',index=False)
            
        return accesDataFrame