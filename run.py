#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 03:57:50 2017

@author: rudresh
"""

from TrainRecommendLearningObject import TrainRecommendLearningObject as TS
from StudentData import StudentData as SD
from AccuracyCalculator import AccuracyCalculator as AC 
from ProcessStudentData import ProcessStudentData as PS
import datetime as dt
import pandas as pd
import sys
import os
ts = TS('Data/AWT_statements.json','Data/dfStudentData.csv')
def TrainData():
    """
    This method calls “Train Data For All Week” method in “Train Recommend Learning Object” class.
    """      
    ts.TrainDataForAllWeek()
def RecommendData(noOfRecommendations,startWeek,endWeek):
    """
    This method calls “Recommen Data” method in “Train Recommend Learning Object” class
    """
    startTime = dt.datetime.now()
    timingsData = pd.DataFrame(columns=['WeekNumber','TimeTaken'])
    for i in range(startWeek,endWeek):
        
        ts.RecommendData(i,noOfRecommendations)
        timingsData = timingsData.append({'WeekNumber':str(i),'TimeTaken':(dt.datetime.now()-startTime).total_seconds()}, ignore_index=True)
    
    timingsDirectory = 'Data/CalculationTimings/Top'+str(noOfRecommendations)
    if not os.path.exists(timingsDirectory):
        os.makedirs(timingsDirectory)
    timingsData.to_csv(timingsDirectory+'/recommendation_timings.csv')

def AccuracyCalculator(noOfRecommendations,startWeek,endWeek):
    """
    This method calculates all the metrics
    """
    ac = AC(ts)
    ps = PS()
    train = SD()
    test = SD()
    AverageDaysToConsumption = pd.DataFrame(columns=['WeekNumber','TrainedStudents','TrainedTopics','TestedStudents','TestedTopics','MATD','MATD_Clean','RMSTD','RMSTD_Clean','Precision','Recall','TotalUsers'])
    for i in range(startWeek,endWeek):
        weekNumber = str(i)
        train.Data = pd.read_csv('Data/week'+str(i-1)+'/trainingData.csv')
        test.Data = pd.read_csv('Data/week'+str(i-1)+'/testingData.csv')
        accuracyData = ac.CalculateRecommendationAccuracy(i,noOfRecommendations,0,False,True)
        AverageDaysToConsumption = AverageDaysToConsumption.append({'WeekNumber':str(i),'TrainedStudents':str(len(train.GetUniqueStudents())),'TrainedTopics':str(len(train.GetUniqueTopics())),'TestedStudents':str(len(test.GetUniqueStudents())),'TestedTopics':str(len(test.GetUniqueTopics())),'MATD':str(accuracyData['MATD'].mean()),'MATD_Clean':str(accuracyData['MATD_Clean'].mean()),'RMSTD':str(accuracyData['RMSTD'].mean()),'RMSTD_Clean':str(accuracyData['RMSTD_Clean'].mean()),'Precision':str(accuracyData['Precision'].mean()),'Recall':str(accuracyData['Recall'].mean()),'TotalUsers':str(len(accuracyData))},ignore_index=True)
    print('-------------------------------------')
    print(AverageDaysToConsumption)
    import os
    directory = 'Data/Accuracy/'+str(noOfRecommendations)
    if not os.path.exists(directory):
        os.makedirs(directory)
    AverageDaysToConsumption.to_csv(directory+'/accuracy.csv')
if __name__=="__main__":
    
    if(sys.argv[1].lower()=='true'):
        print('Training Started ')
        TrainData()
        print('Training Ended ')
        print('_____________________________________________________________________________________')

    
    print('Recommendation Started ')
    
    RecommendData((int)(sys.argv[2]),(int)(sys.argv[3]),(int)(sys.argv[4]))
    
    print('Recommendation Ended ')
    print('_____________________________________________________________________________________')

    print('Accuracy calculation Started ')
    AccuracyCalculator((int)(sys.argv[2]),(int)(sys.argv[3]),(int)(sys.argv[4]))
    
    print('Accuracy calculation Ended ')
    print('_____________________________________________________________________________________')