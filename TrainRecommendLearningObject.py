from SimilarityMatrixClass import SimilarityMatrixClass as SM
import datetime as dt
from ProcessStudentData import ProcessStudentData as PS
import pdb
import os
from LearningObjectMapper import LearningObjectMapper as LOM
from TimeAwareRecommendation import TimeAwareRecommendation as TAR
from StudentData import StudentData as SD
import pandas as pd
class TrainRecommendLearningObject:
    """
    This class trains and recommend learning object on an increased time window basis and stores all the data in Data folder.
    """
    def __init__(self,inputDataFilePath,groupedFilePath=''):
        """
         The first time it cleans and groups the input data and stores the grouped data under Data folder. next time onwards the data is read from Data folder.
        """
        if(not os.path.exists(groupedFilePath)):
            self.student = PS(inputDataFilePath)
            self.student.FilterData()
            self.student.GroupData()
            self.student.WriteOutputToCSVFile(groupedFilePath)
        else:
            self.student = PS()
            self.student.ReadGroupedDataFromFile(groupedFilePath)      
        self.sTrain = SD()
        self.sTest = SD()
        #self.ps = PS()
    def TrainDataForAllWeek(self):
        """
        Trains the data based on increasing time window concept.
        """
        minTimeStamp = self.student.GetMinTimeStamp()
        minTimeStamp = minTimeStamp - dt.timedelta(days=(0+minTimeStamp.weekday()-7)%7)
        maxTimeStamp = self.student.GetMaxTimeStamp()
        maxTimeStamp = maxTimeStamp + dt.timedelta(days=7) 
        weekNumber = 0
        trainingEndTime = minTimeStamp
        timingsData = pd.DataFrame(columns=['WeekNumber','TimeTaken'])
        while(trainingEndTime<=maxTimeStamp):
            print('week '+str(weekNumber + 1 )) 
            startTime = dt.datetime.now()
            trainingEndTime = trainingEndTime + dt.timedelta(days=7)
            self.TrainDataForWeek(trainingEndTime,weekNumber)
            print('trained week '+str(weekNumber + 1)) 
            timingsData = timingsData.append({'WeekNumber':str(weekNumber+1),'TimeTaken':(dt.datetime.now()-startTime).total_seconds()}, ignore_index=True)
            weekNumber+=1
        timingsDirectory = 'Data/CalculationTimings'
        if not os.path.exists(timingsDirectory):
            os.makedirs(timingsDirectory)
        timingsData.to_csv(timingsDirectory+'/timings.csv')
    def TrainDataForWeek(self,trainingEndTime,weekNumber):
        """
        splits the data into two parts based on the timestamp and stores data in 
        Data/week{weekNumber}/trainingData.csv     ---- Training data
        Data/week{weekNumber}/testingData.csv      ---- Testing data
        Data/week{weekNumber}/mapped.csv           ---- This contains information about when a user has accessed a learning object. The dimension of the matrix data stored in this file is "Number of Students in the Training set" X "Number of Learning Objects in the Training set" 
        Data/week{weekNumber}/userAccess.csv       ---- This file contains infomration about how many times a student has accessed a particular learning object
        Data/week{weekNumber}/dictionary.pickle.csv ---- This file contains learning object to learning object similarity in a pickle format
        Data/week{weekNumber}/loMapDictionary.csv  ---- The leaning objects are mapped to keys like LO_0, LO_1, LO_3 and this maping is stored in this file
        """
        weekNumber = str(weekNumber+1)
        directory = 'Data/week'+weekNumber
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.student.TrainTestDataSplit(trainingEndTime)
        groupedFilePath = directory+'/trainingData.csv'
        testDataFilePath = directory+'/testingData.csv'
        loMappedFilePath = directory+'/mappedData.csv'
        userAccessFilePath = directory +'/userAccess.csv'
        picklePath = directory+'/dictionary.pickle.csv'
        mapDictPath =directory+'/loMapDictionary.csv'
        self.student.WriteTrainingDataSetToCSVFile(groupedFilePath)
        self.student.WriteUserAccessDataToFile(userAccessFilePath)
        self.student.WriteTestDataSetToCSVFile(testDataFilePath)
        self.student.trainingData = None
        self.student.testingData = None
        lo = LOM(groupedFilePath)
        print('LOM done')
        
        lo.MapLearningObjects(loMappedFilePath,mapDictPath)
        print('LOM Mapping done')
        lo=None
        sm = SM(loMappedFilePath,trainingEndTime,picklePath)
        print('Similarity Mapping done')
        sm=None

    def RecommendData(self,weekNumber,topRecommendations = 10):
        """
        This recommends learning object for the specified week number. The training and testing data for the week specified is one week before this week. For example, The recommendations for week 2 are calculated using trained data till week 1 end and testing data from week1 end. 
        The recommendations are stored under "Data/week{week number}/recommendations.csv"
        """
        
        week = str(weekNumber-1)
        directory =  'Data/week'+week+'/'
        groupedFilePath = directory+'trainingData.csv'
        testDataFilePath =  directory+'testingData.csv'
        loMappedFilePath =  directory+'mappedData.csv'
        picklePath =  directory+'dictionary.pickle.csv'
        userAccessDFilePath = directory+'userAccess.csv'
        if((not os.path.exists(userAccessDFilePath)) | (not os.path.exists(loMappedFilePath))):
            raise ValueError('Train the data for week '+week)
        self.sTrain.Data = pd.read_csv(groupedFilePath)
        self.sTest.Data = pd.read_csv(testDataFilePath)
        trainedUsers =  self.sTrain.GetUniqueStudents()
        testedUsers = self.sTest.GetUniqueStudents()
        allUsers = set(trainedUsers).union(testedUsers)
        if(not os.path.exists('Data/week'+str(weekNumber)+'/')):
            os.makedirs('Data/week'+str(weekNumber)+'/')
        tar = TAR(loMappedFilePath,picklePath,userAccessDFilePath)
        recommendations = pd.DataFrame(columns=['StudentId','Recommendations'])
        for user in trainedUsers:
            recommendation = tar.recommendation(user,topRecommendations)
            recommendations = recommendations.append({'StudentId':user,'Recommendations':list(recommendation.values())}, ignore_index=True)
        recommendations.to_csv('Data/week'+str(weekNumber)+'/recommendations.csv',index=False)