import pandas as pd
import datetime as dt
from StudentData import StudentData as sd
import numpy as np
import os
class ProcessStudentData:
    def __init__(self,inputFilePath=''):
        """
        processes user click data using object of StudentData classs 
        """
        self.student = sd(inputFilePath)
        self.uniqueVerbs = []
        self.learningObjects = []
        self.groupedData = pd.DataFrame()
        self.trainingData = pd.DataFrame()
        if(inputFilePath!=''):
            self.SetUniqueVerbs(['initialized','answered'])
            self.students = self.student.GetUniqueStudents()
        self.groupbyKeys = [self.student.TopicsKey,self.student.StudentKey,self.student.VerbKey,self.student.TimeStamp]
        self.LOTypes = [self.student.ModuleType,self.student.LessonType]
        self.groupbyIndexes= [self.student.TopicsKey,self.student.StudentKey]
    def SetUniqueVerbs(self,keepVerbs):
        """  
        http://adlnet.gov/expapi/verbs/initialized
        http://adlnet.gov/expapi/verbs/answered
        
        filter data based on abve two status. We can add more in the constructor if we need to consider other status also
        """
        self.uniqueVerbs = self.student.GetUniqueVerbs()
        self.uniqueVerbs = self.uniqueVerbs[self.uniqueVerbs.str.contains('|'.join(keepVerbs))]
        
    def SetUniqueLearningObjects(self):
        """
        get list of learning objectives or topics
        """
        self.learningObjects = self.student.GetUniqueTopics()
        self.learningObjects.dropna(inplace=True)
        
    def FilterData(self):
        """
        remove unnecessary data tuples from the dataset. keep only tuples having verbid in initialized and or answered. tuples containing student records and ignore non learning topics like first day introduction lecture. 
        """
        self.StudentData = self.student.Data[self.student.FilterVerbs(self.uniqueVerbs) & self.student.FilterStudents(self.students) & self.student.FilterLearningObjects(self.LOTypes)]
        self.student.Data = None
        #self.StudentData[self.student.TopicsKey] = self.StudentData[self.student.TopicsKey].apply(lambda x:x.split('/')[-1])
        self.StudentData[self.student.ChildTopicsKey] = self.StudentData[self.student.ChildTopicsKey].apply(lambda x:x.split('/')[-1])
        self.StudentData[self.student.ModuleKey] = self.StudentData[self.student.ModuleKey].apply(lambda x:x[0]['id'].split('/')[-1])
        mask = self.StudentData[self.student.RecordTypeKey] == self.student.ModuleType
        self.StudentData.loc[mask,self.student.ModuleKey] = self.StudentData.loc[mask,self.student.ChildTopicsKey]
        
        mask = None
    def GroupData(self,groupbyKey = ''):
        """
        group data based on topic name, student name, verbid and timestamp to keep a student data together
        """
        if not groupbyKey : groupbyKey = self.groupbyKeys
        #self.StudentData[self.student.TopicsKey] = self.StudentData[self.student.TopicsKey].str.strip()
        
        self.groupedData = self.StudentData.groupby(groupbyKey,as_index=True).size().reset_index(name='count')        
        self.StudentData = None
        #self.groupedData=self.groupedData.rename(columns = {self.student.TopicsKey:self.student.OldTopicsKey})
        self.groupedData[self.student.TimeStamp] = pd.to_datetime(self.groupedData[self.student.TimeStamp],format=self.student.DateTimeFormat)
    def WriteUserAccessDataToFile(self,filePath,groupbyIndex=''):
        if not groupbyIndex : groupbyIndex = self.groupbyIndexes
        finalGroupedData= pd.DataFrame(self.trainingData).groupby(groupbyIndex,as_index=True).size().reset_index(name='count')
        finalGroupedData.to_csv(filePath,index=False)
        finalGroupedData=None
    def TrainTestDataSplit(self,trainEndTime):
        """
        This method is used to split data into test and training set based on timestamp.
        """
        self.trainingData = self.groupedData[self.groupedData[self.student.TimeStamp]<trainEndTime]
        self.testData = self.groupedData[self.groupedData[self.student.TimeStamp]>=trainEndTime] 
        return self.trainingData,self.testData
    
    def WriteOutputToCSVFile(self,filePath):
        """
        finally write the cleaned dataset to specified filepath.
        """
        self.groupedData.to_csv(filePath,index=False)
    def WriteTrainingDataSetToCSVFile(self,filePath):
        """
        writes divided training dataset per week to csv file so that it can used for training each week
        """
        self.trainingData.to_csv(filePath,index=False)
    def WriteTestDataSetToCSVFile(self,filePath):
        """
        writes divided data testing dataset per week to csv file so that it can be
        """
        self.testData.to_csv(filePath,index=False)
        self.testData = None
    def GetMinTimeStamp(self):
        """
        returns minumim timestamp in the dataset and it is mainly used for training and testing data split(boundary condition lower end). 
        """
        #minDate = dt.datetime.strptime(self.groupedData[self.student.TimeStamp].min(),self.student.DateTimeFormat)
        minDate = self.groupedData[self.student.TimeStamp].min()
        return minDate.replace(hour=0,minute=0,second=0, microsecond=0)
    def GetMaxTimeStamp(self):
        
        """
        returns maximum timestamp in the dataset and it is mainly used for training and testing data split(boundary condition higher end).   
        """
        #maxDate = dt.datetime.strptime(self.groupedData[self.student.TimeStamp].max(),self.student.DateTimeFormat)
        maxDate = self.groupedData[self.student.TimeStamp].max()
        maxDate = maxDate.replace(hour=0,minute=0,second=0, microsecond=0)
        return maxDate + dt.timedelta(days=1)
    def ReadGroupedDataFromFile(self,groupedFilePath):
        """
        If the data is already and cleaned and written to disk previously then no need to redo the same things. The grouped data is simply read from the disk.
        """
        if(not os.path.exists(groupedFilePath)):
            raise ValueError('specified grouped data doesn\'t exist')
        self.groupedData = pd.read_csv(groupedFilePath)
        self.groupedData[self.student.TimeStamp] = pd.to_datetime(self.groupedData[self.student.TimeStamp],format = self.student.DateTimeFormat)
    def ReadTrainingDataFromFile(self,filePath):
        """
        The training data split in previous steps is read from specified file while training the model.
        """
        if(not os.path.exists(filePath)):
            raise ValueError('specified training data doesn\'t exist')
        self.trainingData = pd.read_csv(filePath)
    def ReadTestDataFromFile(self,filePath):
        """
        The testing data split in previous steps is read from specified file while testing the model.
        """
        if(not os.path.exists(filePath)):
            raise ValueError('specified testing data doesn\'t exist')
        self.testData = pd.read_csv(filePath)
