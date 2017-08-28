import pandas as pd
class StudentData:
    def __init__(self,path=''):
        """
        Constuctor reads click data of students from specified path and initializes column names
        """
        self.Data = pd.DataFrame()
        if(path!=''):
            self.Data = pd.read_json(path)
            self.Data = pd.io.json.json_normalize(self.Data['statement'])
        self.VerbKey = 'verb.id'
        self.StudentKey ='actor.name'
        #self.TopicsKey ='object.definition.name.de-DE'
        self.OldTopicsKey ='object.definition.name.de-DE'
        self.ObjectType = 'object.definition.type'
        #self.TopicsKey ='object.id'
        self.TopicsKey ='context.contextActivities.parent'
        self.ChildTopicsKey ='object.id'
        self.StudentEmailFormat = 'student[0-9]*@smartlearning.edu'
        self.TimeStamp = 'timestamp'
        self.DateTimeFormat = "%Y-%m-%d %H:%M:%S.%f"
        self.ModuleType = 'http://adlnet.gov/expapi/activities/module'
        self.LessonType ='http://adlnet.gov/expapi/activities/lesson'
        self.ModuleGroupFormat = 'https://vfh143.beuth-hochschule.de/fokus/fame/openStudio/middleware/repository/modules/'
        self.ModuleKey = 'context.contextActivities.parent'
        self.RecordTypeKey =  'object.definition.type'
    def GetUniqueVerbs(self):
        """
        returns list of unique verb ids from the click data
        """
        return pd.Series(self.Data[self.VerbKey].unique())
    #def GetUniqueModules(self):
        """
        return list of modules in the dataset
        """
    #    return pd.Series(
    def GetUniqueStudents(self):
        """
        return list of students matching student[0-9]*@smartlearning.edu format
        """
        students = pd.Series(self.Data[self.StudentKey].unique())
        students.dropna(inplace=True)
        students = students[students.str.contains(self.StudentEmailFormat)]
        students.reset_index(inplace=True,drop=True)
        return students
    
    def GetUniqueTopics(self):
        """
        returns list of unique lecture topics in the provided data
        """
        return pd.Series(self.Data[self.TopicsKey].unique())
    
    def GetStudentData(self):
        """
        returns dataset at current time 
        """
        return self.Data
    
    def FilterVerbs(self,verbs):
        """
        filters and returns dataset by provided verbids, usually by initialized and answered
        """
        return self.Data[self.VerbKey].isin(verbs)
    
    def FilterStudents(self,students):
        """
        filters and returns dataset based on actor name to keep only student data
        """
        return self.Data[self.StudentKey].isin(students)
    
    def FilterLearningObjects(self,recordTypes):
        """
        filters dataset based on the specified recordType like 'module','lesson'
        """
        #return self.Data[self.TopicsKey].isin(learningObjects)
        return self.Data[self.RecordTypeKey].isin(recordTypes)