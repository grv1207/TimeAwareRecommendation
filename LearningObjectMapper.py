import pandas as pd
class LearningObjectMapper :
    def __init__(self,groupedDataPath):
        self.df_full = pd.read_csv(groupedDataPath,usecols= [0,1,3])
        self.df_full.columns = ['LO','StudentID','time']
        self.df_full['StudentID'] = self.df_full.StudentID.apply(lambda x : x.split('@smartlearning.edu')[0]) # removing redundent data
        self.df_full['time'] = self.df_full.time.apply(lambda x : x.split('.')[0]) 
        LO_list = self.df_full.LO.unique()
        LO_ID= [ 'LO_'+ str(x) for x in range (len(LO_list))]      # created and ID for LO
        combine = zip(LO_list,LO_ID)
        self.listdict = {}
        for x,y in combine:
            self.listdict[x] = y
        self.df_full['LO'] = self.df_full.LO.apply(lambda x :self.listdict[x])   # mapped LO  from dataset with an ID from LO_ID
    def MapLearningObjects(self,savePath,saveDictPath):
        
        df_final = self.df_full.groupby(['StudentID','LO'], as_index=False).last().pivot(index='StudentID',columns='LO', values='time')
        self.df_full = None
        #print(df_final.head())
        df_final.reset_index(level=None,inplace=True)
        df_Student_User = df_final.iloc[:,0:900]
        df_final = None
        df_Student_User.fillna('',inplace=True)
        df_Student_User.to_csv(savePath,index=False)
        df_Student_User = None
        pd.Series(self.listdict).to_csv(saveDictPath)
        self.listdict = None
