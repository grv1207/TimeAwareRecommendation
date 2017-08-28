import pandas as pd
import numpy as np
from collections import OrderedDict
import time
import warnings
from StudentData import StudentData as SD
class TimeAwareRecommendation :
    #def __init__(self,dataFramePath,Utility_data,groupedDataPath,username,N_recommendation):
    def __init__(self,loMappedFilePath,picklePath,userAccessFilePath):
        self.sd = SD()
        self.LoadData(loMappedFilePath,picklePath,userAccessFilePath)
    def weight_matrix(self,student_id):
        """
        Input : Takes arguemnt as each student id and Group data json file(which contains count of LOs for each Student)
        Output:
                  Return Wieght_Matrix, which contains weight of each LOs corresponds to respective stduents.
        """
        LO_weight = np.array(self.LO_Count_pivot.loc[student_id].dropna(axis=0))  # weight corresponding to each student
        return LO_weight


    def Similarity_Matrix(self):
        """
        Input : Takes Pickle file, which is the calculation of Similairity matrix.
        Output: Retrun Similraity matrix corresponding to each LOs.

        """
        return self.Similarity_matrix

    #def rating_recommendation_with_dict(self,top_recom, df, LO_Count_file, username):
    def rating_recommendation_with_dict(self,top_recom, username):
        """
        Input : Takes number of items to be recommended, dataframe df of student &LOs and LOs count json file for each student
        Output: Return list of all possible recommendtaions corresponding to each students.

        """
        user =username[0:10]

        stu_data_index = self.df.loc[user].to_frame().T  # All LO's for given student
        not_done = list(stu_data_index.columns[stu_data_index.isnull().any()])  # Find LOs not done by Student
        done = list(stu_data_index.dropna(axis=1))  # Get LOs done by the student
        Rating_dict = {}
        #
        for k in range(0, len(not_done)):
            #print('reached till weight')
            df_weight = self.weight_matrix(username)  # Get Weights of all LO's done by the user, corresponding to LO not done by user
            similarilty_student = np.array(self.Similarity_matrix.loc[not_done[k], done]) # Get similarity of ~LO and LO
            Rating_dict[(similarilty_student.dot(df_weight.T)) / similarilty_student.sum()] = not_done[k]  # Rating with Weight
            Final_rating_per_Student = OrderedDict(sorted(Rating_dict.items(), reverse=True)[:top_recom]) # top rated LO's

        return Final_rating_per_Student


    #@app.route('/recommendation/mongo/<string:user>', methods=['GET'])
    def recommendation(self,user,topRecommendations):
        """
        Input :
        Output:  It return list of top-N recommendation
        """
        t = time.clock()
        rating_list = []
        rating_dictionary = self.rating_recommendation_with_dict(topRecommendations, user)
        for L0_Value, LO in rating_dictionary.items():
            rating_list.append(LO)
        #print(time.clock() - t)
        dict_recom = {}
        dict_recom[user] = rating_list
        return dict_recom
    
    def LoadData(self,loMappedFilePath,picklePath,userAccessFilePath):
        pickle = pd.read_pickle(picklePath)
        utility_data =pd.DataFrame(pickle)
        a = list(utility_data)
        self.Similarity_matrix = pd.DataFrame.from_dict(pickle, orient='index', dtype=None)
        self.Similarity_matrix.columns = a
        self.LO_Count_pivot = pd.read_csv(userAccessFilePath)
        self.df = pd.read_csv(loMappedFilePath)  # read Student and LOs data corresponding to each in dataframe df
        self.df.set_index('StudentID', inplace=True)  # Set index of df as StudentID
        #LO_list = self.LO_Count_pivot['object.definition.name.de-DE'].unique()
        LO_list = self.LO_Count_pivot[self.sd.TopicsKey].unique()
        LO_ID = ['LO_' + str(x) for x in range(len(LO_list))]
        dict_List = {}
        for k, v in zip(LO_list, LO_ID):    # Mapping of Original Subject name with LO ID
            dict_List[k] = v
        #self.LO_Count_pivot['LO'] = self.LO_Count_pivot['object.definition.name.de-DE'].apply(lambda x: dict_List[x])
        self.LO_Count_pivot['LO'] = self.LO_Count_pivot[self.sd.TopicsKey].apply(lambda x: dict_List[x])
        #self.LO_Count_pivot = self.LO_Count_pivot.pivot(index='actor.name', columns='LO', values='count')
        self.LO_Count_pivot = self.LO_Count_pivot.pivot(index=self.sd.StudentKey, columns='LO', values='count')
       