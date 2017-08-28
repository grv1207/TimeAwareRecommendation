import pandas as pd
import functions as fn
import multiprocessing as mp
import pickle
import datetime
from functools import partial
from itertools import repeat
import logging
import socket
class SimilarityMatrixClass:
    def SimilarityMatrix(self,dataframe,currentTime,picklePath):
        """
        Finds the item-item similarity matrix and stores result in a dictionary(dictionary.pickle)
        Input: Dataframe (student-LO matrix)
        Output : Time taken to create the  item-item similarity matrix
        """
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger('similaritymatrix')
        logger.info('Starting calculation....')
        Col_List = dataframe.columns.tolist()
        Col_List.remove('StudentID')
        Sim_Dict = {}
        logger.info(Col_List)
        start = datetime.datetime.now()
        try :
            pool = mp.Pool(10)
            for i, LO in enumerate(Col_List):
                try:

                    Sim_Dict[LO] = pool.map(partial(fn.Similarity,dataframe,currentTime), zip(repeat(LO), Col_List))
                    if (i%100 == 0):
                        logger.info(str(LO))
                        end = datetime.datetime.now()
                        print("timeTaken: ", end - start)
                except socket.error as ex:
                    if str(ex) == "[Errno 35] Resource temporarily unavailable":
                        logger.info('socket error....')
                        continue
                    raise ex
            with open(picklePath, 'wb') as handle:
                pickle.dump(Sim_Dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
            logger.info('Finish calculation')
            end = datetime.datetime.now()
            print("timeTaken: ", end-start)
        finally:
            pool.close()
            pool.join()
        
    def sum_keys(self,d):
        return (0 if not isinstance(d, dict)
                else len(d) + sum(self.sum_keys(v) for v in d.items()))

    def count_values(self,d):
        dict_count = {}
        for k,v in d.items():
            dict_count[k] = [len([item for item in v])]
        return dict_count

    #    def GetStudentDataToFile(fileName):
    #        """
    #        below code creates an instance of ProcessStudentData and writes cleaned data to specified file.
    #        """
    #        student = ps('AWT_statements.json')
    #        student.FilterData()
    #        student.GroupData()
    #        student.WriteOutputToCSVFile(fileName)
    #
    def __init__(self,loFilePath,currentTime,picklePath = 'Data/dictionary.pickle'):  
        #if __name__ == '__main__':

        #fileName = 'df_Student_User'
        #GetStudentDataToFile(fileName)
        """
        Returns a list top 10  items that are similar to LO_1
        """
        df_Student_User = pd.read_csv(loFilePath)
        self.SimilarityMatrix(df_Student_User,currentTime,picklePath)
        print('Similarity done')