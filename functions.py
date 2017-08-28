import pandas as pd
import datetime as dt
import collections




def UwP(dataframe, item1, item2):
    """
    UWP function return a list of Users who have preferences for item i and item j
    Input: Array of user and items ,Item i and Item j
    Output: List of users
    """

    return dataframe[(dataframe.loc[:, item1] != dataframe.loc[:, item2]) & (dataframe.loc[:, item1].notnull())
                     & (dataframe.loc[:, item2].notnull())
                     ].iloc[:, 0].tolist()


def Similarity(dataframe,currentTime,lists):
    """
    Similarity between item1 and item2
    Input: item i and item j
    Output: Average Similarity score between two items
    """
    item1, item2 = lists[0], lists[1]
    if (item1==item2):
        return 1
    else:



        df_test = dataframe[(dataframe.loc[:, item1] != dataframe.loc[:, item2]) & (dataframe.loc[:, item1].notnull())
                            & (dataframe.loc[:, item2].notnull())
                            ].loc[:, [item1, item2]]
        df_test[item1] = pd.to_datetime(df_test[item1])  # converting to date time
        df_test[item2] = pd.to_datetime(df_test[item2])
        # Delta_t
        df_test['Delta_t'] = abs(((df_test.loc[:, item1] - df_test.loc[:, item2]).dt.total_seconds() / 3600).astype(int))
        # Delta D
        df_test['Delta_D'] = abs(df_test[[item1, item2]].min(axis=1) - currentTime.date()).astype(
            'timedelta64[h]').astype('int')
        #df_test['Delta_D'] = abs(df_test[[item1, item2]].min(axis=1) - dt.datetime.now().date()).astype(
        #   'timedelta64[h]').astype('int')
        df_test['S_ij'] = 1 / (df_test.Delta_t + df_test.Delta_D)
        SimScore = df_test.S_ij.sum(axis=0)
        del df_test
        return SimScore


def ListItems(dataframe, item1, number,currentTime):
    """
    Input : dataframe , item for which similar items are required and number of items required
    Output : List of top n items ranked by their average similarity from a particular item i .
    """
    ColList = dataframe.columns[1:].tolist()  # Get the list of the all columns
    ColList.remove(item1)  # remove the item1 from COlList

    SimDict = {}  # Create a dictionary with similar with  Average similar score
    # of items that are similar to item1
    for item in ColList:
        SimDict[item] = Similarity(dataframe,currentTime, item1, item)

    od = collections.OrderedDict(sorted(SimDict.items(), reverse=True))  # Sorted the dictionary using Score value

    Listitem = []  # get list of LO
    for k, v in od.items():
        Listitem.append(k)

    return Listitem[:number]
