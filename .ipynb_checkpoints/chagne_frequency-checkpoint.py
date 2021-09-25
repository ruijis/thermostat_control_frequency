"""Import packages for querying dataset"""
import pymortar
import pandas as pd

def hourly_outlier(md, sd, ed, sh, eh, sl, su, wl, wu):
    """
    Calculate the percentage of normal occupied time outside a specified temeprature range.
    The normal occupied days is Monday to Friday but the occupied time can be specified.
    
    Parameters
    ----------
    md : str
         sensor metadata with prefix of http://buildsys.org/ontologies
    sd : str
         start date with format year-month-day, e.g.'2016-1-1'
    ed : str
         end date with format year-month-day, e.g.'2016-1-31'
    sh : int
         start hour of normal occupied time with 24-hour clock, e.g. 9
    eh : int
         end hour of normal occupied time with 24-hour clock, e.g. 17
    sl : float
         lower bound of the tempearture range in summer, with default F unit
    su : float
         upper bound of the temperature range in summer, with default F unit
    wl : float
         lower bound of the tempearture range in summer, with default F unit
    wu : float
         upper bound of the temperature range in summer, with default F unit

    Returns
    ----------
    p : float
        percentage of outside range time
    """
    assert isinstance(sd, str), 'The start date should be in a string.'
    assert isinstance(ed, str), 'The end date should be in a string.'
    assert sh < eh, "The start and end hour should be 24-hour clock."
    # connect client to Mortar frontend server
    client = pymortar.Client("https://beta-api.mortardata.org")
    data_sensor = client.data_uris([md])
    data = data_sensor.data
    # get a pandas dataframe between start date and end date of the data
    sd_ns = pd.to_datetime(sd, unit='ns', utc=True)
    ed_ns = pd.to_datetime(ed, unit='ns', utc=True)
    df = data[(data['time'] >= sd_ns) & (data['time'] <= ed_ns)]
    # parse the hour and weekday info and add it as a column
    df['hr'] = pd.to_datetime(df['time']).dt.hour
    df['wk'] = pd.to_datetime(df['time']).dt.dayofweek
    df['mo'] = pd.to_datetime(df['time']).dt.month
    # create occupied df by normal office hours and by weekdays
    df_occ = df[(df['hr'] >= sh) & (df['hr'] < eh) &
                (df['wk'] >= 0) & (df['wk'] <= 4)]
    # split the occupied data to the summer and  winter
    df_occ_sum = df_occ[(df_occ['mo'] >= 6) & (df_occ['mo'] <= 8)]
    df_occ_win = df_occ[(df_occ['mo'] >= 12) | (df_occ['mo'] <= 2)]
    # create df that is lower or upper the temperature range
    df_sum_out = df_occ_sum[(df_occ_sum['value'] < sl) | 
                            (df_occ_sum['value'] > su)]
    df_win_out = df_occ_win[(df_occ_win['value'] < wl) |
                           (df_occ_win['value'] > wu)]
    # the number of summer and winter occupied time
    n_occ_all = len(df_occ_sum) + len(df_occ_win)
    # Calculate the percentage of occupied time outside the temeprature range
    p = (len(df_sum_out) + len(df_win_out)) / n_occ_all if n_occ_all != 0 else 0
    return round(p, 2)
    
    