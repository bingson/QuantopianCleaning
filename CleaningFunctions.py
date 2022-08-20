# -*- coding: utf-8 -*-



def timeit(method):
    """
    Calculate and print function runtime.
    """
    def timed(*args, **kw):
        ts         = time.time()
        result     = method(*args, **kw)
        te         = time.time()
        hrs, rem   = divmod(te-ts,3600)
        mins, secs = divmod(te-ts,60)
        print 'Run time: {} {} hrs {} mins {:4.4f} secs'.\
            format(method.__name__, int(hrs), int(mins), secs)
        return result
    return timed


@timeit
def run_pipeline_list(pipe, pipeline_dates):
    """
    Run pipeline data request for a list of specific dates.

    This function wrapper enables the batch download of data for a list of dates
    or discontinuous time intervals—useful for reducing the size of the
    data file necessary to backtest mid to long-term fundamental trading strategies
    driven by quarterly reporting dates.

    Parameters
    ----------
    pipe : `quantopian.pipeline`
        A Pipeline object represents a collection of named expressions to be
        compiled and executed by Quantopian's PipelineEngine.
        A Pipeline has two main attributes: 'columns', a dictionary of named
        :class:`~quantopian.pipeline.Term` instances (e.g., stock price, earnings)
        , and 'screen', a :class:`~quantopian.pipeline.Filter` representing
        criteria for including an asset in the results of a Pipeline.
    pipeline_dates : List[pandas.DatetimeIndex]
        List of dates to download data for.

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe where the first level is a datetime, the second level
        is an Equity object corresponding to pipeline security filters and the
        columns for the previously added pipeline factors for each stock.

    """

    chunks     = []
    item_count = 0
    start      = time.time()
    for date in pipeline_dates:

        print "Processing {:.10} pipeline".format(str(date[0]))

        results = run_pipeline(pipe, date[0], date[0])

        # convert category dtype to object to fix pd.concat error
        for col in list(results.select_dtypes(include=['category']).columns):
            results[col] = results[col].astype('O')
        chunks.append(results)

        # show pipeline wait time estimate
        end = time.time()
        item_count += 1
        elapsed                  = (end - start)
        est_wait                 = (len(pipeline_dates) - item_count) * (elapsed/item_count)
        elapsed_min, elapsed_sec = divmod(elapsed, 60)
        est_min, est_sec         = divmod(est_wait, 60)
        print 'elapsed time: {} mins {} secs'.format(int(elapsed_min), int(elapsed_sec))
        print 'shape: {1}'.format(str(date[0]),results.shape)
        print 'estimated wait: {} mins {} secs\n'.format(int(est_min), int(est_sec))

    results = pd.concat(chunks)

    # convert Object dtype columns back to category after pd.concat
    for col in list(results.select_dtypes(include=['O']).columns):
            results[col] = results[col].astype('category')

    print '\nCombined dataframe created'
    return results

@timeit
def create_dt_list(beg_date, end_date, day_index):
    """
    Generate a list of valid historic market open dates corresponding to a day of the week.

    The list can be used as an input for the run_pipeline_list function
    to test mid-term to long-term trading strategies.

    Parameters
    ----------
    beg_date : str
        Starting date that defines the time interval over which date points will
        be generated (format: YYYY-MM-DD)
    end_date : str
        End date that defines the time interval over which date points will
        be generated (format: YYYY-MM-DD)
    day_index : int
        day of the week mon = 1, tue = 2, wed = 3, thu = 4, fri = 5

    Returns
    -------
    List[pandas.DatetimeIndex]
        list of DatetimeIndex objects
    """
    trng                   = pd.date_range(beg_date, end_date)
    cal                    = USFederalHolidayCalendar()
    holidays               = cal.holidays(start=trng.min(), end=trng.max())
    trng_no_holidays       = trng[~trng.isin(holidays)]
    trng_no_holidays_wknds = trng_no_holidays[trng_no_holidays.weekday < 5]  # exclude saturday/sunday; != 5/6
    pipeline_dates         = []

    for year in set(trng_no_holidays_wknds.year):                               
        tmp = trng_no_holidays_wknds[trng_no_holidays_wknds.year == year]       
        for week in set(tmp.week): 
            temp = tmp[tmp.week == week]
            day  = temp[temp.weekday == day_index]
            if len(day) == 1: pipeline_dates.append(day) 
            else: pipeline_dates.append(temp[temp.weekday == temp.weekday.max()]) 
            # pipeline_dates.append(temp[temp.weekday == temp.weekday.min()]) # begining of week
    return sorted(pipeline_dates)

@timeit
def run_pipeline_chunks(pipe, start_date, end_date, chunks_len = None): 
    """
    Split large pipeline data request into smaller chunks.

    This function wrapper splits up a large pipeline data request into smaller
    pieces to overcome size and memory limits of Quantopian's pipeline engine.

    Parameters
    ----------
        pipe : `quantopian.pipeline`         
            A Pipeline object represents a collection of named expressions to be 
            compiled and executed by Quantopian's PipelineEngine.
            A Pipeline has two main attributes: 'columns', a dictionary of named
            :class:`~quantopian.pipeline.Term` instances (e.g., stock price, earnings)
            , and 'screen', a :class:`~quantopian.pipeline.Filter` representing 
            criteria for including an asset in the results of a Pipeline.
        start_date : pandas.Timestamp 
            starting date of time period to retrieve data for
            e.g., pd.to_datetime('2003-01-01', format = '%Y-%m-%d')
        end_date : pandas.Timestamp
            end date of time period to retrieve data for
        chunks_len : pandas.Timedelta
            the time interval or duration that determines the size 
            of each chunk or pipeline data request. 

    Returns
    -------
    pd.DataFrame
        MultiIndex dataframe where the first level is a datetime, the second level
        is an Equity object corresponding to pipeline security filters and the
        columns for the previously added pipeline factors for each security.
    """
    chunks  = []  
    current = pd.Timestamp(start_date)  
    end     = pd.Timestamp(end_date)  
    step    = pd.Timedelta(weeks=26) if chunks_len is None else chunks_len  
    while current <= end:  
        current_end = current + step  
        if current_end > end:  
            current_end = end  
        print 'Running pipeline: {:.10} - {:.10}'.format(str(current), str(current_end))
        results = run_pipeline(pipe, current.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))  
        
        
        # addresses issue where pd.concat() fails if category levels are different between dataframes
        for col in list(results.select_dtypes(include=['category']).columns):
            results[col] = results[col].astype('O')  

        chunks.append(results)  
        # pipeline returns more days than requested (if no trading day), get last date from the results  
        current_end = results.index.get_level_values(0)[-1].tz_localize(None)
        current     = current_end + pd.Timedelta(days=1)
    try:
        results = pd.concat(chunks)
        for col in list(results.select_dtypes(include=['O']).columns):
            results[col] = results[col].astype('category')
        print 'Combined dataframe created'
        return results
    
    except:
        print 'pd.concat failed'
        return chunks


def Trl_dates (end_date, per, mltplr):
    """get dates for trailing information update intervals""" 
    end_date = datetime.strptime(end_date[0], '%Y-%m-%d')
    out = []
    for n in xrange(per):
        if n != 0:
            Q = str(end_date - MonthEnd(mltplr)*n)[:10]
            out.append([Q])
        else: pass
    end_date = str(end_date)[:10]
    out.append([end_date])
    return sorted(out)

# @timeit
def sum_col_append(df_tgt, df_src, var_list):
    """sum pandas.dataframe columns and add them to a tgt DataFrame"""
    for var, components in var_list.iteritems():
        df_tgt[var] = df_src[components].sum(axis = 1)
    return df_tgt

# DollarVolume will calculate yesterday's 
# dollar volume for each stock in the universe.
class DollarVolume(CustomFactor):
    
    inputs = [USEquityPricing.close, 
              USEquityPricing.volume]
    window_length = 1
    
    # Dollar volume is volume * closing price.
    def compute(self, today, assets, out, close, volume):
        out[:] = (close[0] * volume[0])

@timeit
def mltplyArr(FS, asof_valid):
    """iterative multiplication of two lists of dataframes"""
    out = []
    for arr in zip(FS, asof_valid):
        out.append(pd.DataFrame(arr[0].values * arr[1].values, 
                                columns=arr[0].columns, 
                                index=arr[0].index))
    return out

@timeit
def subset_df(src_df, srchType, srchStrngs, mrg_out = True ):
    """
    Create subset(s) of pandas.DataFrame based on different string match patterns
    on column index labels.

    Parameters
    ----------
    src_df : pd.DataFrame
        MultiIndex dataframe where the first level is a datetime, the second level
        is an Equity object corresponding to pipeline security filters, and the
        columns for the pipeline factors for each security.
    srchType : str
        Determines the type of match pattern used to select columns
        for the dataframe subset. Accepted options include whether the source 
        DataFrame column label 'startswith','contains', or '!contains' (does not contain) 
        any of the strings provided in the search string parameter.
    srchStrngs : list of str
        list of strings to select columns for the subset Dataframe(s)
    mrg_out : bool, Optional
        If set to True any column matches from the list of strings in `srchStrngs`
        will be combined in a single returned subset DataFrame. If set to
        false a separate subset DataFrame will be returned for each 
        string provided in `srchStrngs`. By default set to True.

    Returns
    -------
    pd.DataFrame
        MultiIndex dataframe where the first level is a datetime, the second level
        is an Equity object corresponding to pipeline security filters and the
        columns for the previously added pipeline factors for each security.
    """

    out = []
    if srchType == 'startswith':
        if mrg_out == True:
            return src_df[src_df.columns[pd.Series(src_df.columns).
                                         str.startswith(tuple(srchStrngs))]]
        else:
            for strng in srchStrngs:
                out.append(src_df[src_df.columns[pd.Series(src_df.columns).
                                                 str.startswith(strng)]])
            return out
    elif srchType == 'contains':
        if mrg_out == True: 
            srchStrngs = '|'.join(srchStrngs)
            return src_df.filter(regex=srchStrngs, axis = 1)
        else:
            for idx, string in enumerate(srchStrngs):
                out.append(src_df[idx][src_df[idx].columns[pd.Series(src_df[idx].columns).
                                                           str.contains(string)]])
            return out
    elif srchType == '!contains':
        if mrg_out == True:
            srchStrngs = '|'.join(srchStrngs)
            srchStrngs = '^((?!' + srchStrngs + ').)*$'      # regex for match exclusion
            return src_df.filter(regex=srchStrngs, axis =1)  
        else:
            for idx, string in enumerate(srchStrngs):
                out.append(src_df[idx][src_df[idx].columns[pd.Series(src_df[idx].columns).
                                                           str.contains(string)==False]])
            return out 
    else: print 'invalid subset_df parameter input'


@timeit        
def drop_rows(df_list, bad_rows): 
    """Drops bad_rows(index) from each dataframe in df_list"""
    out = []
    for df in df_list:
        out.append(df[~df.index.isin(bad_rows)])
    return out

@timeit
def date_validate(asof_df, asof_maxdates):  
    """ 
    @summary: Create boolean df to determine which financial statement numbers 
        are still relevant. These boolean validation tables will be multiplied (element by element)
        by relevant financials statement df
    @param asof_df = pd.dataframe of datetimes(ns);
    @param asof_maxdates = pd.series of datetimes(ns)
    @return asof_lag: (days) difference between most recent asof date on record (asof_maxdates) 
        for a particular point in time and the asof date associated with a particular 
        financial statement line item.     
    @return asof_maxdates: boolean df where value = 1 indicates an item's as_of date matches
        the most rec   ent asof date on record.
    """
    asof_lag = -asof_df.sub(asof_maxdates, axis = 0)
    asof_valid = asof_lag.astype(int)
    asof_valid[asof_valid != 0] = np.NaN 
    asof_valid[asof_valid == 0] = 1
    return asof_lag, asof_valid  

