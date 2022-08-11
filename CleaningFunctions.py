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
        list of dates to download data for
 
    Returns
    -------
    pd.DataFrame
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
        input format YYYY-MM-DD
    end_date : str
        input format YYYY-MM-DD
    day_index : int
        mon = 1, tue = 2, wed = 3, thu = 4, fri = 5

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

    for year in set(trng_no_holidays_wknds.year):                                 # set -> get unique year values in time range
        tmp = trng_no_holidays_wknds[trng_no_holidays_wknds.year == year]         # slice year wise
        for week in set(tmp.week):                                                # for each week in slice
            temp = tmp[tmp.week == week]
            day = temp[temp.weekday == day_index]                                 # select a day of the week 0-monday, 1-tuesday . . . 4-friday
            if len(day) == 1: pipeline_dates.append(day) 
            else: pipeline_dates.append(temp[temp.weekday == temp.weekday.max()]) # last day of week if problem
            # pipeline_dates.append(temp[temp.weekday == temp.weekday.min()]) # begining of week
    return sorted(pipeline_dates)

@timeit
def run_pipeline_chunks(pipe, start_date, end_date, chunks_len = None): 
    """
    
    Split large pipeline data request into smaller chunks



    Args:
        pipe (_type_): `quantopian.pipeline`         
            A Pipeline object represents a collection of named expressions to be 
            compiled and executed by Quantopian's PipelineEngine.
            A Pipeline has two main attributes: 'columns', a dictionary of named
            :class:`~quantopian.pipeline.Term` instances (e.g., stock price, earnings)
            , and 'screen', a :class:`~quantopian.pipeline.Filter` representing 
            criteria for including an asset in the results of a Pipeline.
        start_date (_type_): _description_
        end_date (_type_): _description_
        chunks_len (_type_, optional): _description_. Defaults to None.

    Returns:
        _type_: _description_

    @summary: Drop-in replacement for run_pipeline. Run_pipeline fails over 
        a very long period of time (memory usage), so we need to split in 
        chunks the pipeline and concatenate the results  
    @param start_date/end_date: start and end timestamps
    @param chunks_len: number of pipeline weeks to process each data call
    @param pipe: pipeline name (init feature columns)
    @return dataframe corresponding to pipeline columns initialized
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
        
        
        # addresses problem that pd.concat() fails if category levels are different between dataframes
        for col in list(results.select_dtypes(include=['category']).columns):
            results[col] = results[col].astype('O')  

        chunks.append(results)  
        # pipeline returns more days than requested (if no trading day), so get last date from the results  
        current_end = results.index.get_level_values(0)[-1].tz_localize(None)  
        current = current_end + pd.Timedelta(days=1)
    try:
        results = pd.concat(chunks)
        for col in list(results.select_dtypes(include=['O']).columns):
            results[col] = results[col].astype('category')
        print 'Combined dataframe created'
        return results
    
    except:
        print 'pd.concat failed'
        return chunks

