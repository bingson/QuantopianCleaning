# -*- coding: utf-8 -*-

## GETTING DATA ________________________________________________________________

def timeit(method):
    """ decorator to print the runtime of any function """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        hrs, rem = divmod(te-ts,3600)
        mins, secs = divmod(te-ts,60)
        print 'Run time: {} {} hrs {} mins {:4.4f} secs'.\
        format(method.__name__, int(hrs), int(mins), secs)
        # print 'Run time: %r %2.2f sec' %(method.__name__, te-ts)
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
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing selected pipeline factors.
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
def dt_intervals(beg_date, end_date, day_index):
    """
    Generate a list of valid historic weekly market open dates corresponding to 
    a particular day of the week.

    The list can be used as an input for the run_pipeline_list function
    to test mid-term to long-term trading strategies.

    Parameters
    ----------
    beg_date : str
        Starting date that defines the time interval over which date points will
        be generated (format: YYYY-MM-DD).
    end_date : str
        End date that defines the time interval over which date points will
        be generated (format: YYYY-MM-DD).
    day_index : int
        Day of the week mon = 1, tue = 2, wed = 3, thu = 4, fri = 5.

    Returns
    -------
    List[pandas.DatetimeIndex]
        List of DatetimeIndex objects corresponding to the input parameters.
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
    Split large pipeline data download into smaller chunks.

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
    pandas.DataFrame
        MultiIndex dataframe where the first level represent date (datetime64), 
        the second level company stock symbols, and columns pipeline factors.
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

@timeit
def sum_col_append(df_tgt, df_src, var_list):
    """
    Sum specified list of columns from one pandas.DataFrame and append result to another
    pandas.DataFrame.

    Parameters
    ----------
    df_tgt : pandas.DataFrame
        DataFrame to append summary columns to.
    df_src : pandas.DataFrame
        Dataframe with the columns to sum.
    var_list : Dict [str,list[str]]
        Dictionary of lists, with each list containing column headings of 
        columns to sums (key becomes column heading of summed columns).

    Returns
    -------
    pandas.DataFrame
        df_tgt DataFrame with summed columns from the df_src DataFrame.
    """
    for var, components in var_list.iteritems():
        df_tgt[var] = df_src[components].sum(axis = 1)
    return df_tgt

class DollarVolume(CustomFactor):
    """ 
        Create DollarVolume custom factor 

        DollarVolume will calculate the previous day's
        dollar volume for each stock in the universe.

    """
    
    inputs = [USEquityPricing.close, 
              USEquityPricing.volume]
    window_length = 1
    
    # Dollar volume is volume * closing price.
    def compute(self, today, assets, out, close, volume):
        out[:] = (close[0] * volume[0])

## TRANSFORMING DATA ___________________________________________________________

@timeit
def mltplyArr(FS, asof_valid):
    """
    Multiply (element by element) two lists of DataFrames together.

    Used to filter out stale data. Input DataFrames must have the same 
    dimensions and hierarchical index levels.

    Parameters
    ----------
    FS : list of pandas.DataFrame
        MultiIndex dataframe where the first level are date (datetime64), 
        the second level company stock symbols, and columns represent pipeline factors.
    asof_valid : list of pandas.DataFrame
        MultiIndex dataframe where the first level are date (datetime64), 
        the second level company stock symbols. The column headings represent 
        pipeline factors and column values are boolean values with 1s representing 
        valid data. 

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe where the first level are date (datetime64), 
        the second level company stock symbols, and the columns pipeline factors.
    """
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
        MultiIndex dataframe where the first levels are datetimes, the second levels
        correspond company stock symbols and the columns for the previously 
        added pipeline factors for each security.
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
    Create 2 DataFrames: 1 used for auditing and 1 to filter out date mismatches 
    between filing date and reported 'as of' dates for each morningstar line item. 

    Parameters
    ----------
    asof_df : pandas.DataFrame of datetimes(ns)
        MultiIndex dataframe where 1st level index = data date, 
        2nd level index = stock symbol, and column values are the as of dates 
        associated with each line item within a particular data date group
    asof_maxdates : pandas.DataFrame of datetimes(ns)
        MultiIndex dataframe with same index levels where column values represent 
        the most recent as_of date among all line items within a particular data date level

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing the differences (in days) between the two dates above. 
    pandas.DataFrame
        MultiIndex boolean dataframe (same index levels as above) where ones indicate 
        valid data with matching 'as of' dates.
        

    """
    asof_lag = -asof_df.sub(asof_maxdates, axis = 0)
    asof_valid = asof_lag.astype(int)
    asof_valid[asof_valid != 0] = np.NaN 
    asof_valid[asof_valid == 0] = 1
    return asof_lag, asof_valid  

@timeit
def reformulate_CFS(CFS_f):
    """
    Reformulated cashflow statement. 

    Free cash flow the difference between cash flow from operations and cash 
    investment in operationsis the main focus in DCF analysis, liquidity analysis, 
    and financial planning. Free cash flow, the net cash generated by operations 
    (after cash investment), determines the ability of the firm to pay off its debt 
    and return cash to shareholders. 
    
    After the balance sheet and income statement are reformulated, separating 
    financing from operating activities, we can directly calculate Free Cash Flow 
    (FCF) using either of the following accounting identities.

    
    FCF = Operating Income - Change in Net Operating Assets 
    FCF = OI - Delta NOA 

    FCF = Net financial expense - Change in net financial obligations + Net dividends 
    FCF = NFE - Delta NFO + d

    These identities make reformulation of the cashflow statement unnecessary.

    Parameters
    ----------
    CFS_f : pandas.DataFrame
        

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing cash flow statement line items.
        _description_
    """
    return CFS_f

@timeit
def reformulate_BS(BS_f):
    """
    Reformulate balance sheet.
    
    The main idea behind financial statement reformulation is isolating financial 
    assets/liabilities from operating assets/liabilities as a precursor to calculating 
    more refined measures of financial leverage and operating profitability. 
    (See Penman, Financial Statement Analysis and Security Valuation (2013) 
    ISBN 978-007-132640-7)
    
    Parameters
    ----------  
    BS_f : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing balance sheet related pipeline factors.

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing reformulated balance sheet items.
    """
    
    BS_shrtTrm_FA = [
        'BS_cash_cash_equivalents_and_marketable_securities',
    ]

    BS_lngTrm_FA = [    
        'BS_investments_and_advances'
    ]

    BS_FO = [
        'BS_current_debt_and_capital_lease_obligation',
        'BS_long_term_debt_and_capital_lease_obligation',
        'BS_preferred_stock'
    ]
    BS_CSE = ['BS_common_stock_equity']
    
    BS_rf_list = {
        'BS_shrtTrm_FA' : BS_shrtTrm_FA,
        'BS_lngTrm_FA'  : BS_lngTrm_FA,
        'BS_FO'         : BS_FO,
        'BS_CSE'        : BS_CSE,
    }
    
    BS_rf = pd.DataFrame()
    BS_rf = sum_col_append(BS_rf, BS_f, BS_rf_list)
    BS_rf['BS_FA']= BS_rf[['BS_shrtTrm_FA','BS_lngTrm_FA']].sum(axis=1)
    BS_rf['BS_OA'] = BS_f['BS_total_assets'] - BS_rf['BS_FA']
    BS_rf['BS_NFO'] = BS_rf['BS_FO'] - BS_rf['BS_FA']
    BS_rf['BS_NOA'] = BS_rf['BS_NFO'] + BS_rf['BS_CSE'] + BS_f['BS_minority_interest'].fillna(0)
    BS_rf['BS_OL'] = BS_rf['BS_OA'] - BS_rf['BS_NOA']

    col_order = [
        'BS_FA',
        'BS_OA',
        'BS_FO',
        'BS_NFO',
        'BS_CSE',
        'BS_NOA',
        'BS_OL'
        # 'BS_shrtTrm_FA',
        # 'BS_lngTrm_FA'
    ]

    return BS_rf.reindex(columns = col_order)

@timeit
def reformulate_IS(IS_f):
    """
    Reformulate income statement.
    
    The main idea behind financial statement reformulation is isolating financial 
    assets/liabilities from operating assets/liabilities as a precursor to calculating 
    more refined measures of financial leverage and operating profitability. 
    (See Penman, Financial Statement Analysis and Security Valuation (2013) 
    ISBN 978-007-132640-7)

    Parameters
    ----------  
    BS_f : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing income statement related pipeline factors.

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with column values 
        representing reformulated income statement items.
    """
    
    IS_rf = pd.DataFrame()
    
    # Core Net Financial Expense
    IS_rf['IS_CoreNFE'] \
        = IS_f['IS_net_non_operating_interest_income_expense'].fillna(0) * (1-IS_f['IS_tax_rate_for_calcs']) \
        - IS_f['IS_preferred_stock_dividends'].fillna(0) 
    
    IS_rf['IS_UFE'] = 0 
        # = IS_f['IS_net_foreign_exchange_gain_loss'].fillna(0) \
        # + IS_f['IS_foreign_exchange_trading_gains'].fillna(0)
        # Unusual Financial Expense; data provider doesn't provide information on adjustments to 
        # marketable securities separate from other unusual gains/losses. Fields exist, but only with nan data.

    IS_rf['IS_NFE'] = IS_rf['IS_CoreNFE'] + IS_rf['IS_UFE']
    IS_rf['IS_OI'] = IS_f['IS_operating_income']
    IS_rf['IS_OR'] = IS_f['IS_operating_revenue']
    
    # IS_rf['PM'] = IS_rf['IS_OI']/IS_f['IS_operating_revenue'] # profit margin
    # IS_f['IS_net_income_including_noncontrolling_interests'].fillna(0)

    col_order = [
        'IS_CoreNFE',
        'IS_UFE',
        'IS_NFE',
        'IS_OI',
        'IS_OR'
        # 'BS_shrtTrm_FA',
        # 'BS_lngTrm_FA'
    ]

    return IS_rf.reindex(columns = col_order)

@timeit
def DPNT_ratios(BSrf_0yAgo, BSrf_1yAgo, ISrf_0yAgo, BS_avg = 1):
    
    """
    Calculate refined Dupont measures of profitability and leverage as an intermediate input
    to calculate S-score model features (Penman and Zhang, 2006). 
    
    See Nissim and Penman (2003) for more detailed information on the Dupont calculation.

    Parameters
    ----------
    BSrf_0yAgo : pandas.DataFrame
        Reformulated balance sheet for most recent year available.
    BSrf_1yAgo : _type_
        Reformulated balance sheet of year prior to the most recent available.
    ISrf_0yAgo : _type_
        Reformulated income stateemnt most recent year.
    BS_avg : int, optional
        Boolean variable to switch between using most recent end of year balance 
        sheet values or an average of last two balance sheet years, by default uses
        an average

    Returns
    -------
    pandas.DataFrame
        Components of advanced Dupont decomposition
        ROCE = RNOA + FLEV (RNOA - NBC)

        where
            ROCE = Return on Common Equity 
            RNOA = Return on Net Operating Assets 
            FLEV = Financial Leverage
            NBC  = net borrowing costs
    """

    DPNT_df = pd.DataFrame(index = ISrf_0yAgo.index.union(BSrf_1yAgo.index))
    if BS_avg == 1: 
        DPNT_df['RNOA']   = ISrf_0yAgo['IS_OI']  / ((BSrf_1yAgo['BS_NOA'] + BSrf_0yAgo['BS_NOA'])/2) 
        DPNT_df['NBC']    = ISrf_0yAgo['IS_NFE'] / ((BSrf_1yAgo['BS_NFO'] + BSrf_0yAgo['BS_NFO'])/2)
        DPNT_df['FLEV']   = ((BSrf_1yAgo['BS_NFO'] + BSrf_0yAgo['BS_NFO'])/2) / \
                            ((BSrf_1yAgo['BS_CSE'] + BSrf_0yAgo['BS_CSE'])/2)
        DPNT_df['CR']     = ((BSrf_1yAgo['BS_NOA'] + BSrf_0yAgo['BS_NOA'])/2)  / \
                            ((BSrf_1yAgo['BS_CSE'] + BSrf_0yAgo['BS_CSE'])/2)
        DPNT_df['ATO']    = ISrf_0yAgo['IS_OR']  / ((BSrf_1yAgo['BS_NOA'] + BSrf_0yAgo['BS_NOA'])/2)
    else: 
        DPNT_df['RNOA']   = ISrf_0yAgo['IS_OI']  / BSrf_1yAgo['BS_NOA'] # RNOA = rtn on net operating assets
        DPNT_df['NBC']    = ISrf_0yAgo['IS_NFE'] / BSrf_1yAgo['BS_NFO'] # NBC  = net borrowing cost
        DPNT_df['FLEV']   = BSrf_1yAgo['BS_NFO'] / BSrf_1yAgo['BS_CSE'] # FLEV = financial leverage
        DPNT_df['CR']     = BSrf_1yAgo['BS_NOA'] / BSrf_1yAgo['BS_CSE'] # CR   = capitalization ratio
        DPNT_df['ATO']    = ISrf_0yAgo['IS_OR']  / BSrf_1yAgo['BS_NOA'] # ATO  = asset turnover (operating)
    
    DPNT_df['PM']     = ISrf_0yAgo['IS_OI'] / ISrf_0yAgo['IS_OR']  
    DPNT_df['SPREAD'] = DPNT_df['RNOA'] - DPNT_df['NBC'] 
    DPNT_df['ROCE']   = (DPNT_df['PM']  * DPNT_df['ATO']) + (DPNT_df['FLEV'] * DPNT_df['SPREAD'])
    
    return DPNT_df

# @timeit
def reformulate_FS(FS_collection):
    """
    Apply financial statement reformulation functions.

    Parameters
    ----------
    FS_collection : pandas.DataFrame
        Raw morningstar balance sheet, income, and cash flow statement DataFrames.

    Yields
    ------
    pandas.DataFrame
        Reformulated balance sheet, income, and cash flow statements.
    """
    
    
    for FS in FS_collection:
        if   FS.columns.equals(BS_f_df.columns)  : yield reformulate_BS(FS)
        elif FS.columns.equals(IS_f_df.columns)  : yield reformulate_IS(FS)
        elif FS.columns.equals(CFS_f_df.columns) : yield reformulate_CFS(FS)
        else: print "reformulate_FS: Financial statement not recognized"

# @timeit
def calc_factors(DPNT_0yAgo, DPNT_1yAgo, BS_0yAgo, BS_1yAgo, IS_0yAgo_TTM, CFS_0yAgo_TTM, current_end):
    """
    Calculate S-score factors for a particular data date.
    
    For a more detailed explanation see Penman and Zhang (2006) 
    Modeling Sustainable Earnings and P/E Ratios with Financial Statement Analysis 
    [https://ssrn.com/abstract=318967]. 

    Parameters
    ----------
    DPNT_0yAgo : pandas.DataFrame 
        Dupont ratio components most recent year
    DPNT_1yAgo : pandas.DataFrame 
        Dupont ratio components 1 year ago
    BS_0yAgo : pandas.DataFrame 
        Reformulated balance sheet most recent year
    BS_1yAgo : pandas.DataFrame 
        Reformulated balance sheet 1 year ago
    IS_0yAgo_TTM : pandas.DataFrame 
        Annualized reformulated income statement (add 4 quarters) 
    CFS_0yAgo_TTM : pandas.DataFrame 
        Annualized cash flow statement (add 4 quarters)
    current_end : pandas.DatetimeIndex
        most recent end of year period for historic data date.

    Returns
    -------
    pandas.DataFrame 
        DataFrame of S-score features for a particular data date.
    """
    out = pd.DataFrame()
    out['RNOA_0']       = DPNT_0yAgo['RNOA']
    out['delta_RNOA_0'] = ((DPNT_0yAgo['RNOA']  - DPNT_1yAgo['RNOA']) / DPNT_1yAgo['RNOA'])
    out['delta_PM_0']   = ((DPNT_0yAgo['PM']    - DPNT_1yAgo['PM']  ) / DPNT_1yAgo['PM'])
    out['delta_ATO_0']  = ((DPNT_0yAgo['ATO']   - DPNT_1yAgo['ATO'] ) / DPNT_1yAgo['ATO'])
    out['delta_NOA_0']  = ((BS_0yAgo['BS_NOA']  - BS_1yAgo['BS_NOA']) / BS_1yAgo['BS_NOA'])
    out['accr_0']       = IS_0yAgo_TTM['IS_OI'] - CFS_0yAgo_TTM['CFS_operating_cash_flow']
    out['mkt_EP']       = IS_0yAgo_TTM['IS_OI']/nfo_df.xs(current_end)['V_mktCp']
    out = winsorize(out, limits = 0.01)
    out = zscore(out) 
    out['BS_negNOA'] = nfo_df.xs(current_end)['BS_negNOA']
    out['price'] = nfo_df.xs(current_end)['price']
    return out

# @timeit
def winsorize(df, limits=0.01):
    out = pd.DataFrame().reindex_like(df)
    for col in df.columns:
        out[col] = sp.stats.mstats.winsorize(df[col], limits=limits)
    return out

# @timeit
def zscore(df):
    out = pd.DataFrame().reindex_like(df)
    for col in df.columns:
        out[col] = (df[col] - np.mean(df[col])) / np.std(df[col])
    return out

@timeit
def calc_signal_df(BS_rf_df, IS_rf_df, CFS_rf_df, nfo_df, date_idx, BS_avg_bool = 1):
    """
    Calculate S-score features over a historical time period.

    Parameters
    ----------
    BS_rf_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with reformulated 
        balance sheet column values.
    IS_rf_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with reformulated 
        income statement items (annualized) column values.
    CFS_rf_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with cash flow 
        statement items (annualized) column values.
    nfo_df : pandas.DataFrame
        Dataframe containing non-financial statement information including historic as of 
        dates and quarterly filing dates used to validate data. 
    date_idx : pandas.DatetimeIndex
        list of dates with enough data to calculate S-score factors.
    BS_avg_bool : int, optional
        switch to toggle between using end of year balance sheet values or the average of
        beginning and end of year values, by default 1 (average value)

    Returns
    -------
    pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) with columns 
        of historic S-score factors.
    """
    start = time.time(); item_count = 0
    # col names of trading signals
    feat = ['RNOA_0', 'delta_RNOA_0', 'delta_PM_0', 'delta_ATO_0',
            'delta_NOA_0', 'accr_0', 'mkt_EP', 'BS_negNOA', 'price']

    idx = result.loc[date_idx.min():].index
    # init dataframe that will store trading signals
    features_df = pd.DataFrame().reindex(index = result.loc[date_idx.min():].index,
                                         columns = feat)

    for date in date_idx:
        print'\n-----=== date: {:.10} ===-----\n'.format(str(date))

        # grab 3 years of trailing financial statements
        # with IS & CFS annualized (trailing twelve months)
        BS_0yAgo,      BS_1yAgo,      BS_2yAgo,      BS_3yAgo, \
        IS_0yAgo_TTM,  IS_1yAgo_TTM,  IS_2yAgo_TTM, \
        CFS_0yAgo_TTM, CFS_1yAgo_TTM, CFS_2yAgo_TTM \
        = Hist_FS(BS_rf_df, IS_rf_df, CFS_rf_df, nfo_df, date)
        # dupont ratio calcs
        
        DPNT_0yAgo = DPNT_ratios(BS_0yAgo, BS_1yAgo, IS_0yAgo_TTM, BS_avg = BS_avg_bool)
        DPNT_1yAgo = DPNT_ratios(BS_1yAgo, BS_2yAgo, IS_1yAgo_TTM, BS_avg = BS_avg_bool)
        DPNT_2yAgo = DPNT_ratios(BS_2yAgo, BS_3yAgo, IS_2yAgo_TTM, BS_avg = BS_avg_bool)

        # calc trading signals
        factors = calc_factors(DPNT_0yAgo,   DPNT_1yAgo,
                               BS_0yAgo,     BS_1yAgo, 
                               IS_0yAgo_TTM, CFS_0yAgo_TTM, date)

        assert features_df.loc[date].index.equals(factors.index),\
        'index mismatch btn src and tgt slice df assignment'

        # store trading signals
        features_df.loc[date] = factors.values
        
        # debugging/timing
        end = time.time(); item_count += 1
        elapsed  = (end - start)
        est_wait = (len(date_idx) - item_count) * (elapsed/item_count)
        elapsed_min, elapsed_sec = divmod(elapsed, 60)
        est_min, est_sec = divmod(est_wait, 60)
        print 'elapsed time: {} mins {} secs'.format(int(elapsed_min), int(elapsed_sec))
        print'shape: {1}'.format(str(date),features_df.loc[date].shape)
        print 'estimated wait: {} mins {} secs\n'.format(int(est_min), int(est_sec))
        
    print features_df.info()
    return features_df

def calc_ft_dates(yrs):
    """
    Returns dates with enough historical data for fundamental factor calculations.


    @return returns datetimeIndex of dates

    Parameters
    ----------
    yrs : int
        number of years of trailing financial statements required for calc'n

    Returns
    -------
    _type_
        _description_
    """
    TS_dates = pd.unique(result.index.get_level_values(0).tz_localize(None))
    TS_dates = pd.DatetimeIndex(TS_dates)
    dtmin_3yHist = TS_dates.min() + MonthEnd(12) * yrs
    return TS_dates[TS_dates > dtmin_3yHist]

def _add_4qtrs(stk, idx, asof_dTbl, asof_grp, src_df):
    """annualizes IS and CFS from quarterly morningstar sourced data"""
    FS = None
    for i in idx:
        if FS is None: FS = src_df.loc[asof_dTbl[asof_grp[i]]].loc[stk].fillna(0)
        else:     FS = FS + src_df.loc[asof_dTbl[asof_grp[i]]].loc[stk].fillna(0)
    if src_df.columns.equals(CFS_f_df.columns):  
        # set beg cash to value at the start of first quarter of TTM instead of summing across 4 qtrs
        FS.CFS_beginning_cash_position = \
        src_df.loc[asof_dTbl[asof_grp[idx[0]]]].loc[stk].CFS_beginning_cash_position
    return FS

@timeit
def Hist_FS(BS_f_df, IS_f_df, CFS_f_df, nfo_df, TGT_endDate, num = 4):
    """
    Create dataframes for 4 years of trailing balance sheets and 3 years of annualized
    trailing income & cash flow statements used as an intermediate input to calculate S-score factors. 

    Parameters
    ----------
    BS_f_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) of Balance sheet values over the period of interest.
    IS_f_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) of income statement values over the period of interest.
    CFS_f_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) of cash flow statement values over the period of interest.
    nfo_df : pandas.DataFrame
        MultiIndex dataframe (level 1 = date, level 2 = symbol) of company information over the period of interest;  
    TGT_endDate : pandas.Timestamp
        timestamp for the date to use as target (i.e., current period) or time 0 period.
    num : int, optional
        number of years to slice data required for financio ratio calculations, by default 4

    Returns
    ------- 
    pandas.DataFrames
        10 MultiIndex dataframes (level 1 = date, level 2 = symbol) for 
        - 4 dataframes beging and ending year balance steets for 3 years of trailing balance sheet data
        - 3 years of trailing income statement data (annualized)
        - 3 years of trailing cashflow statement statement data (annualized)

    """
    TTM_begDate = TGT_endDate - MonthBegin((12*num)+1) # 
    min_qtrs = num * 3 + 1 # min qtrs of data for 3 yrs trailing
    
    # create 3mth sequences of past asof dates for debugging purposes and to test consistency of quaterly
    # asof date sequences (sometimes companies change quarter ends). 
    # for n in range(num):
    #     nfo_df['asof_'+str(n + 1)+'QSub'] = nfo_df['max_asof'] - MonthEnd(3 * (n + 1))
    
    # initialize empty dataframes with same symbol and column indexes as last rpt'd qtrj
    BS_0yAgo      = pd.DataFrame().reindex_like(BS_f_df.xs(TGT_endDate))
    BS_1yAgo      = pd.DataFrame().reindex_like(BS_f_df.xs(TGT_endDate))
    BS_2yAgo      = pd.DataFrame().reindex_like(BS_f_df.xs(TGT_endDate))
    BS_3yAgo      = pd.DataFrame().reindex_like(BS_f_df.xs(TGT_endDate)) 
    IS_0yAgo_TTM  = pd.DataFrame().reindex_like(IS_f_df.xs(TGT_endDate))
    IS_1yAgo_TTM  = pd.DataFrame().reindex_like(IS_f_df.xs(TGT_endDate))
    IS_2yAgo_TTM  = pd.DataFrame().reindex_like(IS_f_df.xs(TGT_endDate))
    CFS_0yAgo_TTM = pd.DataFrame().reindex_like(CFS_f_df.xs(TGT_endDate))
    CFS_1yAgo_TTM = pd.DataFrame().reindex_like(CFS_f_df.xs(TGT_endDate))
    CFS_2yAgo_TTM = pd.DataFrame().reindex_like(CFS_f_df.xs(TGT_endDate))
    
    insufficient_data = {} # init dict for logging stks with less than 3 yrs of data
    # c=0 # debugging 
    
    # 3-year of slice of nfo_df data grouped by symbol 
    stk_grpby = nfo_df[TTM_begDate:TGT_endDate].reset_index()
    stk_grpby = stk_grpby.loc[:,('level_0','level_1','max_asof')].groupby('level_1')
    
    for stk in nfo_df.xs(TGT_endDate).index:
        
        # filter out unique asof dates from nfo_df for each stock
        asof_grp = pd.unique(stk_grpby.get_group(stk).max_asof)
        # asof_grp = np.unique(stk_grpby.get_group(stk).max_asof.values)
        
        asof_dTbl = {}  # stores qtr end dates and the pipeline dates when that historic data was most recent
        
        if len(asof_grp) >= min_qtrs: # filter out firms w/ less than 13 qtrs of trailing data
            
            for n in xrange(min_qtrs): 
            # this loop finds the date when each historic quarterly was the most recent
            # available; associated timestamps will be used to annualize IS & CFS 10Qs (3 mth ending) ,
                
                asof_dateIdx = -(n + 1) 
                
                # data_date = last pipeline date when old asof dates were the most recent data available;
                # using most recent pipeline dates for each qtr end rpt'g data to reflect updated 
                # info in amended filings 
                data_date = stk_grpby.get_group(stk).level_0.loc[\
                            stk_grpby.get_group(stk).max_asof == \
                            asof_grp[asof_dateIdx]].max().tz_localize(None)
                
                # asof_grp[asof_dateIdx] = quarter end asof date 
                asof_dTbl[asof_grp[asof_dateIdx]] = data_date
            
            #  position idx of asof dates associated with historic 10Qs in asof_grp
            y0_Qidx =  xrange(-1, -5,  -1) # xrange(start, stop, step)
            y1_Qidx =  xrange(-5, -9,  -1)
            y2_Qidx =  xrange(-9, -13, -1)

            # if c==0: print asof_grp; c += 1 # debugging
            # if c==0: AAPL_dt = asof_dTbl; AAPL_grp = asof_grp; c += 1 # debugging
                
            # calc trailing TTM data for BS, IS, CFS over the past 3 years
            BS_0yAgo.loc[stk]      = BS_f_df.loc[asof_dTbl[asof_grp[-1]]].loc[stk]
            BS_1yAgo.loc[stk]      = BS_f_df.loc[asof_dTbl[asof_grp[-5]]].loc[stk]
            BS_2yAgo.loc[stk]      = BS_f_df.loc[asof_dTbl[asof_grp[-9]]].loc[stk]
            BS_3yAgo.loc[stk]      = BS_f_df.loc[asof_dTbl[asof_grp[-13]]].loc[stk]
            IS_0yAgo_TTM.loc[stk]  = _add_4qtrs(stk, y0_Qidx, asof_dTbl, asof_grp, IS_f_df)
            IS_1yAgo_TTM.loc[stk]  = _add_4qtrs(stk, y1_Qidx, asof_dTbl, asof_grp, IS_f_df)
            IS_2yAgo_TTM.loc[stk]  = _add_4qtrs(stk, y2_Qidx, asof_dTbl, asof_grp, IS_f_df)
            CFS_0yAgo_TTM.loc[stk] = _add_4qtrs(stk, y0_Qidx, asof_dTbl, asof_grp, CFS_f_df)
            CFS_1yAgo_TTM.loc[stk] = _add_4qtrs(stk, y1_Qidx, asof_dTbl, asof_grp, CFS_f_df)
            CFS_2yAgo_TTM.loc[stk] = _add_4qtrs(stk, y2_Qidx, asof_dTbl, asof_grp, CFS_f_df)
                
        else: insufficient_data[stk] = len(asof_grp) # log stks with insufficient data 

    return BS_0yAgo,      BS_1yAgo,      BS_2yAgo,      BS_3yAgo,\
           IS_0yAgo_TTM,  IS_1yAgo_TTM,  IS_2yAgo_TTM,\
           CFS_0yAgo_TTM, CFS_1yAgo_TTM, CFS_2yAgo_TTM