import diskcache as dc
import os
import shutil
import pandas as pd
import functools
import urllib.parse
import atexit
import argparse

class LocalCache:
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = dc.Cache('cache',size_limit=int(6e9))
            atexit.register(cls.close)
            print("Cache created or accessed")
        return cls._instance

    @classmethod
    def close(cls):
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None
            print("Cache instance closed")

def parse_cache_key(key):
    func_name, args_str = key.split("|", 1)
    args = dict(urllib.parse.parse_qsl(args_str))
    return func_name, args


def parse_cache_key(key):
    func_name, args_str = key.split("|", 1)
    args = dict(urllib.parse.parse_qsl(args_str))
    return func_name, args

def cache_dataframe(key_args=None):
    """
    Decorator to cache function outputs based on selected keyword arguments.

    This decorator caches the output of functions to avoid repeated computation or retrieval
    costs on the second or later invocation. The decorator uses a subset of the function's 
    keyword arguments specified in the decorator by `key_args` as a cache key. 
    If `key_args` is None, all keyword arguments are used. Only kwargs are used for caching
    so this impacts how the wrapped generating function is called. It leaves open the possiblity
    that the non-kwarg arguments can supply data ... flexible but a bit dangerous.

    The library assumes (or is only tested for) the case where any single decorated function 
    returns a dataframe with similar column structure and
    a time index. But you can cache multiple functions.

    Parameters
    ----------
    key_args : list of str, optional
        A list of keyword argument names to be included in the cache key.
        If None (default), all keyword arguments are used for the cache key.

    Returns
    -------
    callable
        A wrapper function that adds caching based on the specified key arguments.

    Examples
    --------
    >>> @cache_dataframe(key_args=['string_arg', 'int_arg'])
    ... def compute_complex_operation(string_arg, int_arg, other_arg=None):
    ...     # expensive computation here
    ...     return string_arg * int_arg

    Using the decorator without specifying key_args uses all kwargs for caching:

    >>> @cache_dataframe()
    ... def fetch_data(arg1, arg2, kwarg1=None, kwarg2=None):
    ...     # some data fetching logic
    ...     return arg1 + arg2 + (kwarg1 if kwarg1 else 0) + (kwarg2 if kwarg2 else 0)
    """        
    def decorator(func):
        func_name = func.__name__  # Capture the function name as a string

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache = LocalCache.instance()
            key_kwargs = {k: v for k, v in kwargs.items() if k in key_args} if key_args is not None else kwargs
            cache_key = generate_cache_key(func_name, **key_kwargs)
            
            if cache_key in cache:
                return cache[cache_key]
            else:
                result = func(*args, **kwargs)
                cache[cache_key] = result
                return result
        return wrapper
    return decorator

def generate_cache_key(func_name, **kwargs):
    """
    Generates a cache key based on the function name and its keyword arguments.

    This function creates a unique cache key by combining the function's name with
    a URL-encoded string of the keyword arguments passed to it. This key is used
    to store and retrieve items from the cache.

    Parameters
    ----------
    func_name : str
        The name of the function for which the cache key is being generated.
    **kwargs : dict
        Keyword arguments used for generating the cache key.

    Returns
    -------
    tuple
        A tuple containing the cache key and the dictionary of keyword arguments used.

    Examples
    --------
    >>> def example_function(param1, param2):
    ...     return param1 * param2
    >>> key, params = generate_cache_key(example_function, param1=5, param2=10)
    >>> print(key)
    "example_function|param1=5&param2=10"
    """        
    sorted_kwargs = {k: kwargs[k] for k in sorted(kwargs)}
    args_str = urllib.parse.urlencode(sorted_kwargs)
    key = f"{func_name}|{args_str}"
    return key
    
def retrieve_all_data(func_name):
    """
    Retrieves and concatenates all data related to a specific function from the cache.

    This function aggregates all entries in the cache for a specified function,
    combining them into a single pandas DataFrame with a MultiIndex. Each level of
    the MultiIndex corresponds to one of the function's arguments used as a cache key.

    Please be careful using the result -- time alone is not a unique key.

    Parameters
    ----------
    func_name : str
        The name of the function whose data is to be retrieved.

    Returns
    -------
    pandas.DataFrame
        A concatenated DataFrame of all cached entries for the function, or
        an empty DataFrame if no data is found.

    Examples
    --------
    >>> df = retrieve_all_data('get_dataframe1')
    >>> print(df.head())
    """     
    cache = LocalCache.instance()
    dataframes = []
    for key in cache.iterkeys():
        if key.startswith(func_name + '|'):
            df = cache[key].copy()
            _, args = parse_cache_key(key)
            # Extend the index with function arguments for each row
            extended_index = pd.MultiIndex.from_arrays([df.index] + [[value] * len(df) for value in args.values()],
                                                       names=['datetime'] + list(args.keys()))
            df.set_index(extended_index, inplace=True)
            dataframes.append(df)
    return pd.concat(dataframes) if dataframes else pd.DataFrame()



def load_cache_csv(file_path):
    """
    Loads data from a CSV file into the cache, reconstructing the cached DataFrame structure.

    This function reads the CSV file specified by `file_path`, which contains cached data
    for a function, along with metadata headers indicating the function and keys used for
    caching. It reconstructs the DataFrame with the appropriate MultiIndex based on the
    metadata and repopulates the cache with these DataFrames.

    Parameters
    ----------
    file_path : str
        The path to the CSV file to be loaded. This file should contain metadata headers
        and data formatted as saved by `cache_to_csv()`.

    Returns
    -------
    None

    Side Effects
    ------------
    - Populates the cache with the data loaded from the CSV file, potentially overwriting
      any existing entries that match the keys derived from the file.

    Examples
    --------
    >>> load_cache_csv('get_dataframe1.csv')
    # This will load data from 'get_dataframe1.csv' into the cache, reconstructing
    # the DataFrame structure and using it to populate the cache.
    """    
def load_cache_csv(file_path):
    cache = LocalCache.instance()
    with open(file_path, 'r') as f:
        function_line = f.readline().strip().split(": ")[1]
        index_name_line = f.readline().strip().split(": ")[1]
        keys_line = f.readline().strip().split(": ")[1].split(', ')
    df = pd.read_csv(file_path, comment='#', header=0)
    # Combine the dynamic index name with the keys for setting the index
    df.set_index([index_name_line] + keys_line, inplace=True)
    
    grouped = df.groupby(keys_line)

    for name, group in grouped:
        kwargs = {key: value for key, value in zip(keys_line, name)}
        cache_key = generate_cache_key(function_line, **kwargs)
        group.reset_index(keys_line + [index_name_line], inplace=True)
        group.set_index(index_name_line, inplace=True)

        cache[cache_key] = group


def cache_to_csv():
    """
    Writes all cached data to CSV files, one for each unique function.

    This function iterates over all entries in the cache, grouped by function name.
    For each group associated with a function, it retrieves all related data using
    `retrieve_all_data`, and saves it to a CSV file named after the function.
    The CSV file includes metadata headers specifying the function and keys used
    for caching.

    No parameters.

    Returns
    -------
    None

    Side Effects
    ------------
    - Creates or overwrites CSV files in the current directory, one for each function
      cached, containing all data related to that function.
    - Outputs files named `<function_name>.csv`.

    Examples
    --------
    >>> cache_to_csv()
    # This will create CSV files like `get_dataframe1.csv`, `get_dataframe2.csv`, etc.,
    # containing all cached data for these functions.
    """       
    cache = LocalCache.instance()
    seen = set()
    for key in cache.iterkeys():
        func_name = key.split('|')[0]
        if func_name not in seen:
            seen.add(func_name)
            print(f"Exporting {func_name} to csv")
            all_data = retrieve_all_data(func_name)
            if not all_data.empty:
                index_name = all_data.index.names[0] if all_data.index.names else 'index'
                file_path = f'{func_name}.csv'
                with open(file_path, 'w') as f:
                    f.write(f"# cached_function: {func_name}\n")
                    f.write(f"# index_name: {index_name}\n")
                    keys = [name for name in all_data.index.names[1:]]
                    f.write(f"# keys: {', '.join(keys)}\n")
                all_data.to_csv(file_path, mode='a', header=True)
            else:
                print(f"No data available to save for function: {func_name}")


def main_example():
    # Using the caching system example. No entry to here
    df1 = get_dataframe1(string_arg="example1", int_arg=10)
    df2 = get_dataframe2(string_arg="example2", float_arg=2.5)

    print(df1)
    print(df2)

    # Save all caches to CSV
    cache_to_csv()

    # Using the caching system
        
    df1 = get_dataframe1(string_arg="example1", int_arg=10)
    df1b = get_dataframe1(string_arg="example1bb", int_arg=15)
    df2 = get_dataframe2(string_arg="example2")
    print(df1)

    # Save all caches to CSV
    cache_to_csv()

    # Load the data back into the cache to verify it was saved correctly
    load_cache_csv('get_dataframe1.csv')
    load_cache_csv('get_dataframe2.csv')

    # Retrieve data from cache to confirm it's correctly loaded
    cache = LocalCache.instance()
    print(cache[generate_cache_key(get_dataframe1, string_arg="example1", int_arg=10)[0]])
    print(cache[generate_cache_key(get_dataframe1, string_arg="example1bb", int_arg=15)[0]])
    print(cache[generate_cache_key(get_dataframe2, string_arg="example2")[0]])

def create_arg_parser():
    """ Create an argument parser
    """
    parser = argparse.ArgumentParser(description="Create inventory files, including a file inventory, a data inventory and an obs-links file.")
    parser.add_argument('--clear',  action='store_true', help = "clear local cache") 
    parser.add_argument('--to_csv', action='store_true', help = "flush to csv")
    parser.add_argument('--delete', action='store_true', help = "clear local cache") 
    return parser


def main():

    parser = create_arg_parser()
    args = parser.parse_args()
    clear = args.clear
    to_csv = args.to_csv
    delete = args.delete
    if (to_csv and delete) or (to_csv and clear):
        raise ValueError("to_csv and delete/clear are incompatible. dump to csv, check the result then delete")
    
    if clear:
        print("Clearing local cache.")
        LocalCache.instance().clear()

    if to_csv:
        cache_to_csv()

    if delete:
        if os.path.exists('cache'):
            shutil.rmtree("cache")            




if __name__ == "__main__":
    main()

