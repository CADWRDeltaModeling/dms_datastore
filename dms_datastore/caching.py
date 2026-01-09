import diskcache as dc
import os
import shutil
import inspect
import pandas as pd
import functools
import urllib.parse
import atexit
import click


class LocalCache:
    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = dc.Cache("cache", size_limit=int(6e9))
            atexit.register(cls.close)
        return cls._instance

    @classmethod
    def close(cls):
        if cls._instance is not None:
            cls._instance.close()
            cls._instance = None


def wrap_cache_value(data, func):
    return {"data": data, "fname": f"{func.__module__}.{func.__name__}"}


def unwrap_cache_value(obj):
    if isinstance(obj, dict) and "data" in obj:
        return obj["data"]
    return obj


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


import inspect


def cache_dataframe(key_args=None):
    """
    Decorator to cache function outputs based on selected keyword arguments.

    Parameters
    ----------
    key_args : list of str, optional
        Names of arguments to include in the cache key. If None, use all named arguments.
    """


import inspect


def cache_dataframe(key_args=None):
    """
    Decorator to cache function outputs based on selected keyword arguments.

    Parameters
    ----------
    key_args : list of str, optional
        Names of arguments to include in the cache key. If None, use all named arguments.
    """

    def decorator(func):
        sig = inspect.signature(func)
        func_name = func.__name__

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Bind args and kwargs to parameter names
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            all_args = bound.arguments  # ordered dict of all arguments

            # Filter to just those used in the cache key
            key_dict = {
                k: v for k, v in all_args.items() if key_args is None or k in key_args
            }

            cache = LocalCache.instance()
            cache_key = generate_cache_key(func_name, **key_dict)

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
        if key.startswith(func_name + "|"):
            df = cache[key].copy()
            _, args = parse_cache_key(key)
            # Extend the index with function arguments for each row
            if isinstance(df.index, pd.MultiIndex):
                index_arrays = list(df.index.levels)
                index_values = list(zip(*df.index.values))
                df_index_parts = [pd.Index(level) for level in zip(*df.index.values)]
            else:
                df_index_parts = [df.index]

            if isinstance(df.index, pd.MultiIndex):
                index_names = list(df.index.names)
                df_index_parts = [df.index.get_level_values(i) for i in index_names]
            else:
                index_names = [df.index.name or "index"]
                df_index_parts = [df.index]

            key_parts = [[args[k]] * len(df) for k in args]
            extended_index = pd.MultiIndex.from_arrays(
                df_index_parts + key_parts, names=index_names + list(args.keys())
            )
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

    class DummyFunction:
        def __init__(self, name):
            self.__name__ = name
            self.__module__ = "from_csv"

    cache = LocalCache.instance()
    header_map = {}

    with open(file_path, "r") as f:
        while True:
            pos = f.tell()
            line = f.readline()
            if not line.startswith("#"):
                f.seek(pos)
                break
            if ":" in line:
                key, val = line[1:].strip().split(":", 1)
                header_map[key.strip()] = val.strip()

    function_line = header_map.get("cached_function")
    keys_line = [s.strip() for s in header_map.get("keys", "").split(",") if s.strip()]
    col_keys_line = [
        s.strip() for s in header_map.get("col_keys", "").split(",") if s.strip()
    ]
    index_names = [
        s.strip() for s in header_map.get("index_name", "").split(",") if s.strip()
    ]

    # Load CSV body
    df = pd.read_csv(file_path, comment="#", header=0)

    # groupby requires keys to be in columns, not index — DO NOT drop them yet
    # Just confirm drop_keys for later
    drop_keys = [k for k in keys_line if k not in col_keys_line]

    # Group using keys
    for name, group in df.groupby(keys_line):
        kwargs = dict(zip(keys_line, name if isinstance(name, tuple) else (name,)))

        # Remove unwanted key columns from inside each group
        group = group.drop(columns=drop_keys, errors="ignore")

        # Set final index
        final_index = [col for col in index_names if col in group.columns]
        group = group.set_index(final_index)

        group = coerce_datetime_index(group)

        wrapped = wrap_cache_value(group, DummyFunction(function_line))
        print("CACHE INSERT:", generate_cache_key(function_line, **kwargs))
        cache[generate_cache_key(function_line, **kwargs)] = wrapped


def cache_to_csv(float_format=None):
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
        func_name = key.split("|")[0]
        if func_name in seen:
            continue
        seen.add(func_name)

        all_data = retrieve_all_data(func_name)
        if all_data.empty:
            continue

        index_name = all_data.index.names[0]
        keys = all_data.index.names[1:]
        file_path = f"{func_name}.csv"

        # Determine which keys are retained in the data
        col_keys = [key for key in keys if key in all_data.columns]
        with open(file_path, "w") as f:
            f.write(f"# cached_function: {func_name}\n")
            f.write(f"# index_name: {', '.join(all_data.index.names)}\n")
            f.write(f"# keys: {', '.join(keys)}\n")
            if len(col_keys) > 0:
                f.write(f"# col_keys: {', '.join(col_keys)}\n")
        all_data.to_csv(file_path, mode="a", float_format=float_format)


def coerce_datetime_index(df):
    """
    Try to convert the index of a DataFrame to a DatetimeIndex if possible.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame whose index may need datetime coercion.

    Returns
    -------
    pandas.DataFrame
        The DataFrame with a datetime index if conversion succeeded.
    """
    if isinstance(df.index, pd.Index) and df.index.dtype == object:
        try:
            df.index = pd.to_datetime(df.index)
        except Exception:
            pass
    return df


def caching(clear, to_csv, from_csv, delete, float_format):
    """Manage the local cache"""

    if (to_csv and delete) or (to_csv and clear):
        raise ValueError(
            "to_csv and delete/clear are incompatible. dump to csv, check the result then delete"
        )

    if (from_csv and delete) or (from_csv and clear) or (from_csv and to_csv):
        raise ValueError("from_csv and delete/clear/to_csv are incompatible.")

    if clear:
        print("Clearing local cache.")
        LocalCache.instance().clear()

    if to_csv:
        cache_to_csv(float_format)

    if from_csv:
        load_cache_csv(from_csv)

    if delete:
        if os.path.exists("cache"):
            shutil.rmtree("cache")


@click.command()
@click.option("--clear", is_flag=True, help="Clear local cache")
@click.option("--delete", is_flag=True, help="(Alias for --clear)")
@click.option(
    "--to-csv",
    "to_csv",
    is_flag=True,
    help="Flush current cache to CSV files (one per function)",
)
@click.option(
    "--float-format",
    "float_format",
    default=None,
    help="Float format string for to_csv",
)
@click.option(
    "--from-csv",
    "from_csv",
    default=None,
    help="Load cache from a previously saved CSV file",
)
@click.help_option("-h", "--help")
def data_cache_cli(clear, to_csv, from_csv, delete, float_format):
    """CLI for managing the local cache."""

    caching(clear, to_csv, from_csv, delete, float_format)


if __name__ == "__main__":
    data_cache_cli()

    # Main Example:
    # # Using the caching system example. No entry to here
    # df1 = get_dataframe1(string_arg="example1", int_arg=10)
    # df2 = get_dataframe2(string_arg="example2", float_arg=2.5)

    # print(df1)
    # print(df2)

    # # Save all caches to CSV
    # cache_to_csv()

    # # Using the caching system

    # df1 = get_dataframe1(string_arg="example1", int_arg=10)
    # df1b = get_dataframe1(string_arg="example1bb", int_arg=15)
    # df2 = get_dataframe2(string_arg="example2")
    # print(df1)

    # # Save all caches to CSV
    # cache_to_csv()

    # # Load the data back into the cache to verify it was saved correctly
    # load_cache_csv('get_dataframe1.csv')
    # load_cache_csv('get_dataframe2.csv')

    # # Retrieve data from cache to confirm it's correctly loaded
    # cache = LocalCache.instance()
    # print(cache[generate_cache_key(get_dataframe1, string_arg="example1", int_arg=10)])
    # print(cache[generate_cache_key(get_dataframe1, string_arg="example1bb", int_arg=15)])
    # print(cache[generate_cache_key(get_dataframe2, string_arg="example2")])
