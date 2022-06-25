# ============================================================================ #
# START
# ============================================================================ #

# Packages
import numpy as np
import pandas as pd
import os
import glob

# Print better
from rich.console import Console
console = Console()

# ============================================================================ #
# Working Directory Functions
# ============================================================================ #

# Get Working Directory
def getwd(pwd=None):
    cwd = os.getcwd()
    console.print(f'Working Directory: {cwd}', style='bold yellow')

# Change working directory
def changewd(wd):
    os.chdir(wd)
    mod_wd = os.getcwd()
    console.print(f'Current Working Directory: {mod_wd}', style='bold blue')

# ============================================================================ #
# Files
# ============================================================================ #

# List all files
def list_all_files(files=None):
    all_files = os.listdir()  # all files
    for f in all_files:
        console.print(f, style='green')

# List all csv files
def list_csv_files(files=None):
    all_files = os.listdir()  # all files
    csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
    console.print(f'Csv in cwd:\n {csv_files}', style='bold red')


# ============================================================================ #
# CSV section
# ============================================================================ #
def read_multiple_files(directory, extension="*.csv", show_index=True):
    
    # Path section
    path_csv = glob.glob(os.path.join(directory, extension))
    
    # Read multiple section
    d = {}
    for i in path_csv:
        d[i] = pd.read_csv(i, delimiter="separate", engine="python")
    
    # List names
    names_df = d.keys()
    
    if show_index == True:
        console.print("Which file do you need?", style="magenta")
        for index, item in enumerate(names_df):
            print(index, item)
    
    elif show_index == False:
        console.print("Files parsed", style="yellow")
    
    return d


# ============================================================================ #
# Selecting columns from pandas data frame (By number)
# ============================================================================ #
def select_by_number(data, *rest_arg):
    df = data.iloc[:, np.r_[rest_arg]]
    
    return df

# ============================================================================ #
# Function View Data Frame
# ============================================================================ #
def view_df(data, limit=None):

    for i in range(len(data.columns)):

        # Define variables
        q = i
        x = i + (i + (i + 1))
        y = i + (i + (i + 1)) + 1
        z = i + (i + (i + 1)) + 2
        b = len(data.columns)-1

        # No limit
        if not limit:
            
            # This is for ODD
            if len(data.columns) % 2 == 1:

                if q < 1:
                    with pd.option_context\
                        ('display.max_rows', 6, \
                            'display.max_columns', None,
                            'display.width', 70):

                        # Print first 4
                        print('\n')
                        print('1st & nth column: Red & Green')
                        print('\n')
                        console.print(data.iloc[:, np.r_[q]], style='red')
                        console.print(data.iloc[:, np.r_[b]],\
                             style='green')
                        print('\n')
                        console.print(data.iloc[:, np.r_[x, y, z]],\
                            style='blue')
                        print('\n')

                elif x < len(data.columns) or y < len(data.columns) or\
                        z < len(data.columns):

                        try:
                            with pd.option_context\
                            ('display.max_rows', 6, \
                                'display.max_columns', None,
                                'display.width', 70):

                                # Print rest
                                print('\n')
                                console.print(data.iloc[:, np.r_[x, y, z]],\
                                    style='yellow')
                                print('\n')
                        except:
                            print('\n')
                            console.print("COLNUM QTY IS ODD", style='red')
                            print('\n')

                        

            # This is for EVEN
            elif len(data.columns) % 2 != 1:
                if q < 1:
                    with pd.option_context\
                        ('display.max_rows', 6, \
                            'display.max_columns', None,
                            'display.width', 70):

                        # Print first 4
                        print('\n')
                        print('1st & nth column: Red & Green')
                        print('\n')
                        console.print(data.iloc[:, np.r_[q]], style='red')
                        console.print(data.iloc[:, np.r_[b]],\
                             style='green')
                        print('\n')
                        console.print(data.iloc[:, np.r_[x, y, z]],\
                            style='blue')
                        print('\n')

                elif x < len(data.columns) or y < len(data.columns) or\
                        z < len(data.columns):

                        try:

                            with pd.option_context\
                                ('display.max_rows', 6, \
                                    'display.max_columns', None,
                                    'display.width', 70):

                                # Print rest
                                print('\n')
                                console.print(data.iloc[:, np.r_[x, y, z]],\
                                    style='yellow')
                                print('\n')
                        
                        except:
                            console.print("COLUMN QTY IS EVEN", style='green')

        # With Limit
        elif limit:

            for u in range(limit):

                t = u
                j = u + (u + (u + 1))
                k = u + (u + (u + 1)) + 1
                l = u + (u + (u + 1)) + 2

                if t < 1:
                    
                    with pd.option_context\
                        ('display.max_rows', 6, \
                            'display.max_columns', None,
                            'display.width', 70):

                        # Print first 4
                        console.print(data.iloc[:, np.r_[t]], style='red')
                        console.print(data.iloc[:, np.r_[j, k, l]],\
                            style='blue')
                
                elif j < len(range(limit)) or k < len(range(limit)) or\
                    l < len(range(limit)):
                    
                    with pd.option_context\
                        ('display.max_rows', 6, \
                            'display.max_columns', None,
                            'display.width', 70):

                        # Print rest
                        print('\n')
                        console.print(data.iloc[:, np.r_[j, k, l]],\
                            style='yellow')
                        print('\n')
            
            break


# ============================================================================ #
# Function View Index
# ============================================================================ #
def view_index(data=None):
    # Print columns
    console.print({data.columns.get_loc(c): c \
        for idx, c in enumerate(data.columns)})