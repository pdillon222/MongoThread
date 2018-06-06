#!/export/data/python/3.6.4/bin/python3.6

from subprocess import Popen, PIPE
import re
import datetime
import os
import sys


def command_map():
    '''
    Return a mapping of hsi commands to simple aliases
    '''
    cmd_list = ['hpss.nersc.gov',
                '/usr/syscom/nsg/opt/hsi/default/bin/hsi',
                'hostdump/mndlmdb02.nersc.gov/mongobackups']
    cmd_names = ['host',
                 'hsicmd',
                 'bakdir']
    cmd_map = {alias:cmd for alias, cmd in zip(cmd_names, cmd_list)}
    return cmd_map


def removal_list(time_target=187) -> list:
    '''
    Create a list of backup directories targeted for removal
    Takes a single argument: "time_target"
    Any directory older (in days) than this integer argument 
    will be considered targeted for removal
    '''
    dir_cmd = "{hsicmd} -h {host} -P 'ls {bakdir}'".format(**command_map())
    parse_out = Popen(dir_cmd, shell=True, stdout=PIPE, stderr=PIPE)
    decoded_dirs = [byte.decode('unicode_escape') 
                    for byte in parse_out.communicate()[0].split()]

    match = re.compile(r'.*\.([0-9\-]*)/')

    decoded_dirs = {"".join([char for char in list(teststr) if char != '/']): #remove trailing '/' from key
                    datetime.datetime.strptime(re.sub(match, r"\1", teststr), "%Y-%m-%d") #convert dict value to dto
                    for teststr in decoded_dirs if re.match(match, teststr)} 

    removal_list = []
    for values in decoded_dirs: 
        if (datetime.datetime.now() - decoded_dirs[values]).days > time_target:
            removal_list.append(values)

    return removal_list

def print_statement(backup_dir_list: list, file_out=sys.stdout) -> str:
    '''
    Change file_out argument as needed for printing within a file
    when function gets called 
    '''
    print_statement = "\n\n" + "Directories marked for removal:"
    print_statement += "\n" + ("*" * len(print_statement)) + "\n"
    for dir in backup_dir_list:
        print_statement += dir + "\n"
    return print_statement
   

def remove_dirs(backup_dir_list: list) -> None:
    '''
    Remove all directories, subfolders and files
    from list of remote backups passed in as argument
    '''
    cmd_map = command_map()
    if not backup_dir_list:exit()
    for directory in backup_dir_list: #make total destroy 
        cmd_map['directory'] = directory   
        os.system("{hsicmd} -h {host} 'rm {bakdir}/{directory}/*/*' > /dev/null 2>&1".format(**cmd_map))
        os.system("{hsicmd} -h {host} 'rmdir {bakdir}/{directory}/*' > /dev/null 2>&1".format(**cmd_map))
        os.system("{hsicmd} -h {host} 'rmdir {bakdir}/{directory}' > /dev/null 2>&1".format(**cmd_map))    
        os.system("{hsicmd} -h {host} 'rm {bakdir}/{directory}/*' > /dev/null 2>&1".format(**cmd_map))    
        os.system("{hsicmd} -h {host} 'rmdir {bakdir}/{directory}' > /dev/null 2>&1".format(**cmd_map))    

if __name__ == "__main__":
    print("\n", "Two years ({} total): ".format(len(removal_list(730))), removal_list(730))
    print("\n", "One year ({} total): ".format(len(removal_list(365))), removal_list(365))
    print("\n", "Six months ({} total): ".format(len(removal_list(187))), removal_list(187))
    print("\n", "Three months ({} total): ".format(len(removal_list(90))), removal_list(90))
    print("\n", "All directories ({} total): ".format(len(removal_list(0))), removal_list(0))
    print("\n")
    print(print_statement(removal_list(0)))
    prompt =  input("\nWould you like to proceed with erasing these backups (Y || N)?: ")
    #if prompt.upper() == "Y": remove_dirs(removal_list())
