# Veeam Coding Test

Example call:

`python .\folder_sync.py -s "path_of_source_folder" -d "path_of_destination_folder" -si second_of_interval -l "log_file_path"`  


`'-s', '--source_path', type=str`  
`'-d', '--destination_path', type=str`  
`'-si', '--sync_interval', type=int`  
`'-l', '--log_file', type=str`  

Only `watchdog` and `schedule` external libraries are required.  
`pip install requirements.ini` to install them.  