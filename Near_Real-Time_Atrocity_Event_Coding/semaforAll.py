import os
from subprocess import Popen, PIPE

base_dir = "/Users/manishagalla/Documents/UTD/Spring17/BigDataManagement/Project"
input_files_path=os.path.join(base_dir,'parsed_files')
input_files_list=os.listdir(input_files_path)

for i in input_files_list:
    try:
        input_path = os.path.join(input_files_path,i)
        output_path = os.path.join(base_dir,"output_files1")
        cmd = 'semafor/bin/runSemafor.sh '+input_path + ' ' + output_path + '/' + i + ' 10'
        #print cmd
        p = Popen(cmd , shell=True, stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        print "Return code: ", p.returncode
    except:
        print "cannot parse file: ", i
        
    
    
    
