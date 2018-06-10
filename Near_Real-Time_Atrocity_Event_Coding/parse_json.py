import json
import os, glob

base_dir = "/Users/manishagalla/Documents/UTD/Spring17/BigDataManagement/Project"
input_files_path=os.path.join(base_dir,'input_files')
input_files_list=glob.glob(os.path.join(input_files_path,'*.txt'))
for i in input_files_list:
    with open(i) as f:
        jsn=f.read()
        jsn=json.loads(jsn)
        try:
            story=jsn['STORY']
            story_lines=story.split('. ')        
            output_path = os.path.join(base_dir,'parsed_files')
            base = os.path.basename(i)
            with open(output_path+'/'+base,'a') as f:
                for i in story_lines:
                    i=i.encode('ascii','ignore')
                    f.write(i)
                    f.write('\n')
        except:
            print "cannot parse file: ", os.path.basename(i)
    
        
