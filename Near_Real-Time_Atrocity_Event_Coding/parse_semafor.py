import json
import os, glob

base_dir = "/Users/manishagalla/Documents/UTD/Spring17/BigDataManagement/Project"
output_files_path=os.path.join(base_dir,'output_files')
output_files_list=glob.glob(os.path.join(output_files_path,'*.txt'))
for parsed_file in output_files_list:
    all_frames=[]
    with open(parsed_file) as f:
        frames=f.readlines()
        for i in frames:
            print "######"
            span=[]
            frame_dict={}
            jsn=json.loads(i)
            for annotation in jsn['frames']:
               for element in annotation['annotationSets']:
                   for each in element['frameElements']:
                       if each != []:
                           if each['name'] in ['Victim','Weapon']:
                               if each['spans']!=[]:
                                   for spn in each['spans']:
                                       span.append(spn['text'])
            if span !=[]:
                frame_dict['Victim']=span
                all_frames.append(frame_dict)
    output_path=os.path.join(base_dir,'semafor_filter_files')
    base=os.path.basename(parsed_file)
    with open(output_path+'/'+base,'a') as f:
        f.write(str(all_frames))
