import sys
sys.path.append("/home/solutions/user_module")
from internal_functions import *
from collections import defaultdict
import pandas as pd


# create class_name_vs_annotation_type_vs_linked_attributes_list
class_name_vs_annotation_type_vs_linked_attributes_list = exec_query_replica('''SELECT
	pc.name,
	pc.annotation_type,
	ARRAY_AGG(pa."name")
FROM
	project_class pc
	INNER JOIN project_class_attribute_associator pcaa ON pc.id = pcaa.project_class_id
	INNER JOIN project_attribute pa ON pcaa.project_attribute_id = pa.id
WHERE
	pcaa.project_id = 'b5e2526a-6c95-4c62-a417-08a0cd55712d'
	AND pc.is_deleted = FALSE
GROUP BY
	1, 2
ORDER BY
	1;''')

# print(json.dumps(class_name_vs_annotation_type_vs_linked_attributes_list))

class_name_vs_annotation_type_vs_linked_attributes = {}
annotation_type_vs_present_classes = {}
for class_name, annotation_type, linked_attributes in class_name_vs_annotation_type_vs_linked_attributes_list:
    if annotation_type not in annotation_type_vs_present_classes.keys():
        annotation_type_vs_present_classes[annotation_type] = []
    annotation_type_vs_present_classes[annotation_type].append(class_name)
    if class_name not in class_name_vs_annotation_type_vs_linked_attributes.keys():
        class_name_vs_annotation_type_vs_linked_attributes[class_name] = {}
    class_name_vs_annotation_type_vs_linked_attributes[class_name][annotation_type] = linked_attributes

# read csv
csv_data = pd.read_excel('Data_SFV2 (2).xlsx')

job_id_vs_track_id_vs_label= defaultdict(dict)

for idx, csv_data_entry in csv_data.iterrows():
    job_id_vs_track_id_vs_label[csv_data_entry['job_id']][csv_data_entry['track_id']] = csv_data_entry['label']    
for job_id, track_id_vs_label in job_id_vs_track_id_vs_label.items():
    
    build = get_build_data(job_id)
    
    build_changed = False
    
    for track in build['maker_response']['sensor_fusion_v2']['data']['tracks']:
        if track['_id'] in track_id_vs_label.keys() and track['label'] != track_id_vs_label[track['_id']]:
            assert track_id_vs_label[track['_id']] in class_name_vs_annotation_type_vs_linked_attributes.keys(), f"Please check class in csv : {track_id_vs_label[track['_id']]}"
            track['label'] = track_id_vs_label[track['_id']]
            build_changed = True
            
    if build_changed:
        for annotation in build['maker_response']['sensor_fusion_v2']['data']['annotations']:
            if annotation['track_id'] in track_id_vs_label.keys() and annotation['label'] != track_id_vs_label[annotation['track_id']]:
                assert track_id_vs_label[annotation['track_id']] in annotation_type_vs_present_classes[annotation['type']], f"Please check class in csv : {track_id_vs_label[annotation['track_id']]} for annotation type : {annotation['type']}"
                annotation['label'] = track_id_vs_label[annotation['track_id']]
                
                attributes_to_delete = []
                for attribute_name in annotation['attributes'].keys():
                    if attribute_name not in class_name_vs_annotation_type_vs_linked_attributes[annotation['label']][annotation['type']]:
                        attributes_to_delete.append(attribute_name)
                for attribute_name in attributes_to_delete:
                    del annotation['attributes'][attribute_name]
                    
#         update(build, job_id)
        
        print(job_id, "Updated")
    else:
        print(job_id, "Skipped")
        
        
        
