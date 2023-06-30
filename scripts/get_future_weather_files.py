import json
import requests

f = open("./Can_Canada_Future_files.txt", "r")

names = list()

pre_string = "https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/CAN_Canada_Future/"
pre_string_len = len(pre_string)

for line in f:
    line_length = len(line)
    line_start = 0
    while line_start < line_length:
        input_line = line[line_start:len(line)]
        search_string = "title=\""
        search_string_len = len(search_string)
        index = input_line.find(search_string)
        if index >= 0:
            start_index = line_start + index + search_string_len
            partline = line[start_index:line_length]
            index_close = partline.find("\"")
            end_name = start_index + index_close
            line_start = end_name + 1
            name = line[(start_index):end_name]
            name_add = pre_string + name
            if (name_add in names) or (len(name) == 0):
                continue
            else:
                zip_loc = name_add.find("zip")
                if zip_loc >= 0:
                    names.append(name_add)
        else:
            line_start = line_length + 10

sort_names = sorted(names)

f.close()
json_object = json.dumps(sort_names, indent=4)
with open("./download_future_names.json","w") as outfile:
    outfile.write(json_object)

count_ind = 0

for name in sort_names:
    if len(name) == 0:
        continue
    else:
        count_ind += 1
        out_name_start = name[pre_string_len:len(name)]
        start_index = out_name_start.find("/") + 1
        out_name = ".././future/" + out_name_start[start_index:len(out_name_start)]
        r = requests.get(name, allow_redirects=True)
        open(out_name, 'wb').write(r.content)

print(count_ind)

#print("hello")

#https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/CAN_Canada_Future/AB_Alberta/CAN_AB_Abee.AgDM.712850_NRCv12022_TMY_GW3.0.zip
#https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/CAN_Canada_Future/NL_Newfoundland_and_Labrador/CAN_NL_Deer.Lake.Rgnl.AP.718090_NRCv12022_TRY_MaxTemp_GW0.5.zip