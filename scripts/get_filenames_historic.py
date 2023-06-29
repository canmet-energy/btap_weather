import json
import requests

f = open("./Can_Canada_Historic_files.txt", "r")

names = list()

pre_string = "https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/CAN_Canada/"
pre_string_len = len(pre_string)

for line in f:
    index = line.find("href=\"")
    start_name = index + 6
    partline = line[start_name:-1]
    index_close = partline.find("\"")
    end_name = start_name + index_close
    name = line[(start_name):end_name]
    name_add = pre_string + name
    if (name_add in names) or (len(name) == 0):
        continue
    else:
        names.append(name_add)

f.close()

json_object = json.dumps(names, indent=4)
with open("./download_historic_names.json","w") as outfile:
    outfile.write(json_object)

count_ind = 0

for name in names:
    if len(name) == 0:
        continue
    else:
        count_ind += 1
        out_name_start = name[pre_string_len:len(name)]
        start_index = out_name_start.find("/") + 1
        out_name = ".././historic/" + out_name_start[start_index:len(out_name_start)]
        r = requests.get(name, allow_redirects=True)
        open(out_name, 'wb').write(r.content)

print(count_ind)

#print("hello")

#https://climate.onebuilding.org/WMO_Region_4_North_and_Central_America/CAN_Canada/AB_Alberta/CAN_AB_Athabasca.AgCM.712710_TMYx.2004-2018.zip