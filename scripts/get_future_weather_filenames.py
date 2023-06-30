import json
import requests

f = open("./Can_Canada_Future_files.txt", "r")

names = list()

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
            if (name in names) or (len(name) == 0):
                continue
            else:
                zip_loc = name.find("zip")
                if zip_loc >= 0:
                    names.append(name)
        else:
            line_start = line_length + 10

sort_names = sorted(names)

f.close()
json_object = json.dumps(sort_names, indent=4)
with open("../future_weather_filenames.json","w") as outfile:
    outfile.write(json_object)