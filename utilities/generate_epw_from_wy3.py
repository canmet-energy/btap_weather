import math
from zipfile import ZipFile
import shutil
import os.path

# ========================================================================================================
# Find saturation pressure of water vapour. Based on correlations from the ASHRAE Handbook of Fundmentals 
# "Psychrometrics" Chapter
def psat_water_vapour(temp):
    temp_k = temp+273.15
    if (temp >= 0) and (temp < 373.946):
        n1 = 0.11670521452767e+4
        n2 = -0.72421316703206e+6
        n3 = -0.17073846940092e+2
        n4 = 0.12020824702470e+5
        n5 = -0.32325550322333e+7
        n6 = 0.14915108613530e+2
        n7 = -0.48232657361591e+4
        n8 = 0.40511340542057e+6
        n9 = -0.23855557567849e+0
        n10 = 0.65017534844798e+3
        theta = temp_k+n9/(temp_k-n10)
        A = theta**2+n1*theta+n2
        B = n3*theta**2+n4*theta+n5
        C = n6*theta**2+n7*theta+n8
        psat = 1000.0*(2.0*C/(-B+(B**2-4.0*A*C)**0.5))**4
    elif (temp >= -223.15) and (temp < 0):
        a1 = -0.212144006e+2
        a2 = 0.273203819e+2
        a3 = -0.610598130e+1
        b1 = 0.333333333e-2
        b2 = 0.120666667e+1
        b3 = 0.170333333e+1
        theta = temp_k/273.15
        psat = 0.611657*math.exp((a1*theta**b1+a2*theta**b2+a3*theta**b3)/theta)
    return psat

# ========================================================================================================
# Set conversion factors for variables from wy3 to epw
def wy3_to_epw_conv_f():
    conv_f = {}
    conv_f['temperature'] = 0.1
    conv_f['radiation'] = 1.0/3.6
    conv_f['pressure'] = 10
    conv_f['illuminance'] = 100
    conv_f['height'] = 10
    conv_f['speed'] = 0.1
    conv_f['visibility'] = 0.1
    return conv_f

# ========================================================================================================
# Set column numbers for variables in wy3 file. For most variable a list of 3 integers is specified.
# First integer is the start column for variable value. Second integer is for the end column for 
# variable value. The third integer is the end column for the flag value of the variable. If a variable 
# doesn't have a flag associated with it then only two integers are specified. 
def wy3_var_col_info():
    var_col_info = {}
    var_col_info['year'] = [9,12]
    var_col_info['month'] = [13,14]
    var_col_info['day'] = [15,16]
    var_col_info['hour'] = [17,18]
    var_col_info['tdb'] = [94,97,98]                  # Dry-bulb temperature (0.1 Deg C)
    var_col_info['tdp'] = [99,102,103]                # Dew point temperature (0.1 DegC)
    var_col_info['p'] = [88,92,93]                    # Atmospheric pressure (10 Pa) 
    var_col_info['ext_rad'] = [19,22]                 # Extraterrestrial irradiance (kJ/m2)
    var_col_info['global_hor_rad'] = [23,26,28]       # Global Horizontal Irradiance (kJ/m2)
    var_col_info['dir_nor_rad'] = [29,32,34]          # Direct Normal Irradiance (kJ/m2)
    var_col_info['diff_hor_rad'] = [35,38,40]         # Diffuse Horizontal Irradiance (kJ/m2)
    var_col_info['global_hor_ill'] = [41,44,45]       # Global Horizontal Illuminance (100 Lux)
    var_col_info['dir_nor_ill'] = [46,49,50]          # Direct Normal Illuminance (100 Lux)
    var_col_info['diff_hor_ill'] = [51,54,55]         # Diffuse Horizontal Illuminance (100 Lux)
    var_col_info['zenith_ill'] = [56,59,60]           # Zenith Illuminance (100 Lux)
    var_col_info['wind_dir'] = [104,106,107]          # Wind Direction (Degrees)
    var_col_info['wind_speed'] = [108,111,112]        # Wind Speed (0.1 m/s)
    var_col_info['total_sky_cover'] = [113,114,115]   # Total Sky Cover (Tenths)
    var_col_info['opaque_sky_cover'] = [116,117,118]  # Opauqe Sky Cover (Tenths)
    var_col_info['visibility'] = [74,77,78]           # Visibility (100 m)
    var_col_info['clg_height'] = [64,67,68]           # Ceiling Height (10 m)
    return var_col_info

# =============================================== Main Program ============================================
# Generate an EnergyPlus weather file (epw) based on an input CWEEDS (wy3) weather file. The header section 
# of the output epw file is read from an existing input epw file. The stat and ddy files are copies of input
# files of this type. The output epw, stat, and ddy files are then zipped together. 

# ========================================= Input Information ============================================
# update input information if needed
input_cweeds_fname = "input.wy3"  # name of CWEEDS weather file
input_epw_fname = "input.epw"  # name of input EPW file (if blank no header information will be copied)
input_stat_fname = "input.stat"  # name of input STAT file (if blank no output stat file is generated)
input_ddy_fname = "input.ddy"  # name of input DDY file (if blank no output ddy file is generated)
output_fname = "output"  # base name for output files (.epw, .stat, and .ddy)
# =========================================================================================================

# Set remmaining input and output files information base on inputs
input_cweeds_f = open(input_cweeds_fname,"r")  # CWEEDS input weather file
if len(input_epw_fname) > 0:
    input_epw_f = open(input_epw_fname,"r")  # EPW input file for header information ((first 8 lines)) 
output_epw_fname = output_fname + ".epw"
if os.path.isfile(output_epw_fname):
    os.remove(output_epw_fname)
output_epw_f = open(output_epw_fname,"w")  # output EPW file
if len(input_stat_fname) > 0:
    output_stat_fname = output_fname + ".stat"  # output stat file (copy of input STAT file)
    if os.path.isfile(output_stat_fname):
        os.remove(output_stat_fname)
    shutil.copy(input_stat_fname,output_stat_fname)
if len(input_ddy_fname) > 0:
    output_ddy_fname = output_fname + ".ddy"  #  output ddy file (copy of input DDY file)
    if os.path.isfile(output_ddy_fname):
        os.remove(output_ddy_fname)
    shutil.copy(input_ddy_fname,output_ddy_fname)
begin_line = 149018  # This line is the first record in the WY3 file
end_line = begin_line + 8760 - 1  # This line is the last record in the WY# file
if len(input_epw_fname) > 0:
    line_n = 0
    for line in input_epw_f:
        line_n += 1
        output_epw_f.write(line)
        if line_n == 8:
            break
line_n = 0
conv_f = wy3_to_epw_conv_f()
var_col_info = wy3_var_col_info()
for line in input_cweeds_f:
    line_n += 1
    if (line_n < begin_line):
        continue
    year = line[var_col_info['year'][0]-1:var_col_info['year'][1]]
    month = line[var_col_info['month'][0]-1:var_col_info['month'][1]]
    if month[0] == "0":
        month = month[1]
    day = line[var_col_info['day'][0]-1:var_col_info['day'][1]]
    if day[0] == "0":
        day = day[1]
    hour = line[var_col_info['hour'][0]-1:var_col_info['hour'][1]]
    if hour[0] == "0":
        hour = hour[1]
    minute = "0"
    tdb = line[var_col_info['tdb'][0]-1:var_col_info['tdb'][1]] 
    tdb_epw = str(round(int(tdb)*conv_f['temperature'],1))  # Dry-Bulb Temperature (DegC)
    tdb_flag = line[var_col_info['tdb'][1]:var_col_info['tdb'][2]]
    if (tdb_flag == " ") or (tdb_flag == "E"):  # blank is for measured in wy3 file
        tdb_ds = "?9"
    elif tdb_flag == "T":
        tdb_ds = "B9"
    else:
        print(tdb_flag)
        raise Exception("tdb flag is not blank nor E")
    tdp = line[var_col_info['tdp'][0]-1:var_col_info['tdp'][1]] 
    tdp_epw = str(round(int(tdp)*conv_f['temperature'],1))  # Dew-Point Temperature (DegC)
    tdp_flag = line[var_col_info['tdp'][1]:var_col_info['tdp'][2]]
    if (tdp_flag == " ") or (tdp_flag == "E"):  # blank is for measured in wy3 file
        tdp_ds = "?9"
    elif tdp_flag == "T":
        tdp_ds = "B9"
    else:
        print(tdp_flag)
        raise Exception("tdp flag is not blank nor E")
    pv = psat_water_vapour(float(tdp_epw))  
    psat = psat_water_vapour(float(tdb_epw))
    rh_epw = str(round(100*pv/psat))  # Relative Humidity (%)
    rh_ds = "?9"
    p = line[var_col_info['p'][0]-1:var_col_info['p'][1]] 
    p_flag = line[var_col_info['p'][1]:var_col_info['p'][2]]
    if (p_flag == " ") or (p_flag == "E"):  # blank is for measured in wy3 file
        p_ds = "?9"
    elif p_flag == "T":
        p_ds = "B9"
    else:
        print(p_flag)
        raise Exception("p flag is not blank nor E")
    p_epw = str(int(p)*conv_f['pressure'])  # Absolute Pressure (Pa)
    ext_rad = line[var_col_info['ext_rad'][0]-1:var_col_info['ext_rad'][1]]
    ext_rad_epw = str(round(float(ext_rad)*conv_f['radiation']))  # Extraterrestrial irradiance (Wh/m2)
    ext_rad_ds = "E0"  # calculated value
    ext_dir_rad_epw = "9999"  # Extraterrestrial Direct Normal Radiance (Wh/m2). Not present in wy3 file.
    ext_dir_rad_ds = "?9"  # missing in wy3 file
    ext_inf_rad_epw = "9999"  # Horizontal Infrared Radiation Intensity (Wh/m2). Not present in wy3 file.
    ext_inf_rad_ds = "?9"  # missing in wy3 file
    global_hor_rad = line[var_col_info['global_hor_rad'][0]-1:var_col_info['global_hor_rad'][1]]
    global_hor_rad_epw = "9999"
    if global_hor_rad != "9999":
        global_hor_rad_epw = str(round(float(global_hor_rad)*conv_f['radiation']))  # Global Horizontal Irradiance (Wh/m2)
    global_hor_rad_flag = line[var_col_info['global_hor_rad'][1]:var_col_info['global_hor_rad'][2]]
    if (global_hor_rad_flag == "M ") or (global_hor_rad_flag == "S ") or (global_hor_rad_flag == "N ") or \
    (global_hor_rad_flag == "I ") or (global_hor_rad_flag == "9 "):
        global_hor_rad_ds = "?0"
    else:
        print(global_hor_rad_flag)
        raise Exception("global_hor_rad_flag is not M nor S nor nor N nor I nor 9")
    dir_nor_rad = line[var_col_info['dir_nor_rad'][0]-1:var_col_info['dir_nor_rad'][1]]
    dir_nor_rad_epw = "9999"
    if dir_nor_rad != "9999":
        dir_nor_rad_epw = str(round(float(dir_nor_rad)*conv_f['radiation']))  # Direct Normal Irradiance (kJ/m2)
    dir_nor_rad_flag = line[var_col_info['dir_nor_rad'][1]:var_col_info['dir_nor_rad'][2]]
    if (dir_nor_rad_flag == "S ") or (dir_nor_rad_flag == "Q ") or  (dir_nor_rad_flag == "N ") or (dir_nor_rad_flag == "9 "):
        dir_nor_rad_ds = "?0"
    else:
        print(dir_nor_rad_flag)
        raise Exception("dir_nor_rad_flag is not S nor Q nor N nor 9")
    diff_hor_rad = line[var_col_info['diff_hor_rad'][0]-1:var_col_info['diff_hor_rad'][1]]
    diff_hor_rad_epw = "9999"
    if diff_hor_rad != "9999":
        diff_hor_rad_epw = str(round(float(diff_hor_rad)*conv_f['radiation'])) # Diffuse Horizontal Irradiance (Wh/m2)
    diff_hor_rad_flag = line[var_col_info['diff_hor_rad'][1]:var_col_info['diff_hor_rad'][2]]
    if (diff_hor_rad_flag == "S ") or (diff_hor_rad_flag == "M ") or (diff_hor_rad_flag == "N ") or (diff_hor_rad_flag) or \
        (diff_hor_rad_flag == "9 "):
        diff_hor_rad_ds = "?0"
    else:
        print(diff_hor_rad_flag)
        raise Exception("diff_hor_rad_flag is not S nor Q nor N nor I nor 9")
    global_hor_ill = line[var_col_info['global_hor_ill'][0]-1:var_col_info['global_hor_ill'][1]]
    global_hor_ill_epw = "999999"
    if global_hor_ill != "9999":
        global_hor_ill_epw = str(round(float(global_hor_ill)*conv_f['illuminance'])) # Global Horizontal Illuminace (Lux) 
    global_hor_ill_flag = line[var_col_info['global_hor_ill'][1]:var_col_info['global_hor_ill'][2]]
    if (global_hor_ill_flag == "Q") or  (global_hor_ill_flag == "9"):
        global_hor_ill_ds = "?0"
    else:
        raise Exception("global_hor_ill_flag is not Q nor 9")
    dir_nor_ill = line[var_col_info['dir_nor_ill'][0]-1:var_col_info['dir_nor_ill'][1]] 
    dir_nor_ill_epw = "999999"
    if dir_nor_ill != "9999":
        dir_nor_ill_epw = str(round(float(dir_nor_ill)*conv_f['illuminance'])) # Direct Normal Illuminace (Lux) 
    dir_nor_ill_flag = line[var_col_info['dir_nor_ill'][1]:var_col_info['dir_nor_ill'][2]]
    if (dir_nor_ill_flag == "Q") or (dir_nor_ill_flag == "9"):
        dir_nor_ill_ds = "?0"
    else:
        raise Exception("dir_nor_ill_flag is not Q nor 9")
    diff_hor_ill = line[var_col_info['diff_hor_ill'][0]-1:var_col_info['diff_hor_ill'][1]]
    diff_hor_ill_epw = "999999"
    if diff_hor_ill != "9999":
        diff_hor_ill_epw = str(round(float(diff_hor_ill)*conv_f['illuminance'])) # Diffuse Horizontal Illuminace (Lux) 
    diff_hor_ill_flag = line[var_col_info['diff_hor_ill'][1]:var_col_info['diff_hor_ill'][2]]
    if (diff_hor_ill_flag == "Q") or (diff_hor_ill_flag == "9"):
        diff_hor_ill_ds = "?0"
    else:
        print(diff_hor_ill_flag)
        raise Exception("diff_hor_ill_flag is not Q nor 9")
    zenith_ill = line[var_col_info['zenith_ill'][0]-1:var_col_info['zenith_ill'][1]]
    zenith_ill_epw = "9999"
    if zenith_ill != "9999":
        zenith_ill_epw = str(round(float(zenith_ill)*conv_f['illuminance'])) # Zenith Illuminace (Lux) 
    zenith_ill_flag = line[var_col_info['zenith_ill'][1]:var_col_info['zenith_ill'][2]]
    if (zenith_ill_flag == "9"):
        zenith_ill_ds = "?0"
    else:
        raise Exception("zenith_ill_flag is not 9")
    wind_dir = line[var_col_info['wind_dir'][0]-1:var_col_info['wind_dir'][1]]
    wind_dir_epw = "999"
    if wind_dir != "999":
        wind_dir_epw = str(int(wind_dir)) # Wind Direction (Degrees)
    wind_dir_flag = line[var_col_info['wind_dir'][1]:var_col_info['wind_dir'][2]]
    if (wind_dir_flag == " ") or (wind_dir_flag == "E") or (wind_dir_flag == "9"):
        wind_dir_ds = "?9"
    elif wind_dir_flag == "T":
        wind_dir_ds = "B9"
    else:
        print(wind_dir_flag)
        raise Exception("wind_dir_flag is not blank nor E nor 9")
    wind_speed = line[var_col_info['wind_speed'][0]-1:var_col_info['wind_speed'][1]]
    wind_speed_epw = "999"
    if wind_speed != "9999":
        wind_speed_epw = str(round(int(wind_speed)*conv_f['speed'],1)) # Wind Speed (m/s)
    wind_speed_flag = line[var_col_info['wind_speed'][1]:var_col_info['wind_speed'][2]]
    if (wind_speed_flag == " ") or ( wind_speed_flag == "E") or (wind_speed_flag == "9"):
        wind_speed_ds = "?9"
    elif wind_speed_flag == "T":
        wind_speed_ds = "B9"
    else:
        print(wind_speed_flag)
        raise Exception("wind_speed_flag is not blank nor E nor 9")
    total_sky_cover = line[var_col_info['total_sky_cover'][0]-1:var_col_info['total_sky_cover'][1]]
    total_sky_cover_epw = "99"
    if total_sky_cover != "99":
        total_sky_cover_epw = total_sky_cover  # # Total Sky Cover (Tenths)
    total_sky_cover_flag = line[var_col_info['total_sky_cover'][1]:var_col_info['total_sky_cover'][2]]
    if (total_sky_cover_flag == " ") or (total_sky_cover_flag == "9"):
        total_sky_cover_ds = "?9"
    else:
        raise Exception("total_sky_cover_flag is not blank nor 9")
    opaque_sky_cover = line[var_col_info['opaque_sky_cover'][0]-1:var_col_info['opaque_sky_cover'][1]]
    opaque_sky_cover_epw = "99"
    if opaque_sky_cover != "99":
        opaque_sky_cover_epw = opaque_sky_cover  # Opaque Sky Cover (Tenths)
    opaque_sky_cover_flag = line[var_col_info['opaque_sky_cover'][1]:var_col_info['opaque_sky_cover'][2]]
    if (opaque_sky_cover_flag == " ") or (opaque_sky_cover_flag == "9"):
        opaque_sky_cover_ds = "?9"
    else:
        raise Exception("total_sky_cover_flag is not blank nor 9")
    visibility = line[var_col_info['visibility'][0]-1:var_col_info['visibility'][1]]
    visibility_epw = "9999"
    if visibility != "9999":
        visibility_epw = str(round(int(visibility)*conv_f['visibility'],1))  # Visibility (km)
    visibility_flag = line[var_col_info['visibility'][1]:var_col_info['visibility'][2]]
    if (visibility_flag == " ") or (visibility_flag == "E") or (visibility_flag == "9"):
        visibility_ds = "?9"
    else:
        print(visibility_flag)
        raise Exception("visibility_flag is not blank nor E nor 9")
    clg_height = line[var_col_info['clg_height'][0]-1:var_col_info['clg_height'][1]]
    clg_height_epw = "99999"
    if clg_height != "9999":
        clg_height_epw = str(float(clg_height)*conv_f['height'])  # Ceiling Height (m)
    clg_height_flag = line[var_col_info['clg_height'][1]:var_col_info['clg_height'][2]]
    if (clg_height_flag == " ") or (clg_height_flag == "E") or (clg_height_flag == "9"):
        clg_height_ds = "?9"
    else:
        print(clg_height_flag)
        raise Exception("clg_height_flag is not blank nor 9")
    present_wth_obs_epw = "9"  # Present Weather Observation. Not present in wy3 file.
    present_wth_obs_ds = "?9"
    present_wth_codes_epw = "0"  # Present Weather Codes. Not present in wy3 file.
    present_wth_codes_ds = "?9"
    precipatable_water_epw = "999"  # Presipatable Water (mm). Not present in wy3 file.
    precipatable_water_ds = "?9"
    aerosol_optical_depth_epw = ".999"  # Aerosol Oprical Depth (0.001). Not present in wy3 file.
    aerosol_optical_depth_ds = "?9"
    snow_depth_epw = "999"  # Snow Depth (cm)
    snow_depth_ds = "?9"
    day_since_last_snow_fall_epw = "99"  # Day Since Last Snow Fall. Not present in wy3 file
    day_since_last_snow_fall_ds = "?9"
    albedo_epw = "0"  # Albedo ()
    albedo_ds = "?9"
    liquid_precipitation_depth_epw = "999"  # Liquid Precipitation Depth (mm). Not present in wy3 file.
    liquid_precipitation_depth_ds = "?9"
    liquid_precipitation_quantity_epw = "99"  # Liquid Precipitation Quantity (hr). Not present in wy3 file
    liquid_precipitation_quantity_ds = "?9"
    data_source = tdb_ds+tdp_ds+rh_ds+p_ds+ext_rad_ds+ext_dir_rad_ds+ext_inf_rad_ds+global_hor_rad_ds+dir_nor_rad_ds+diff_hor_rad_ds
    data_source += global_hor_ill_ds+dir_nor_ill_ds+diff_hor_ill_ds+zenith_ill_ds+wind_dir_ds+wind_speed_ds+total_sky_cover_ds+opaque_sky_cover_ds
    data_source += visibility_ds+clg_height_ds+present_wth_obs_ds+present_wth_codes_ds+precipatable_water_ds+aerosol_optical_depth_ds+snow_depth_ds
    data_source += day_since_last_snow_fall_ds+albedo_ds+liquid_precipitation_depth_ds+liquid_precipitation_quantity_ds
    epw_line = ""
    epw_line += year+","+month+","+day+","+hour+","+minute+","+data_source+","+tdb_epw+","+tdp_epw+","+rh_epw+","+p_epw+","+ext_rad_epw+","
    epw_line += ext_dir_rad_epw+","+ext_inf_rad_epw+","+global_hor_rad_epw+","+dir_nor_rad_epw+","+diff_hor_rad_epw+","+global_hor_ill_epw+","
    epw_line += dir_nor_ill_epw+","+diff_hor_ill_epw+","+zenith_ill_epw+","+wind_dir_epw+","+wind_speed_epw+","+total_sky_cover_epw+","
    epw_line += opaque_sky_cover_epw+","+visibility_epw+","+clg_height_epw+","+present_wth_obs_epw+","+present_wth_codes_epw+","+precipatable_water_epw+","
    epw_line += aerosol_optical_depth_epw+","+snow_depth_epw+","+day_since_last_snow_fall_epw+","+albedo_epw+","+liquid_precipitation_depth_epw+","
    epw_line += liquid_precipitation_quantity_epw+"\n"
    output_epw_f.write(epw_line)
    if (line_n == end_line):
        break
zip_fname = output_fname+".zip"
if os.path.isfile(zip_fname):
    os.remove(zip_fname)
zip_f = ZipFile(output_fname+".zip","w")
zip_f.write(output_epw_fname)
if len(input_stat_fname) > 0:
    zip_f.write(output_stat_fname)
if len(input_ddy_fname) > 0:
    zip_f.write(output_ddy_fname)
input_cweeds_f.close()
if len(input_epw_fname) > 0:
    input_epw_f.close()
zip_f.close()
output_epw_f.close()



