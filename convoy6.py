from numpy import sign
import pandas as pd
import os.path
import re
import sqlite3
import json

def create_df(excel_filename, csv_filename):
    df = pd.read_excel(excel_filename, sheet_name="Vehicles", dtype=str)
    df.to_csv(csv_filename, index=None, header=True)
    rows = df.shape[0]
    if rows == 1:
        print(f"1 line was added to {csv_filename}")
    else:
        print(f"{rows} lines were added to {csv_filename}")
    return df

def correct_df(df, correct_filename):
    count = 0
    for row in df.values:
        for i in range(len(row)):
            try:
                row[i] = pd.to_numeric(row[i], downcast='signed')
            except Exception:
                row[i] = re.sub(r".*?(\d*).*?", r"\1", row[i])
                row[i] = pd.to_numeric(row[i], downcast='signed')
                count += 1

    df.to_csv(correct_filename, index=None, header=True)
    if count == 1:
        print(f"1 cell was corrected in {correct_filename}")
    if count > 1:
        print(f"{count} cells were corrected in {correct_filename}")

def create_db(df, db_filename):
    if os.path.exists(db_filename):
        os.remove(db_filename)

    con = sqlite3.connect(db_filename)
    cur = con.cursor()

    dbdef = []
    dbdef.append("vehicle_id INTEGER PRIMARY KEY")
    for header in df.columns:
        if header == "vehicle_id":
            continue
        dbdef.append(header + " INTEGER NOT NULL")
    dbdef.append("score INTEGET NOT NULL")

    dbdef = ", ".join(dbdef)
    cur.execute(f"CREATE TABLE convoy(" + dbdef + ");")

    count = 0
    for row in df.values:
        vehicle_id, engine_capacity, fuel_consumption, maximum_load = row
        score = calc_score(engine_capacity, fuel_consumption, maximum_load)
        row = (vehicle_id, engine_capacity, fuel_consumption, maximum_load, score)
        sql = f'INSERT INTO convoy values{row}'
        cur.execute(sql)
        con.commit()
        count += 1
 
    if count == 1:
        print(f"1 record was inserted into {db_filename}")
    if count > 1:
        print(f"{count} records were inserted into {db_filename}")

    return con, cur

def get_colnames(conn):
    cursor = con.execute('select * from convoy')
    colnames = [description[0] for description in cursor.description]
    return colnames

def create_json(con, cur, json_filename):
    colnames = get_colnames(con)
    cur.execute("SELECT * FROM convoy")		 
    rows = cur.fetchall()
    row_includelist = []
    row_excludelist = []
    for row in rows:
        row_dict = dict(zip(colnames, row))
        score = row_dict.pop("score")
        if score > 3:
            row_includelist.append(row_dict)
        else:
            row_excludelist.append(row_dict)
    convoy_dict = {"convoy": row_includelist}
    convoy_json = json.dumps(convoy_dict)

    with open(json_filename, 'w') as file:
	    file.write(convoy_json)

    if len(row_includelist) == 1:
        print(f"1 vehicle was saved into {json_filename}")
    if len(row_includelist) > 1:
        print(f"{len(row_includelist)} vehicles were saved into {json_filename}")

    convoy_dict = {"convoy": row_excludelist}
    convoy_json = json.dumps(convoy_dict)
    return convoy_json

def create_xml(convoy_json, xml_filename):
    json_data = json.loads(convoy_json)
    root = json_data["convoy"]
    xml = "<convoy>"
    count = 0
    for child in root:
        xml += "<vehicle>"
        for key, value in child.items():
            xml += f"<{key}>{value}</{key}>"
        xml += "</vehicle>"
        count += 1
    xml += "</convoy>"

    with open(xml_filename, 'w') as file:
	    file.write(xml)

    if count == 1:
        print(f"1 vehicle was saved into {xml_filename}")
    else:
        print(f"{count} vehicles were saved into {xml_filename}")

def calc_score(engine_capacity, fuel_consumption, maximum_load):
# 1) Number of pitstops. If there are two or more gas stops on the way, the object has 0 points. One stop at the filling station means 1 point. No stops — 2 scoring points.
# 2) Fuel consumed over the entire trip. If a truck burned 230 liters or less, 2 points are given. If more — 1 point.
# 3) Truck capacity. If the capacity is 20 tones or more, it gets 2 points. If less — 0 points.
    fuel_per_km = fuel_consumption / 100
    fuel_per_trip = 450 * fuel_per_km
    pitstop = fuel_per_trip // engine_capacity

    score = 0
    if pitstop == 0:            # 1)
        score += 2
    elif pitstop == 1:
        score += 1

    if fuel_per_trip <= 230:    # 2)
        score += 2
    else:
        score += 1

    if maximum_load >= 20:      # 3)
        score += 2

    return score

print("Input file name")
input_filename = input()
# input_filename = "convoy.xlsx"
filename, ext = os.path.splitext(input_filename)
if ext == ".xlsx":
    csv_filename = filename + ".csv"
    df = create_df(input_filename, csv_filename)

if ext == ".csv":
    df = pd.read_csv(input_filename)

if ext == ".xlsx" or ext == ".csv":
    if not filename.endswith("[CHECKED]"): 
        correct_filename = filename + "[CHECKED].csv"
        correct_df(df, correct_filename)
    else:
        correct_filename = input_filename

    db_filename = correct_filename[:len(correct_filename) - len("[CHECKED].csv")] + ".s3db"
    con, cur = create_db(df, db_filename)

if ext == ".s3db":
    db_filename = input_filename
    con = sqlite3.connect(db_filename)
    cur = con.cursor()

filename, ext = os.path.splitext(db_filename)
json_filename = filename + ".json"
convoy_json = create_json(con, cur, json_filename)
con.close()

xml_filename = filename + ".xml"
create_xml(convoy_json, xml_filename)
