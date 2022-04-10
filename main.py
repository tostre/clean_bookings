import pandas as pd; 
import os
from tabulate import tabulate
import numpy as np
import re

unneeded_columns = ["User", "Email", "Client", "Billable", "Duration", "Start time", "Start date", "End time", "End date", "Tags", "Amount ()"]
clean_columns = ["project", "task", "description"]
dir_path = os.path.dirname(os.path.realpath(__file__))
bookings = pd.read_csv(dir_path + "/bookings.csV", index_col=False)
current_week = bookings["Start date"].unique()
clean_columns = clean_columns + current_week.tolist()
clean_bookings = pd.DataFrame(columns=clean_columns)
agg_functions = {
    "Description": ', '.join,
    current_week[0]: "sum",
    current_week[1]: "sum",
    current_week[2]: "sum",
    current_week[3]: "sum",
    current_week[4]: "sum"
    }

# add weekday colums
for i in range(len(current_week)):
    bookings[current_week[i]] = 0

# iterate, clean table
for index, row in bookings.iterrows():
    # split tasks and description, only keep project number
    summary = row["Description"].split(" - ")
    project = row["Project"].split(" - ")
    bookings.at[index, "Task"] = summary[0]
    bookings.at[index, "Project"] = project[0]
    try: 
        bookings.at[index, "Description"] = summary[1]
    except IndexError: 
        bookings.at[index, "Description"] = ""
    # make duration from str to float, add duration to correct date value
    bookings.at[index, row["Start date"]] = float(re.sub(" h$", "", row["Duration"]))

# drop unneeded columns
bookings = bookings.drop(labels=unneeded_columns, axis=1)

# summarize time, description
bookings.reset_index(drop=True, inplace=True)
bookings = bookings.groupby(["Project", "Task"], as_index=False).agg(agg_functions).sort_values(["Project", "Task"])
bookings['Task'] = bookings[['Task', 'Description']].agg(' - '.join, axis=1)
bookings = bookings.drop("Description", axis=1)

# clearn list
for index, row in bookings.iterrows():
    # split tasks and description, only keep project number
    suffixes = [" - ", " , ", " , , ", " , , , ", " , , , , "]
    for suffix in suffixes:
        if row["Task"].endswith(suffix):
            bookings.at[index, "Task"] = row["Task"][:-len(suffix)]

# misc
#print(tabulate(bookings, headers='keys', tablefmt='psql'))
weekly_hours = 0
for i in reversed(range(1, len(current_week) + 1)):
    daily_hours = sum(bookings.iloc[:, -i].values.tolist())
    print("day -" + i.__str__()  + ": " + daily_hours.__str__())
    weekly_hours += daily_hours
print("week: " + weekly_hours.__str__())

bookings.to_csv(dir_path + "/bookings_clean.csv")