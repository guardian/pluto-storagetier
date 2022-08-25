#!/usr/bin/env python3

import json
from typing import *
import os
import os.path
import csv


def iterate_files(filepath:str)->Generator[List[Union[str, int]], None, None]:
    for name in os.listdir(filepath):
        try:
            with open(os.path.join(filepath, name)) as f:
                content = json.loads(f.read())
            yield [content["nearlineId"], content["itemId"], content["filePath"], content["mediaCategory"], content["mediaTier"], content["projectId"]]
        except KeyError as e:
            print("{0} was missing key {1}, skipping".format(os.path.join(filepath, name), str(e)))

### START MAIN
with open("report.csv", "w") as out:
    writer = csv.writer(out)
    writer.writerow(["NearlineID", "VS Item ID", "File path", "Category","Media Tier", "Project ID"])
    for line in iterate_files("testmsgs"):
        writer.writerow(line)
