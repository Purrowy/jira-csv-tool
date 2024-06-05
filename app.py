import csv
import os
import json
import subprocess
from datetime import datetime, timedelta
import logging

# logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def main():

    # Loads settings from json
    with open("settings.json", 'r') as file:
        data = json.load(file)
        token = data.get("token")
        keywords = data.get("keywords")
        days = data.get("days")
        path = data.get("path")
        issues = data.get("issueColumn")
        attachments = data.get("attColumn")
        timestamp_format = data.get("timestampFormat")
        cell_delimiter = data.get("inputCellDelimiter")    

    # Imports issue number and attachment link from provided CSV into a library
    imported_csv = []
    with open("import.csv", 'r') as file:
        reader = csv.reader(file)
        headers = next(reader)

        # In case of multiple columns with same name -> stops at first found
        issue_index = headers.index(issues)
        attachment_index = headers.index(attachments)

        for row in reader:
            ## Splits data in csv, in case of multiple values in one cell
            ## This only applies to a specific scenario, I should make probably rethink and rewrite this part
            attachments_split = row[attachment_index].split(cell_delimiter)[3]
            columns = {issues: row[issue_index], attachments: attachments_split}
            imported_csv.append(columns)
    
    # print(imported_csv)

    # Downloads each attachment from link in imported_csv and saves it under ticket's name
    for ticket in imported_csv:
        link = ticket[attachments]
        filename = ticket[issues]

        command = [
            "curl",
            "-H", f"Authorization: Bearer {token}",
            "-o", f"{path}{filename}",
            link
        ]

        # Logs executed curls into a txt file, can be disabled
        # logging.info(f"Running command: {' '.join(command)}")   
   
        try:
            subprocess.run(command, check=True, text=True, capture_output=True)
            print(f"Downloaded {filename}")

        except subprocess.CalledProcessError as e:
            print(f"Download failed for {link}: {e}")
    
    # Gets list of files from target folder
    files = os.listdir(path)

    # print(f"Files in dir: {', '.join(files)}")

    # Combines data from all downloaded CSV files into one list + assigns filename to each row
    full_data = []
    for file in files:
        with open(path + file) as f:
            reader = csv.reader(f)
            for row in reader:
                full_data.append((file, row))

    # print(full_data)

    print(f"Excluded keywords: {', '.join(keywords)}")   

    # Creates a set of files containing exceptions
    files_with_keywords = set()
    for keyword in keywords:
        for filename, row in full_data:
            if any(keyword in cell for cell in row):
                files_with_keywords.add(filename)
    
    print(f"Files with keywords: {', '.join(files_with_keywords)}")
    
    # Sets the minimum date for timestamps
    minimum_date = datetime.now() - timedelta(days=days)
    
    print(f"Minimum timestamp set to: {minimum_date}")

    # Creates a set of files that failed the timestamp test
    files_old = set()
    for filename, row in full_data:
        for cell in row:
            timestamp = datetime.strptime(cell[:15], timestamp_format)            
            if timestamp < minimum_date:
                files_old.add(filename)

    print(f"Files with older timestamp: {', '.join(files_old)}")

    # Produces final jql
    remaining_files = set(files) - files_old - files_with_keywords
    query = '", "'.join(remaining_files)
    print(f'Remaining files: \nkey in ("{query}")')

main()