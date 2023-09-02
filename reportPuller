# Script to successively increment the dates on sql queries, connect to a Snowflake database, pull the report, and compress the resultant file before moving onto the next date
# Date filter in queries should be on their own line and use the function "current_date()" instead of "current_date" as that is what the script searches for to increment the date

class sf_hero():
    import pandas as pd
    import timeit
    from datetime import timedelta, date

    def __init__(self):
        import os
        from pathlib import Path

        self.path = []
        self.directory = {}
        self.completed = 0
        self.total = 0
        self.totalExpected = 0
        self.endingStatString = ""

        for i in os.listdir(Path().absolute()):
            if i.endswith('.sql'):
                self.path.append(i)
        for i, h in enumerate(self.path):
            print(str(i + 1) + '. ' + str(h))
        self.total = len(self.path)
        print(str(self.total) + ' sql files in current directory.')

    def flake_em(self, head=False):
        import pandas as pd
        import snowflake.connector
        from time import time
        import os
        import csv
        from datetime import timedelta, date
        import gzip
        user_name = ''
        password = ''

        # Use for a few specific dates
        # e.g. dates = ["2021-01-18","2021-01-19","2021-01-31"]
        dates = []

        # Use across a date range, even at regular intervals
        # Inclusive begin date yyyy, m, d
        dateBegin = date(2022, 1, 19) - timedelta(days=0)
        # Non-inclusive end date
        dateEnd = date(2022, 1, 20)
        # regular interval at which reports should be pulled
        # e.g. 1 for daily, 7 for weekly
        interval = 4

        def daterange(dateBegin, dateEnd):
            dRange = pd.date_range(dateBegin, dateEnd - timedelta(days=1), freq='d')
            return dRange

        def getDateRangeList(dateBegin, dateEnd, interval=1):
            x = []
            y = interval
            for dt in daterange(dateBegin, dateEnd):
                if y == interval:
                    x.append(dt.strftime("%Y-%m-%d"))
                    y = 1
                else:
                    y += 1
            return x

        def setTotalExpected():
            self.totalExpected = self.total * len(dates)

        dates.extend(getDateRangeList(dateBegin, dateEnd, interval))

        setTotalExpected()
        print(f"Total expected reports: {self.totalExpected}")

        errantFiles = []
        try:
            os.mkdir('csv_files')
            print("Directory created: csv_files\n")
        except:
            print()

        finally:
            authentication = snowflake.connector.connect(user=user_name,
                                                                password=password,
                                                                account='ias.us-east-1',
                                                                warehouse='ADHOC_WH',
                                                                database='CDS_PROD',
                                                                schema='ANALYTICS'
                                                        )
            cur = authentication.cursor()

            currentDateString = 'current_date()'

            for d in dates:
                dateString = '\'' + d + '\''
                for i in self.path:
                    current_file = str(i)
                    with open(current_file, "rt") as fin:
                        current_file = current_file[:-4]+'_'+d.replace('-','')+current_file[-4:]

                        with open(current_file, "wt") as fout:
                            for line in fin:
                                if currentDateString in line.lower():
                                    line = line[:-1] + " and hit_date < " + currentDateString + line[-1:]
                                    line = line.lower().replace(currentDateString, "cast("+dateString+" as date)")
                                fout.write(line)
                                if currentDateString in line.lower():
                                    errantFiles.append(current_file)

                    try:
                        tic = time()
                        cur.execute(open(current_file, 'r').read())
                        toc = time()
                        df = cur.fetchall()
                        fShort = current_file[:-3]
                        with open(f"csv_files/{fShort}csv", 'w') as results:
                            writer = csv.writer(results)
                            writer.writerow(x[0].lower() for x in cur.description)
                            for row in df:
                                writer.writerow(row)
                        self.completed += 1
                        print(f"Completed ({self.completed}/{self.totalExpected}): {current_file}\nTime taken: {str(toc-tic)} seconds")

                        with open(f"csv_files/{fShort}csv", 'rb') as f_in, gzip.open(f"csv_files/{fShort}csv.gz", 'wb') as f_out:
                            f_out.writelines(f_in)
                        if os.path.exists(f"csv_files/{fShort}csv.gz") and os.path.exists(f"csv_files/{fShort}csv"):
                            os.remove(f"csv_files/{fShort}csv")
                            print(f"File zipped and replaced: {fShort}csv")
                        else:
                            print(f"File not zipped: {fShort}csv does not exist")

                    except:
                        print('error in the script: ' + current_file)

                    if head:
                        print(df.head())
                    print()

        self.endingStatString = f"{self.completed} out of {self.totalExpected} scripts compiled"
        print(self.endingStatString)
        if len(errantFiles) > 0:
            print("The following files resulted in error: ")
            for i in errantFiles:
                print(i)


a = sf_hero()
a.flake_em()
