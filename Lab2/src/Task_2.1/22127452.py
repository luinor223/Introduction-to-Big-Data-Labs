# 22127452 - Task 2.1
from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime, timedelta
import os

class TotalRevenue(MRJob):

    def mapper(self, _, line):
        fields = line.split(',')
        if fields[0] == "index":
            return

        try:
            date_str, category, amount = fields[2], fields[9], float(fields[15])
            report_date = datetime.strptime(date_str, "%m-%d-%y")

            for i in range(3):
                sliding_date = report_date + timedelta(days=i)
                yield (sliding_date.strftime("%Y-%m-%d"), category), amount
        except ValueError:
            pass

    def reducer(self, key, values):
        total_sales = sum(values)
        formatted_date = datetime.strptime(key[0], "%Y-%m-%d").strftime("%d/%m/%Y")
        yield (formatted_date, key[1]), round(total_sales, 2)

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            )
        ]

if __name__ == "__main__":
    job = TotalRevenue()
    with job.make_runner() as runner:
        runner.run()
        output_dir = runner.get_output_dir()

        all_results = []
        for filename in sorted(os.listdir(output_dir)):
            if filename.startswith('part-'):
                with open(os.path.join(output_dir, filename), 'r') as f:
                    for line in f:
                        all_results.append(line.strip())
        
        with open('output.csv', 'w', newline='') as csvfile:
            csvfile.write("report_date, category, revenue\n")
            for result in all_results:
                key, value = result.split('\t')
                report_date, category = eval(key)
                csvfile.write(f"{report_date},{category},{value}\n")