import csv
import os


class OrderSplitter:
    def __init__(self, base_output_dir='data/raw'):
        self.base_output_dir = base_output_dir
        self.writers = {}
        self.files = {}

    def _open_file_for_dow(self, dow, fieldnames):
        dow_dir = os.path.join(self.base_output_dir, dow)
        os.makedirs(dow_dir, exist_ok=True)
        output_path = os.path.join(dow_dir, f'orders_{dow}.csv')
        f = open(output_path, mode='w', newline='', encoding='utf-8')
        self.files[dow] = f
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        self.writers[dow] = writer

    def _close_all_files(self):
        for f in self.files.values():
            f.close()

    def split(self, input_file):
        try:
            with open(input_file, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    dow = row['order_dow']
                    if dow not in self.writers:
                        self._open_file_for_dow(dow, reader.fieldnames)
                    self.writers[dow].writerow(row)
        finally:
            self._close_all_files()


if __name__ == "__main__":
    splitter = OrderSplitter()
    splitter.split('orders.csv')
