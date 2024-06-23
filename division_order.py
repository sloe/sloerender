import re

import json5


class DivisionOrder:
    def __init__(self, json5_file):
        with open(json5_file, 'r', encoding='utf-8') as file:
            self.order = json5.load(file)

    def filter(self, regexp):
        pattern = re.compile(regexp)
        filtered_items = []
        for item in self.order['appearance_order']:
            if pattern.search(item['name']):
                filtered_items.append(item)
        return filtered_items
