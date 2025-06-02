from datetime import datetime
from typing import Dict, List

from lib import MongoConnect

class OrderReader:
    def __init__(self,mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_orders(self,load_treshload: datetime, limit) -> List[Dict]:
        # фильтр выборки
        filter = {'update_ts': {'$gt': load_treshload}}

        # сортировка по update_ts
        sort = [('update_ts',1)]

        # вычитывание доков из Mongo DB
        docs = list(self.dbs.get_collection("orders").find(filter = filter, sort = sort, limit = limit))
        
        return docs